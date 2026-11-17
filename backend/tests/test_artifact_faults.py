"""产物写入/登记各阶段故障：写出后未完成、登记后产物损坏、半份文件不可读。"""

from src.models import AnalysisRun, Dataset, Experiment, Observation
from src.services import artifact_service, execution_service, recovery_service, run_service
from src.utils.clock import now


def setup(db):
    ds = Dataset(name="d", source="s", description="x", checksum="c")
    db.add(ds)
    db.flush()
    db.add_all([
        Observation(dataset_id=ds.id, subject=f"u{i}", arm="control" if i % 2 == 0 else "treatment",
                    pre_value=float(i + 1), value=float(i % 5 + (2 if i % 2 else 0)))
        for i in range(60)
    ])
    exp = Experiment(name="e", dataset_id=ds.id, hypothesis="h", owner="o", alpha=.05, cuped=False)
    db.add(exp)
    db.commit()
    return exp


def test_execute_fails_cleanly_when_statistics_reject_data(env):
    with env["SessionLocal"]() as db:
        exp = setup(db)
        # 前置指标常量会让 CUPED 拒绝；换成显式校验失败的分组。
        db.query(Observation).update({Observation.arm: "unknown"})
        db.commit()
        row = AnalysisRun(experiment_id=exp.id, status="queued", stage="queued")
        db.add(row)
        db.commit()
        claimed = run_service.claim(db, row.id, "worker-A")
        assert execution_service.execute(claimed, db) is False
        record = run_service.serialize(run_service.get_run(db, row.id))
        assert record["status"] == "failed" and "未知实验分组" in record["error"]
        assert record["viewable"] is False and record["result"] == {}
        assert record["artifact_path"] == ""


def test_commit_is_atomic_and_verified_against_record(env):
    with env["SessionLocal"]() as db:
        exp = setup(db)
        row = AnalysisRun(experiment_id=exp.id, status="queued", stage="queued")
        db.add(row)
        db.commit()
        rid = row.id
        result = {"raw": {"difference": 1.0}}
        artifact = artifact_service.commit(rid, exp.id, 1, result, now())
        # 登记前：文件存在但记录不指向它 → 不可见
        assert artifact_service._absolute(artifact["artifact_path"]).exists()
        assert run_service.serialize(run_service.get_run(db, rid))["viewable"] is False
        # 记录 attempt 与文件 attempt 不一致 → 校验失败
        assert artifact_service.verify(artifact["artifact_path"], rid, exp.id, 2,
                                       artifact["artifact_sha256"], artifact["artifact_size"]) is None
        # 大小/哈希不符 → 校验失败
        assert artifact_service.verify(artifact["artifact_path"], rid, exp.id, 1, "0" * 64,
                                       artifact["artifact_size"]) is None
        assert artifact_service.verify(artifact["artifact_path"], rid, exp.id, 1,
                                       artifact["artifact_sha256"], artifact["artifact_size"] + 1) is None
        assert artifact_service.verify(artifact["artifact_path"], rid, exp.id, 1,
                                       artifact["artifact_sha256"], artifact["artifact_size"])["result"] == result


def test_truncated_file_from_interrupted_write_is_not_visible(env):
    with env["SessionLocal"]() as db:
        exp = setup(db)
        row = AnalysisRun(experiment_id=exp.id, status="queued", stage="queued")
        db.add(row)
        db.commit()
        rid = row.id
        claimed = run_service.claim(db, rid, "worker-A")
        assert execution_service.execute(claimed, db)
        stored = run_service.get_run(db, rid)
        path = artifact_service._absolute(stored.artifact_path)
        body = path.read_bytes()
        path.write_bytes(body[: len(body) // 2])
        record = run_service.serialize(run_service.get_run(db, rid))
        assert record["status"] == "completed" and record["viewable"] is False
        actions = recovery_service.recover(db, "api")
        assert actions["requeued"] == 1
        assert run_service.serialize(run_service.get_run(db, rid))["viewable"] is False
        # 重新计算后恢复可查看，编号不变
        assert execution_service.execute(run_service.claim(db, rid, "worker-B"), db)
        record = run_service.serialize(run_service.get_run(db, rid))
        assert record["id"] == rid and record["viewable"] is True
        assert record["artifact_sha256"] == run_service.get_run(db, rid).artifact_sha256
