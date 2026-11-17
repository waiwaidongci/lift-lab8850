"""分析任务恢复验证：四个中断点、两个进程并发、旧进程迟到、半份结果不可见。"""
import random

import pytest

from src.models import AnalysisRun, Dataset, Experiment, Observation, Decision, User
from src.services import artifact_service, execution_service, recovery_service, run_service
from src.utils.clock import now, now_epoch


def seed(db):
    rng = random.Random(42)
    ds = Dataset(name="ds", source="s.csv", description="d", checksum="c")
    db.add(ds)
    db.flush()
    rows = []
    for i in range(240):
        arm = "control" if i % 2 == 0 else "treatment"
        pre = rng.gauss(36, 12)
        value = 7 + .65 * pre + rng.gauss(0, 7) + (2.8 if arm == "treatment" else 0)
        rows.append(Observation(dataset_id=ds.id, subject=f"u{i}", arm=arm,
                                pre_value=pre, value=value))
    db.add_all(rows)
    exp = Experiment(name="exp", dataset_id=ds.id, hypothesis="h", owner="analyst", alpha=.05, cuped=True)
    db.add(exp)
    db.commit()
    return exp


def make_run(db, exp, **fields):
    values = {"experiment_id": exp.id, "status": "queued", "stage": "queued", "queue_signal_at": 0.0}
    values.update(fields)
    row = AnalysisRun(**values)
    db.add(row)
    db.commit()
    return row


def view(db, run_id):
    return run_service.serialize(run_service.get_run(db, run_id))


def claim_as(db, run_id, owner="worker-A"):
    return run_service.claim(db, run_id, owner)


def run_claimed(db, claimed):
    assert execution_service.execute(claimed, db)


# ---------- 登记后未入队 ----------

def test_run_enqueued_only_after_commit_and_redispatched_when_signal_lost(env):
    with env["SessionLocal"]() as db:
        seed(db)
    with env["SessionLocal"]() as db:
        exp = db.query(Experiment).first()
        row = run_service.enqueue(db, exp.id, User(name="a", email="a@x", role="analyst"))
        rid = row["id"]
        assert row["status"] == "queued" and row["dispatched"] is True
        assert env["redis"].zscore(run_service.QUEUE_KEY, str(rid)) is not None
    # 模拟队列信号丢失（Redis 重启/入队事务已提交但信号未写出）
    env["redis"].delete(run_service.QUEUE_KEY)
    with env["SessionLocal"]() as db:
        row = db.get(AnalysisRun, rid)
        row.queue_signal_at = 0.0
        db.commit()
        actions = recovery_service.recover(db, "api")
        assert actions["redispatched"] == 1
        assert env["redis"].zscore(run_service.QUEUE_KEY, str(rid)) is not None
        # 重复恢复不会重复入队
        actions = recovery_service.recover(db, "api")
        assert actions["redispatched"] == 0
        assert env["redis"].zcard(run_service.QUEUE_KEY) == 1
        # 队列信号恢复后仍沿用同一分析编号：计算进程按编号领取并完成
        claimed = run_service.claim(db, rid, "worker-A")
        assert claimed.run_id == rid and claimed.attempt == 1
        assert execution_service.execute(claimed, db)
        record = view(db, rid)
        assert record["status"] == "completed" and record["viewable"] is True
        assert record["progress"] == 100


def test_redis_unavailable_keeps_run_recoverable(env, monkeypatch):
    from src.services import run_service as rs

    class Broken:
        def zadd(self, *a, **k):
            raise ConnectionError("redis down")

    with env["SessionLocal"]() as db:
        exp = seed(db)
        monkeypatch.setattr(rs, "queue", Broken())
        row = rs.enqueue(db, exp.id, User(name="a", email="a@x", role="analyst"))
        assert row["status"] == "queued" and row["dispatched"] is False and row["recoverable"] is True
        monkeypatch.undo()
        rs.queue = env["redis"]
        actions = recovery_service.recover(db, "api")
        assert actions["redispatched"] == 1


# ---------- 计算中退出 ----------

def test_crash_while_computing_is_taken_over_and_same_run_id_completed(env):
    with env["SessionLocal"]() as db:
        seed(db)
        row = make_run(db, db.query(Experiment).first())
        rid = row.id
        first = claim_as(db, rid, "worker-A")
        assert first.attempt == 1
        run_service.heartbeat(first, "computing", db)
        # 进程崩溃：不再心跳，租约过期
        db.execute(AnalysisRun.__table__.update().where(AnalysisRun.id == rid).values(lease_expires_at=now_epoch() - 1))
        db.commit()
        actions = recovery_service.recover(db, "worker-B")
        assert actions["requeued"] == 1
        record = view(db, rid)
        assert record["status"] == "queued" and record["recovery_count"] == 1
        assert "失联" in record["recovery_reason"]
        second = claim_as(db, rid, "worker-B")
        assert second.run_id == rid and second.attempt == 2
        assert execution_service.execute(second, db)
        record = view(db, rid)
        assert record["status"] == "completed" and record["viewable"] is True
        assert record["attempt"] == 2


def test_repeated_crashes_exhaust_attempts_and_report_reason(env):
    with env["SessionLocal"]() as db:
        seed(db)
        row = make_run(db, db.query(Experiment).first(), status="running",
                       attempt=3, claimed_by="worker-A", lease_expires_at=now_epoch() - 1)
        rid = row.id
        actions = recovery_service.recover(db, "api")
        assert actions["expired"] == 1
        record = view(db, rid)
        assert record["status"] == "failed" and record["stage_label"] == "计算未能完成"
        assert "3 次" in record["error"] and record["viewable"] is False


# ---------- 写出后未完成 ----------

def test_result_written_but_not_committed_is_invisible_until_recovered(env):
    with env["SessionLocal"]() as db:
        seed(db)
        exp = db.query(Experiment).first()
        row = make_run(db, exp, status="running", attempt=1, claimed_by="worker-A",
                       lease_expires_at=now_epoch() + 60, stage="verifying")
        rid = row.id
        # 结果文件已写出，但分析记录尚未登记（进程在 commit 前退出）
        result = {"raw": {"difference": 1.0}, "cuped": None}
        artifact_service.commit(rid, exp.id, 1, result, now())
        assert view(db, rid)["viewable"] is False
        assert view(db, rid)["result"] == {}
        # 管理端各类出口都拒绝半份结果
        with pytest.raises(Exception) as exc:
            run_service.export_run(db, rid)
        assert "尚未完成" in str(exc.value)
        # 租约过期 → 恢复为排队 → 接管后产出完整结果
        db.execute(AnalysisRun.__table__.update().where(AnalysisRun.id == rid).values(lease_expires_at=now_epoch() - 1))
        db.commit()
        assert recovery_service.recover(db, "api")["requeued"] == 1
        second = claim_as(db, rid, "worker-B")
        assert execution_service.execute(second, db)
        record = view(db, rid)
        assert record["viewable"] and record["result"]["raw"]["n_control"] == 120
        assert record["artifact_sha256"] == run_service.get_run(db, rid).artifact_sha256
        # 旧 attempt 文件被清理
        assert not artifact_service._absolute(artifact_service.relative_path(rid, 1)).exists()


# ---------- 旧进程晚返回 ----------

def test_late_worker_cannot_overwrite_takeover_result(env):
    with env["SessionLocal"]() as db:
        seed(db)
        exp = db.query(Experiment).first()
        row = make_run(db, exp)
        rid = row.id
        slow = claim_as(db, rid, "worker-A")
        db.execute(AnalysisRun.__table__.update().where(AnalysisRun.id == rid).values(lease_expires_at=now_epoch() - 1))
        db.commit()
        recovery_service.recover(db, "api")
        fast = claim_as(db, rid, "worker-B")
        assert fast.attempt == 2
        assert execution_service.execute(fast, db)
        good = view(db, rid)
        assert good["viewable"] and good["attempt"] == 2
        good_hash = good["artifact_sha256"]

        # 旧进程苏醒：仍按自己的 attempt 写文件并尝试登记
        stale_result = {"raw": {"difference": -99.0}, "cuped": None}
        artifact = artifact_service.commit(rid, exp.id, slow.attempt, stale_result, now())
        assert artifact_service._absolute(artifact["artifact_path"]).exists()
        assert run_service.finish(slow, stale_result, artifact, db) is False
        assert not artifact_service._absolute(artifact["artifact_path"]).exists()
        assert run_service.fail(slow, "stale failure", db) is False
        record = view(db, rid)
        assert record["status"] == "completed" and record["attempt"] == 2
        assert record["artifact_sha256"] == good_hash
        assert record["result"]["raw"]["difference"] != -99.0
        assert record["error"] == ""


# ---------- 两个执行进程并发接管 ----------

def test_two_workers_race_only_one_takeover_wins(env):
    with env["SessionLocal"]() as db:
        seed(db)
        exp = db.query(Experiment).first()
        rid = make_run(db, exp).id
        first = claim_as(db, rid, "worker-A")
        db.execute(AnalysisRun.__table__.update().where(AnalysisRun.id == rid).values(lease_expires_at=now_epoch() - 1))
        db.commit()
        left = run_service.claim(db, rid, "worker-B")
        right = run_service.claim(db, rid, "worker-C")
        others = [c for c in (left, right) if c]
        assert len(others) == 1, "同一任务只能被一个进程接管"
        winner = others[0]
        assert winner.attempt == 2
        assert execution_service.execute(winner, db)
        # 输掉竞争的一方位次已失效，写入被拒绝
        assert run_service.finish(first, {}, {}, db) is False
        assert view(db, rid)["viewable"] is True


def test_second_worker_processes_other_jobs_concurrently(env):
    with env["SessionLocal"]() as db:
        seed(db)
        exp = db.query(Experiment).first()
        make_run(db, exp)
        make_run(db, exp)
        a = claim_as(db, run_service.get_run(db, 1).id, "worker-A")
        b = claim_as(db, run_service.get_run(db, 2).id, "worker-B")
        assert a.attempt == b.attempt == 1
        assert execution_service.execute(a, db) and execution_service.execute(b, db)
        assert view(db, 1)["viewable"] and view(db, 2)["viewable"]


# ---------- 阅读、导出、决策门禁 ----------

def test_view_export_and_decision_are_gated_on_complete_artifact(env):
    with env["SessionLocal"]() as db:
        seed(db)
        exp = db.query(Experiment).first()
        row = make_run(db, exp)
        rid = row.id
        with pytest.raises(Exception):
            run_service.export_run(db, rid)
        claimed = claim_as(db, rid, "worker-A")
        assert execution_service.execute(claimed, db)
        csv_body = run_service.export_run(db, rid)
        assert "raw" in csv_body and "cuped" in csv_body
        stored = run_service.get_run(db, rid)
        # 破坏产物：查看返回结构但不再带结果，导出门禁 409
        artifact_service._absolute(stored.artifact_path).write_text("{}")
        record = view(db, rid)
        assert record["status"] == "completed" and record["viewable"] is False
        assert record["result"] == {} and record["result_ready"] is False
        with pytest.raises(Exception) as exc:
            run_service.export_run(db, rid)
        assert "校验失败" in str(exc.value)
        # 决策引用同样拒绝
        from src.services.decision_service import create_decision
        from src.schemas.decision import DecisionInput
        with pytest.raises(Exception):
            create_decision(db, DecisionInput(run_id=rid, outcome="ship", note="证据完整才可放量"), User(name="林序", email="admin@x", role="admin"))
        # 恢复扫描发现哈希不符 → 重新计算 → 结果再次完整
        assert recovery_service.recover(db, "api")["requeued"] == 1
        reclaimed = claim_as(db, rid, "worker-B")
        assert execution_service.execute(reclaimed, db)
        assert view(db, rid)["viewable"] is True
        create_decision(db, DecisionInput(run_id=rid, outcome="ship", note="证据完整才可放量"), User(name="林序", email="admin@x", role="admin"))
        assert db.query(Decision).count() == 1
        assert view(db, rid)["progress"] == 100
