"""接口层验证：未完成/半份结果不能查看、导出或引用；恢复进度与原因可读。"""
from fastapi.testclient import TestClient
from sqlalchemy import update

from src.models import AnalysisRun, Dataset, Experiment, Observation, User
from src.services import run_service
from src.utils.clock import now_epoch
from src.utils.security import hash_password


def auth(client, email, password="Lanqiao2026!"):
    response = client.post("/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    return {"Authorization": "Bearer " + response.json()["token"]}


def test_api_hides_half_results_and_reports_recovery(env):
    import random
    from src.main import app

    with env["SessionLocal"]() as db:
        db.add(User(name="林序", email="admin@x", role="admin", password_hash=hash_password("Lanqiao2026!")))
        db.add(User(name="周澄", email="viewer@x", role="viewer", password_hash=hash_password("Lanqiao2026!")))
        rng = random.Random(3)
        ds = Dataset(name="api", source="s", description="d", checksum="c")
        db.add(ds)
        db.flush()
        db.add_all([
            Observation(dataset_id=ds.id, subject=f"u{i}", arm="control" if i % 2 == 0 else "treatment",
                        pre_value=rng.gauss(36, 12), value=rng.gauss(40, 9))
            for i in range(200)
        ])
        exp = Experiment(name="api-exp", dataset_id=ds.id, hypothesis="h", owner="分析师", alpha=.05, cuped=True)
        db.add(exp)
        db.commit()
        exp_id = exp.id
    client = TestClient(app)
    headers = auth(client, "admin@x")

    created = client.post(f"/experiments/{exp_id}/runs", headers=headers)
    assert created.status_code == 202, created.text
    run = created.json()
    rid = run["id"]
    assert run["status"] == "queued" and run["viewable"] is False and run["progress"] == 8
    assert run["dispatched"] is True and run["stage_label"] == "等待计算进程领取"

    assert client.get(f"/runs/{rid}/export", headers=headers).status_code == 409
    listing = client.get("/runs", headers=headers).json()
    assert listing[0]["viewable"] is False and listing[0]["result"] == {}

    # 计算中崩溃：接管前的进度与原因对页面可见
    assert run_service.claim(env["SessionLocal"](), rid, "worker-A")
    body = client.get(f"/runs/{rid}", headers=headers).json()
    assert body["status"] == "running" and body["stage_label"] == "加载实验与观测数据"
    with env["SessionLocal"]() as db:
        db.execute(update(AnalysisRun).where(AnalysisRun.id == rid).values(lease_expires_at=now_epoch() - 1))
        db.commit()
        from src.services.recovery_service import recover
        assert recover(db, "api")["requeued"] == 1
    body = client.get(f"/runs/{rid}", headers=headers).json()
    assert body["status"] == "queued" and body["recovery_count"] == 1
    assert "失联" in body["recovery_reason"]

    # 完成并校验后：查看、导出、决策均开放
    from src.services.execution_service import execute
    with env["SessionLocal"]() as db:
        assert execute(run_service.claim(db, rid, "worker-B"), db)
    body = client.get(f"/runs/{rid}", headers=headers).json()
    assert body["viewable"] is True and body["progress"] == 100 and body["result"]["raw"]["n_control"] == 100
    exported = client.get(f"/runs/{rid}/export", headers=headers)
    assert exported.status_code == 200 and "raw" in exported.text
    decision = client.post("/decisions", headers=headers, json={"run_id": rid, "outcome": "ship", "note": "区间与样本量完整，同意放量"})
    assert decision.status_code == 201, decision.text
    viewer = auth(client, "viewer@x")
    assert client.post("/decisions", headers=viewer, json={"run_id": rid, "outcome": "ship", "note": "观察员不应能归档"}).status_code == 403
