"""进程收到 SIGTERM 时立即交回任务；再次启动沿用同一编号完成。"""
import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def live(tmp_path):
    from fakeredis import TcpFakeServer

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = TcpFakeServer(("127.0.0.1", port), server_type="redis")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield {"redis_url": f"redis://127.0.0.1:{port}/0", "db_url": f"sqlite:///{tmp_path}/live.db",
           "artifacts": str(tmp_path / "artifacts"), "tmp": tmp_path}
    server.shutdown()
    thread.join(timeout=5)


def worker_env(live, **extra):
    env = dict(os.environ)
    env.update({
        "DATABASE_URL": live["db_url"], "REDIS_URL": live["redis_url"],
        "ARTIFACT_DIR": live["artifacts"], "SEED_DEMO": "false",
        "RUN_LEASE_SECONDS": "3", "RUN_HEARTBEAT_SECONDS": "1",
        "RUN_HEAL_INTERVAL_SECONDS": "1", "RUN_MAX_ATTEMPTS": "3",
        "WORKER_POLL_SECONDS": "0.3", "PYTHONPATH": str(ROOT),
    })
    env.update(extra)
    return env


def run_code(code, env):
    return subprocess.run([sys.executable, "-c", textwrap.dedent(code)], cwd=ROOT, env=env,
                          text=True, capture_output=True)


def test_sigterm_hands_task_back_and_restart_keeps_same_run_id(live):
    env = worker_env(live)
    prepared = run_code(
        """
        import random
        from sqlalchemy import insert
        from src.config.database import Base, SessionLocal, engine
        from src.models import Dataset, Experiment, Observation
        Base.metadata.create_all(engine)
        rng = random.Random(11)
        with SessionLocal() as db:
            ds = Dataset(name="live", source="s", description="d", checksum="c")
            db.add(ds); db.flush()
            rows = [dict(dataset_id=ds.id, subject=f"u{i}", arm="control" if i % 2 == 0 else "treatment",
                         pre_value=rng.gauss(36, 12), value=rng.gauss(40, 9)) for i in range(200000)]
            db.execute(insert(Observation), rows)
            db.add(Experiment(name="e", dataset_id=ds.id, hypothesis="h", owner="o", alpha=.05, cuped=True))
            db.commit()
        """,
        env,
    )
    assert prepared.returncode == 0, prepared.stderr[-2000:]
    engine = create_engine(live["db_url"], connect_args={"check_same_thread": False})
    sessions = sessionmaker(bind=engine, expire_on_commit=False)

    worker = subprocess.Popen([sys.executable, "-c", "from src.worker import main; main()"], cwd=ROOT,
                              env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        with sessions() as db:
            from src.models import AnalysisRun, Experiment, User
            os.environ.update({"DATABASE_URL": live["db_url"], "REDIS_URL": live["redis_url"],
                               "ARTIFACT_DIR": live["artifacts"]})
            from src.services import run_service
            exp = db.query(Experiment).first()
            rid = run_service.enqueue(db, exp.id, User(name="a", email="a@x", role="analyst"))["id"]

        def state():
            with sessions() as db:
                run = db.get(AnalysisRun, rid)
                return run

        deadline = time.time() + 40
        while time.time() < deadline:
            run = state()
            if run.status == "running" and run.claimed_by:
                break
            time.sleep(.15)
        else:
            raise AssertionError("任务未被领取")
        assert run.result == {} and not run.artifact_path
        assert run.claimed_by.startswith(socket.gethostname())

        worker.send_signal(signal.SIGTERM)
        worker.wait(timeout=15)
        run = state()
        assert run.status == "queued", "SIGTERM 后任务必须立即交回队列"
        assert "交回队列" in run.recovery_reason
        assert run.id == rid

        second = subprocess.Popen([sys.executable, "-c", "from src.worker import main; main()"], cwd=ROOT,
                                  env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            deadline = time.time() + 60
            while time.time() < deadline:
                run = state()
                if run.status == "completed":
                    break
                time.sleep(.2)
            else:
                raise AssertionError("重启后未能完成")
            assert run.id == rid and run.attempt >= 2
            assert run.artifact_path and run.artifact_sha256
            assert run.result["raw"]["n_control"] == 100000
        finally:
            second.send_signal(signal.SIGTERM)
            try:
                second.wait(timeout=10)
            except subprocess.TimeoutExpired:
                second.kill()
    finally:
        if worker.poll() is None:
            worker.kill()
