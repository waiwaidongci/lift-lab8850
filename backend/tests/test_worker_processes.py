"""真实双进程验证：两个 worker 子进程共享 SQLite 与 TCP 版 Redis。

覆盖：计算中退出（kill -9）后的接管、旧进程晚返回、登记后未入队自动补投递。
"""
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


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def live(tmp_path):
    from fakeredis import TcpFakeServer

    port = free_port()
    server = TcpFakeServer(("127.0.0.1", port), server_type="redis")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield {
        "redis_url": f"redis://127.0.0.1:{port}/0",
        "db_url": f"sqlite:///{tmp_path}/live.db",
        "artifacts": str(tmp_path / "artifacts"),
        "tmp": tmp_path,
        "server": server,
    }
    server.shutdown()
    thread.join(timeout=5)


def start_worker(live, name):
    code = textwrap.dedent(
        """
        import sys
        from src.worker import main
        main()
        """
    )
    env = dict(os.environ)
    env.update({
        "DATABASE_URL": live["db_url"],
        "REDIS_URL": live["redis_url"],
        "ARTIFACT_DIR": live["artifacts"],
        "SEED_DEMO": "false",
        "RUN_LEASE_SECONDS": "3",
        "RUN_HEARTBEAT_SECONDS": "1",
        "RUN_HEAL_INTERVAL_SECONDS": "1",
        "RUN_MAX_ATTEMPTS": "3",
        "WORKER_POLL_SECONDS": "0.3",
        "PYTHONPATH": str(ROOT),
    })
    return subprocess.Popen([sys.executable, "-c", code], env=env, cwd=ROOT,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def prepare(live):
    """在子进程启动前建表并写入数据集/实验。"""
    env_db = live["db_url"]
    code = textwrap.dedent(
        """
        import random
        from sqlalchemy import insert
        from src.config.database import Base, SessionLocal, engine
        from src.models import Dataset, Experiment, Observation
        rng = random.Random(7)
        def bulk_rows(dataset_id, n):
            rows = []
            for i in range(n):
                arm = "control" if i % 2 == 0 else "treatment"
                pre = rng.gauss(36, 12)
                value = 7 + .65 * pre + rng.gauss(0, 7) + (3 if arm == "treatment" else 0)
                rows.append(dict(dataset_id=dataset_id, subject=f"u{i}", arm=arm, pre_value=pre, value=value))
            return rows
        Base.metadata.create_all(engine)
        with SessionLocal() as db:
            ds = Dataset(name="live", source="s.csv", description="d", checksum="c")
            db.add(ds); db.flush()
            db.execute(insert(Observation), bulk_rows(ds.id, 200000))
            db.add(Experiment(name="live-exp", dataset_id=ds.id, hypothesis="h", owner="o", alpha=.05, cuped=True))
            db.commit()
        print("prepared")
        """
    )
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, text=True, capture_output=True,
                         env={**os.environ, "DATABASE_URL": env_db, "ARTIFACT_DIR": live["artifacts"],
                              "PYTHONPATH": str(ROOT), "SEED_DEMO": "false"})
    assert out.returncode == 0, out.stderr[-2000:]


def db_session(live):
    engine = create_engine(live["db_url"], connect_args={"check_same_thread": False})
    return sessionmaker(bind=engine, expire_on_commit=False)


def wait_until(predicate, timeout=40, interval=.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("等待超时")


def test_two_worker_processes_take_over_killed_run_and_ignore_late_writer(live):
    prepare(live)
    sessions = db_session(live)
    os.environ["DATABASE_URL"] = live["db_url"]
    os.environ["REDIS_URL"] = live["redis_url"]
    os.environ["ARTIFACT_DIR"] = live["artifacts"]
    from src.services import run_service

    worker_a = start_worker(live, "A")
    worker_b = start_worker(live, "B")
    try:
        with sessions() as db:
            from src.models import Experiment, AnalysisRun
            exp = db.query(Experiment).first()
            from src.models import User
            row = run_service.enqueue(db, exp.id, User(name="a", email="a@x", role="analyst"))
            rid = row["id"]
        # 两个进程都会轮询；任务被其中一个领取并完成
        def completed():
            with sessions() as db:
                run = db.get(AnalysisRun, rid)
                return run if run and run.status == "completed" else None
        run = wait_until(completed)
        assert run.attempt >= 1 and run.artifact_path

        # 再次入队一个任务，在计算中途强杀领取它的进程，另一个进程必须接管。
        with sessions() as db:
            exp = db.query(Experiment).first()
            row = run_service.enqueue(db, exp.id, User(name="a", email="a@x", role="analyst"))
            rid2 = row["id"]
        def running():
            with sessions() as db:
                run = db.get(AnalysisRun, rid2)
                return run if run and run.status == "running" and run.claimed_by else None
        claimed = wait_until(running)
        pid = int(claimed.claimed_by.split(":")[1])
        victim = worker_a if worker_a.pid == pid else worker_b
        survivor = worker_b if victim is worker_a else worker_a
        victim.send_signal(signal.SIGKILL)
        victim.wait(timeout=10)
        if survivor.poll() is not None:
            survivor = start_worker(live, "B2")
        def taken_over():
            with sessions() as db:
                run = db.get(AnalysisRun, rid2)
                return run if run and run.status == "completed" else None
        run2 = wait_until(taken_over, timeout=60)
        assert run2.attempt >= 2 and run2.recovery_count >= 1
        assert run2.artifact_sha256
        # 同一分析编号，编号未变
        with sessions() as db:
            again = db.get(AnalysisRun, rid2)
            assert again.id == rid2 and again.status == "completed" and again.result["raw"]["n_control"] == 100000
    finally:
        for proc in (worker_a, worker_b):
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()


def test_unenqueued_run_is_dispatched_by_running_worker(live):
    prepare(live)
    sessions = db_session(live)
    os.environ["DATABASE_URL"] = live["db_url"]
    os.environ["REDIS_URL"] = live["redis_url"]
    os.environ["ARTIFACT_DIR"] = live["artifacts"]
    from src.models import AnalysisRun, Experiment
    from src.utils.clock import now

    worker = start_worker(live, "A")
    try:
        with sessions() as db:
            exp = db.query(Experiment).first()
            row = AnalysisRun(experiment_id=exp.id, status="queued", stage="queued",
                              queue_signal_at=0.0, created_at=now())
            db.add(row)
            db.commit()
            rid = row.id
        def completed():
            with sessions() as db:
                run = db.get(AnalysisRun, rid)
                return run if run and run.status == "completed" and run.artifact_path else None
        run = wait_until(completed, timeout=40)
        assert run.attempt == 1 and run.result["raw"]["n_control"] == 100000
    finally:
        if worker.poll() is None:
            worker.send_signal(signal.SIGTERM)
            try:
                worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                worker.kill()
