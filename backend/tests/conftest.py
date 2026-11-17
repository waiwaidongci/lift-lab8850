import os
import tempfile

# 必须在导入 src.* 之前设置环境变量：独立产物目录与较短的恢复参数。
_TMP = tempfile.mkdtemp(prefix="lift-tests-")
os.environ.setdefault("ARTIFACT_DIR", f"{_TMP}/artifacts")
os.environ["ARTIFACT_DIR"] = f"{_TMP}/artifacts"
os.environ["DATABASE_URL"] = "sqlite://"
os.environ["SEED_DEMO"] = "false"
os.environ["RUN_LEASE_SECONDS"] = "2"
os.environ["RUN_MAX_ATTEMPTS"] = "3"
os.environ["RUN_HEAL_INTERVAL_SECONDS"] = "1"
os.environ["RUN_HEARTBEAT_SECONDS"] = "1"

import fakeredis
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


@pytest.fixture()
def server():
    """fakeredis 服务器，让多个客户端共享同一后端。"""
    return fakeredis.FakeServer()


@pytest.fixture()
def db_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    from src.config.database import Base
    from src import models  # noqa: F401

    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture()
def env(server, db_factory, monkeypatch):
    """把 run_service / 数据库使用的会话与队列替换为测试后端。"""
    import fakeredis as _fakeredis
    from src.config import database
    from src.services import run_service

    client = _fakeredis.FakeRedis(server=server, decode_responses=True)
    monkeypatch.setattr(run_service, "queue", client)
    # worker / 恢复线程 / 接口都通过 database.SessionLocal 取会话。
    monkeypatch.setattr(database, "SessionLocal", db_factory)
    monkeypatch.setattr(database, "engine", db_factory.kw["bind"])
    return {"server": server, "redis": client, "SessionLocal": db_factory}
