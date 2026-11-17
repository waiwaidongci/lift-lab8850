import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from sqlalchemy import text

from . import models  # noqa: F401  建表需要先导入全部模型
from .config import database
from .config.database import Base
from .config.settings import ARTIFACT_DIR, RUN_HEAL_INTERVAL_SECONDS, SEED_DEMO
from .middlewares.errors import ErrorMiddleware
from .middlewares.rate_limit import RateLimitMiddleware
from .routes import auth, datasets, experiments, runs, decisions, audits
from .services import run_service
from .services.recovery_service import recover

logger=logging.getLogger("lift.api")
async def heal_once():
    # 恢复扫描也由 API 兜底执行：即使 worker 未启动，登记后未入队或计算中失联的分析
    # 仍会补投递；真正计算仍只由计算进程执行。
    try:
        with database.SessionLocal() as db:
            actions=recover(db,"api")
        if any(actions.values()): logger.info("恢复扫描：%s",actions)
    except Exception: logger.exception("恢复扫描失败")
async def heal_loop():
    while True:
        await heal_once()
        await asyncio.sleep(RUN_HEAL_INTERVAL_SECONDS)
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(database.engine)
    Path(ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)
    if SEED_DEMO:
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception: logging.exception("demo seed failed")
    task=asyncio.create_task(heal_loop())
    yield
    task.cancel()
    try: await task
    except asyncio.CancelledError: pass
app=FastAPI(title="岚桥实验放量台",lifespan=lifespan)
app.add_middleware(ErrorMiddleware)
app.add_middleware(RateLimitMiddleware)
for route in [auth,datasets,experiments,runs,decisions,audits]: app.include_router(route.router)
@app.get("/health")
def health():
    with database.engine.connect() as conn: conn.execute(text("select 1"))
    run_service.queue.ping()
    return {"status":"healthy","service":"lift-lab"}
