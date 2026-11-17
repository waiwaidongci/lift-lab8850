import os

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg://lift:lift@db/lift")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
JWT_SECRET = os.environ.get("JWT_SECRET", "local-development-change-this-key")
SEED_DEMO = os.environ.get("SEED_DEMO", "true").lower() == "true"

# 分析任务恢复：租约、接管上限与产物目录。
ARTIFACT_DIR = os.environ.get("ARTIFACT_DIR", "/artifacts")
RUN_LEASE_SECONDS = float(os.environ.get("RUN_LEASE_SECONDS", "30"))
RUN_MAX_ATTEMPTS = int(os.environ.get("RUN_MAX_ATTEMPTS", "3"))
RUN_HEAL_INTERVAL_SECONDS = float(os.environ.get("RUN_HEAL_INTERVAL_SECONDS", "30"))
RUN_HEARTBEAT_SECONDS = float(os.environ.get("RUN_HEARTBEAT_SECONDS", "10"))
WORKER_POLL_SECONDS = float(os.environ.get("WORKER_POLL_SECONDS", "2"))
