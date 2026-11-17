"""分析计算进程。

- 用两把槽位锁限制同一时刻最多两个执行进程并行；
- 领取任务时原子递增 attempt（fencing token），租约过期可被其他进程接管；
- 心跳续租、阶段进度写入数据库；
- 结果先原子写入 attempt 文件，再带 attempt/owner 条件登记，迟到结果无法覆盖；
- SIGTERM/SIGINT 时把未完成任务立即交回队列，恢复时沿用同一条分析编号。
"""
import logging
import os
import signal as os_signal
import socket
import threading
import time
from pathlib import Path

from .config import database
from .config.database import Base
from .config.settings import (
    ARTIFACT_DIR,
    RUN_HEARTBEAT_SECONDS,
    RUN_HEAL_INTERVAL_SECONDS,
    WORKER_POLL_SECONDS,
)
from .services.recovery_service import recover
from .services.execution_service import execute
from .services.run_service import (
    QUEUE_KEY,
    Claim,
    claim,
    heartbeat,
    queue,
    release,
    signal,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log_ = logging.getLogger("lift.worker")

SLOT_KEYS = ("lift:worker-slot:1", "lift:worker-slot:2")
SLOT_TTL_SECONDS = 60
OWNER = f"{socket.gethostname()}:{os.getpid()}:{int(time.time())}"

_stop = threading.Event()
_current: Claim | None = None
_current_lock = threading.Lock()


def _acquire_slot() -> str | None:
    for key in SLOT_KEYS:
        if queue.set(key, OWNER, nx=True, ex=SLOT_TTL_SECONDS):
            return key
    return None


def _renew_slot(key: str) -> None:
    try:
        queue.expire(key, SLOT_TTL_SECONDS)
    except Exception:
        log_.warning("槽位续期失败")


def _release_slot(key: str) -> None:
    try:
        if queue.get(key) == OWNER:
            queue.delete(key)
    except Exception:
        pass


def _write_heartbeat() -> None:
    # 每个计算进程写自己的心跳文件，容器健康检查不会误读同卷其他进程的心跳。
    Path(ARTIFACT_DIR, "heartbeat").write_text(str(time.time()))
    Path(ARTIFACT_DIR, f"heartbeat-{socket.gethostname()}").write_text(str(time.time()))


def _recover_loop() -> None:
    while not _stop.is_set():
        try:
            with database.SessionLocal() as db:
                actions = recover(db, OWNER)
            if any(actions.values()):
                log_.info("恢复扫描：%s", actions)
        except Exception:
            log_.exception("恢复扫描失败")
        _stop.wait(RUN_HEAL_INTERVAL_SECONDS)


def _execute(claim: Claim) -> None:
    with database.SessionLocal() as db:
        if execute(claim, db):
            log_.info("RUN-%s 完成 attempt=%s", claim.run_id, claim.attempt)


def _run_once() -> bool:
    global _current
    job = queue.zpopmin(QUEUE_KEY)
    if not job:
        return False
    try:
        run_id = int(job[0][0])
    except (TypeError, ValueError, IndexError):
        log_.warning("忽略非法队列成员：%r", job)
        return True
    slot = None
    while slot is None and not _stop.is_set():
        slot = _acquire_slot()
        if slot is None:
            time.sleep(.2)
    if slot is None:
        # 正在退出：把任务原样放回队列，其他进程仍可领取。
        queue.zadd(QUEUE_KEY, {str(run_id): time.time()}, nx=True)
        return True
    with database.SessionLocal() as db:
        claimed = claim(db, run_id, OWNER)
    if not claimed:
        _release_slot(slot)
        return True
    with _current_lock:
        _current = claimed
    renewed = threading.Event()

    def _keep_alive() -> None:
        while not renewed.wait(RUN_HEARTBEAT_SECONDS):
            try:
                with database.SessionLocal() as db:
                    heartbeat(claimed, None, db)
                _renew_slot(slot)
            except Exception:
                log_.exception("心跳失败 RUN-%s", claimed.run_id)

    keeper = threading.Thread(target=_keep_alive, daemon=True)
    keeper.start()
    try:
        _execute(claimed)
    finally:
        renewed.set()
        keeper.join(timeout=2)
        with _current_lock:
            _current = None
        _release_slot(slot)
    return True


def _shutdown(signum, _frame) -> None:
    log_.info("收到信号 %s，准备交回未完成任务", signum)
    _stop.set()
    with _current_lock:
        claimed = _current
    if claimed:
        try:
            with database.SessionLocal() as db:
                if release(claimed, "计算进程退出，任务已交回队列", db):
                    log_.info("RUN-%s 已交回队列", claimed.run_id)
                    signal(claimed.run_id)
        except Exception:
            log_.exception("交回任务失败 RUN-%s", claimed.run_id)


def main() -> None:
    Base.metadata.create_all(database.engine)
    Path(ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)
    for sig in (os_signal.SIGTERM, os_signal.SIGINT):
        os_signal.signal(sig, _shutdown)
    _write_heartbeat()
    healing = threading.Thread(target=_recover_loop, daemon=True)
    healing.start()
    log_.info("计算进程 %s 启动", OWNER)
    while not _stop.is_set():
        _write_heartbeat()
        try:
            # 队列里没有新任务时，扫描一次排队中但未入队的记录并补投递。
            if not _run_once() and not _stop.is_set():
                with database.SessionLocal() as db:
                    actions = recover(db, OWNER)
                if any(actions.values()):
                    log_.info("轮询恢复：%s", actions)
                _stop.wait(WORKER_POLL_SECONDS)
        except Exception:
            log_.exception("计算循环异常")
            _stop.wait(WORKER_POLL_SECONDS)
    log_.info("计算进程退出")


if __name__ == "__main__":
    main()
