"""分析任务自愈：补投递、回收失联租约、终止重试耗尽的任务。

恢复线程只修改数据库中的任务状态并重新投递队列信号；真正执行仍由计算进程
完成。所有状态迁移都带 attempt/owner 条件，两个进程同时恢复不会互相覆盖。
"""
import logging
from pathlib import Path

from sqlalchemy import update

from ..config.settings import ARTIFACT_DIR, RUN_LEASE_SECONDS, RUN_MAX_ATTEMPTS
from ..models import AnalysisRun
from ..utils.clock import now, now_epoch
from . import artifact_service, run_service
from .audit_service import log

log_ = logging.getLogger("lift.recovery")


def _worker_alive() -> bool:
    """共享心跳文件由计算进程持续刷新；API 侧据此避免与 worker 抢着扫描。"""
    try:
        return now_epoch() - float(Path(ARTIFACT_DIR, "heartbeat").read_text()) < RUN_LEASE_SECONDS * 2
    except (OSError, ValueError):
        return False


def _dispatch(db, run_id: int, stamp: float) -> bool:
    """补投递信号；若任务在投递瞬间已完成，立刻撤回这条冗余信号。"""
    if not run_service.signal(run_id):
        return False
    updated = db.execute(
        update(AnalysisRun)
        .where(AnalysisRun.id == run_id)
        .where(AnalysisRun.status == "queued")
        .values(queue_signal_at=stamp)
    )
    db.commit()
    if updated.rowcount != 1:
        try:
            run_service.queue.zrem(run_service.QUEUE_KEY, str(run_id))
        except Exception:
            log_.warning("撤回冗余队列信号失败 RUN-%s", run_id)
        return False
    return True


def recover(db, owner: str, stale_seconds: float = RUN_LEASE_SECONDS) -> dict:
    """一轮恢复：返回本轮处理的动作计数。

    owner 为 ``api`` 时，若计算进程仍在刷新心跳则跳过：worker 自身的轮询与
    恢复线程已经覆盖补投递，API 只需在 worker 缺席时兜底。
    """
    actions = {"requeued": 0, "redispatched": 0, "expired": 0}
    if owner == "api" and _worker_alive():
        return actions
    stamp = now_epoch()
    try:
        members = set(run_service.queue.zrange(run_service.QUEUE_KEY, 0, -1))
    except Exception:
        log_.warning("队列不可读，本轮跳过补投递")
        members = set()

    # 1) 已完成登记的产物保持完成；状态完成但文件缺失/哈希不符 → 重新排队。
    # 只读取记录中登记的路径，不扫描目录，避免把用户数据目录当作产物目录。
    recent = (
        db.query(AnalysisRun)
        .filter(AnalysisRun.status == "completed")
        .order_by(AnalysisRun.id.desc())
        .limit(500)
        .all()
    )
    for row in recent:
        if not row.artifact_path:
            continue
        verified = artifact_service.verify(
            row.artifact_path, row.id, row.experiment_id, row.attempt,
            row.artifact_sha256, row.artifact_size,
        )
        if verified:
            continue
        if row.attempt >= RUN_MAX_ATTEMPTS:
            failed = db.execute(
                update(AnalysisRun)
                .where(AnalysisRun.id == row.id)
                .where(AnalysisRun.status == "completed")
                .values(
                    status="failed",
                    stage="failed",
                    result={},
                    error="结果文件缺失或校验失败，且已用尽自动重新计算次数",
                    updated_at=now(),
                    queue_signal_at=0.0,
                )
            )
            db.commit()
            if failed.rowcount == 1:
                actions["expired"] += 1
                log(db, "recovery", "analysis.exhausted", row.id)
                db.commit()
            continue
        reset = db.execute(
            update(AnalysisRun)
            .where(AnalysisRun.id == row.id)
            .where(AnalysisRun.status == "completed")
            .values(
                status="queued",
                stage="queued",
                attempt=AnalysisRun.attempt + 1,
                result={},
                error="结果文件校验失败，已自动重新计算",
                artifact_path="",
                artifact_sha256="",
                artifact_size=0,
                updated_at=now(),
                recovery_count=AnalysisRun.recovery_count + 1,
                recovery_reason="结果文件与分析记录不一致，重新计算",
                queue_signal_at=0.0,
            )
        )
        db.commit()
        if reset.rowcount == 1:
            actions["requeued"] += 1
            _dispatch(db, row.id, stamp)
            log(db, "recovery", "analysis.requeued", row.id)
            db.commit()

    # 2) 登记后未入队：数据库有排队记录但队列里没有信号。
    queued = db.query(AnalysisRun).filter(AnalysisRun.status == "queued").all()
    for row in queued:
        if row.attempt >= RUN_MAX_ATTEMPTS:
            # 排队状态下已无机会再被领取：直接给出明确失败原因。
            failed = db.execute(
                update(AnalysisRun)
                .where(AnalysisRun.id == row.id)
                .where(AnalysisRun.status == "queued")
                .values(
                    status="failed",
                    stage="failed",
                    error=f"计算任务多次中断，已用尽 {RUN_MAX_ATTEMPTS} 次尝试",
                    updated_at=now(),
                    queue_signal_at=0.0,
                )
            )
            db.commit()
            if failed.rowcount == 1:
                actions["expired"] += 1
                log(db, "recovery", "analysis.exhausted", row.id)
                db.commit()
            continue
        if str(row.id) in members:
            continue
        if _dispatch(db, row.id, stamp):
            actions["redispatched"] += 1
            if row.attempt == 0:
                log(db, "recovery", "analysis.dispatch", row.id)
                db.commit()

    # 3) 计算中失联：租约过期 → 交回队列（由任意计算进程重新领取）。
    now_iso = now()
    expired = db.query(AnalysisRun).filter(
        AnalysisRun.status == "running",
        AnalysisRun.lease_expires_at < stamp,
    ).all()
    for row in expired:
        if row.attempt >= RUN_MAX_ATTEMPTS:
            updated = db.execute(
                update(AnalysisRun)
                .where(AnalysisRun.id == row.id)
                .where(AnalysisRun.status == "running")
                .where(AnalysisRun.attempt == row.attempt)
                .values(
                    status="failed",
                    stage="failed",
                    error=f"计算进程连续失联，已用尽 {RUN_MAX_ATTEMPTS} 次尝试",
                    claimed_by="",
                    lease_expires_at=0.0,
                    updated_at=now_iso,
                    queue_signal_at=0.0,
                )
            )
            db.commit()
            if updated.rowcount == 1:
                actions["expired"] += 1
                log(db, "recovery", "analysis.exhausted", row.id)
                db.commit()
            continue
        updated = db.execute(
            update(AnalysisRun)
            .where(AnalysisRun.id == row.id)
            .where(AnalysisRun.status == "running")
            .where(AnalysisRun.attempt == row.attempt)
            .values(
                status="queued",
                stage="queued",
                claimed_by="",
                lease_expires_at=0.0,
                updated_at=now_iso,
                heartbeat_at=now_iso,
                queue_signal_at=0.0,
                recovery_count=AnalysisRun.recovery_count + 1,
                recovery_reason=f"执行进程 {row.claimed_by or '未知'} 失联超过 {int(stale_seconds)} 秒，任务已自动交回队列",
            )
        )
        db.commit()
        if updated.rowcount == 1:
            actions["requeued"] += 1
            _dispatch(db, row.id, stamp)
            log(db, "recovery", "analysis.requeued", row.id)
            db.commit()
    return actions

