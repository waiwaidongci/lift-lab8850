import csv
import io
from collections import namedtuple

from fastapi import HTTPException
from redis import Redis
from sqlalchemy import and_, case, or_, select, update

from ..config.settings import REDIS_URL, RUN_LEASE_SECONDS, RUN_MAX_ATTEMPTS
from ..models import AnalysisRun, Experiment
from ..utils.clock import now, now_epoch
from ..utils.serialize import record
from . import artifact_service
from .audit_service import log

# 新版队列使用有序集合（成员为分析编号），天然去重；旧版列表键不再使用。
QUEUE_KEY = "lift:jobs:v2"
queue = Redis.from_url(REDIS_URL, decode_responses=True)

Claim = namedtuple("Claim", "run_id owner attempt")

STAGES = {
    "queued": (8, "等待计算进程领取"),
    "loading": (32, "加载实验与观测数据"),
    "computing": (55, "计算 Welch / CUPED 统计量"),
    "writing": (78, "原子写出结果文件"),
    "verifying": (90, "校验结果文件与分析记录"),
    "completed": (100, "结果已完整落盘"),
    "failed": (100, "计算未能完成"),
}


def _stage(row) -> str:
    """状态与阶段保持一致的展示口径；兼容升级前没有 stage 的历史记录。"""
    if row.status in ("completed", "failed"):
        return row.status
    if row.status == "queued":
        return "queued"
    stage = row.stage or ""
    if stage in ("loading", "computing", "writing", "verifying"):
        return stage
    return "loading"


def _progress(stage: str) -> int:
    return STAGES.get(stage, (8, "等待计算进程领取"))[0]


def _stage_label(stage: str) -> str:
    return STAGES.get(stage, (8, "等待计算进程领取"))[1]


def _fresh_result(row) -> dict:
    """只有状态完成且产物与记录一致时才返回结果。

    历史版本（升级前已完成的记录）没有产物文件与 attempt，按兼容规则直接可读。
    """
    if row.status != "completed" or not row.result:
        return {}
    if not row.artifact_path:
        return dict(row.result)
    data = artifact_service.verify(
        row.artifact_path, row.id, row.experiment_id, row.attempt,
        row.artifact_sha256, row.artifact_size,
    )
    return dict(data["result"]) if data else {}


def is_viewable(row) -> bool:
    return bool(_fresh_result(row))


def serialize(row) -> dict:
    data = record(row)
    result = _fresh_result(row)
    data["result"] = result
    data["viewable"] = bool(result)
    data["result_ready"] = bool(result)
    stage = _stage(row)
    data["stage"] = stage
    data["progress"] = _progress(stage)
    data["stage_label"] = _stage_label(stage)
    data["dispatched"] = bool(row.queue_signal_at)
    within_budget = row.attempt < RUN_MAX_ATTEMPTS
    data["recoverable"] = within_budget and (
        row.status in ("queued", "running") or (row.status == "completed" and not result)
    )
    return data


def list_runs(db):
    return [serialize(x) for x in db.query(AnalysisRun).order_by(AnalysisRun.id.desc())]


def get_run(db, rid):
    row = db.get(AnalysisRun, rid)
    if not row:
        raise HTTPException(404, "运行记录不存在")
    return row


def signal(run_id: int) -> bool:
    """投递队列信号。有序集合按成员去重，重复投递不会产生重复任务。"""
    try:
        queue.zadd(QUEUE_KEY, {str(run_id): now_epoch()}, nx=True)
        return True
    except Exception:
        return False


def enqueue(db, eid, user):
    if not db.get(Experiment, eid):
        raise HTTPException(404, "实验不存在")
    row = AnalysisRun(experiment_id=eid, status="queued", stage="queued")
    db.add(row)
    db.flush()
    log(db, user.email, "analysis.enqueue", row.id)
    db.commit()
    if signal(row.id):
        row.queue_signal_at = now_epoch()
        row.updated_at = now()
        db.commit()
    # 信号失败也保留排队记录：恢复线程会补投递，不会永久停在未入队状态。
    return serialize(row)


def claim(db, run_id: int, owner: str) -> Claim | None:
    """原子领取或接管；attempt 递增并作为 fencing token。"""
    stamp = now_epoch()
    takeover = and_(AnalysisRun.status == "running", AnalysisRun.lease_expires_at < stamp)
    stmt = (
        update(AnalysisRun)
        .where(AnalysisRun.id == run_id)
        .where(AnalysisRun.attempt < RUN_MAX_ATTEMPTS)
        .where(or_(AnalysisRun.status == "queued", takeover))
        .values(
            status="running",
            attempt=AnalysisRun.attempt + 1,
            claimed_by=owner,
            lease_expires_at=stamp + RUN_LEASE_SECONDS,
            heartbeat_at=now(),
            updated_at=now(),
            stage="loading",
            queue_signal_at=0.0,
            error="",
            result={},
            artifact_path="",
            artifact_sha256="",
            artifact_size=0,
            completed_at="",
            recovery_count=case(
                (takeover, AnalysisRun.recovery_count + 1),
                else_=AnalysisRun.recovery_count,
            ),
            recovery_reason=case(
                (takeover, "执行进程失联，租约过期后由新的计算进程接管"),
                else_=AnalysisRun.recovery_reason,
            ),
        )
        .returning(AnalysisRun.attempt)
    )
    attempt = db.execute(stmt).scalar_one_or_none()
    db.commit()
    if attempt is None:
        return None
    return Claim(run_id, owner, int(attempt))


def release(claim: Claim, reason: str, db=None) -> bool:
    """进程主动退出时立即交回队列，不必等租约过期。"""
    own = db is not None
    session = db or _session()
    try:
        result = session.execute(
            update(AnalysisRun)
            .where(AnalysisRun.id == claim.run_id)
            .where(AnalysisRun.attempt == claim.attempt)
            .where(AnalysisRun.claimed_by == claim.owner)
            .where(AnalysisRun.status == "running")
            .values(
                status="queued",
                claimed_by="",
                lease_expires_at=0.0,
                heartbeat_at=now(),
                updated_at=now(),
                stage="queued",
                queue_signal_at=0.0,
                recovery_count=AnalysisRun.recovery_count + 1,
                recovery_reason=reason,
            )
        )
        session.commit()
        return result.rowcount == 1
    finally:
        if not own:
            session.close()


def heartbeat(claim: Claim, stage: str | None, db) -> bool:
    """续租。stage 为 None 时只刷新租约与心跳，不改变展示中的阶段。"""
    values = {
        "lease_expires_at": now_epoch() + RUN_LEASE_SECONDS,
        "heartbeat_at": now(),
        "updated_at": now(),
    }
    if stage is not None:
        values["stage"] = stage
    result = db.execute(
        update(AnalysisRun)
        .where(AnalysisRun.id == claim.run_id)
        .where(AnalysisRun.attempt == claim.attempt)
        .where(AnalysisRun.claimed_by == claim.owner)
        .where(AnalysisRun.status == "running")
        .values(**values)
    )
    db.commit()
    return result.rowcount == 1


def finish(claim: Claim, result: dict, artifact: dict, db) -> bool:
    """把完整产物登记到分析记录；attempt/owner 不匹配时丢弃迟到结果。"""
    if not {"artifact_path", "artifact_sha256", "artifact_size"} <= set(artifact):
        return False
    verified = artifact_service.verify(
        artifact["artifact_path"], claim.run_id, _experiment_id(db, claim.run_id),
        claim.attempt, artifact["artifact_sha256"], artifact["artifact_size"],
    )
    if not verified or verified["result"] != result:
        _discard_if_unreferenced(db, claim.run_id, artifact["artifact_path"])
        return False
    stamp = now()
    updated = db.execute(
        update(AnalysisRun)
        .where(AnalysisRun.id == claim.run_id)
        .where(AnalysisRun.attempt == claim.attempt)
        .where(AnalysisRun.claimed_by == claim.owner)
        .where(AnalysisRun.status == "running")
        .values(
            status="completed",
            result=result,
            error="",
            stage="completed",
            completed_at=stamp,
            updated_at=stamp,
            heartbeat_at=stamp,
            lease_expires_at=0.0,
            claimed_by="",
            queue_signal_at=0.0,
            artifact_path=artifact["artifact_path"],
            artifact_sha256=artifact["artifact_sha256"],
            artifact_size=artifact["artifact_size"],
        )
    )
    if updated.rowcount != 1:
        db.rollback()
        _discard_if_unreferenced(db, claim.run_id, artifact["artifact_path"])
        return False
    log(db, "worker", "analysis.completed", claim.run_id)
    db.commit()
    artifact_service.prune_other_attempts(claim.run_id, artifact["artifact_path"])
    return True


def fail(claim: Claim, error: str, db) -> bool:
    updated = db.execute(
        update(AnalysisRun)
        .where(AnalysisRun.id == claim.run_id)
        .where(AnalysisRun.attempt == claim.attempt)
        .where(AnalysisRun.claimed_by == claim.owner)
        .where(AnalysisRun.status == "running")
        .values(
            status="failed",
            error=error,
            stage="failed",
            updated_at=now(),
            lease_expires_at=0.0,
            claimed_by="",
            queue_signal_at=0.0,
        )
    )
    if updated.rowcount != 1:
        db.rollback()
        return False
    log(db, "worker", "analysis.failed", claim.run_id)
    db.commit()
    return True


def _discard_if_unreferenced(db, run_id: int, relative: str) -> None:
    referenced = db.execute(
        select(AnalysisRun.artifact_path).where(AnalysisRun.id == run_id)
    ).scalar_one_or_none()
    if relative and relative != referenced:
        artifact_service.discard(relative)


def _experiment_id(db, run_id: int) -> int:
    value = db.execute(select(AnalysisRun.experiment_id).where(AnalysisRun.id == run_id)).scalar_one()
    return int(value)


def _session():
    # 延迟导入，避免 worker 入口的循环依赖。
    from ..config.database import SessionLocal
    return SessionLocal()


def finish_is_fenced(claim: Claim, db) -> bool:
    """读取库中最新位次；不能被同一会话的身份映射缓存误导。"""
    row = db.execute(
        select(AnalysisRun.attempt, AnalysisRun.claimed_by).where(AnalysisRun.id == claim.run_id)
    ).one_or_none()
    if row is None:
        return True
    return int(row[0]) != claim.attempt or row[1] != claim.owner


def export_run(db, rid):
    row = get_run(db, rid)
    result = _fresh_result(row)
    if not result:
        if row.status == "completed":
            raise HTTPException(409, "结果文件缺失或校验失败，已阻止导出")
        raise HTTPException(409, "运行尚未完成，结果不可导出")
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["method", "n_control", "n_treatment", "difference", "p_value", "ci_low", "ci_high"])
    for method in ("raw", "cuped"):
        comparison = result.get(method)
        if comparison:
            writer.writerow([method] + [comparison[k] for k in ["n_control", "n_treatment", "difference", "p_value", "ci_low", "ci_high"]])
    return out.getvalue()
