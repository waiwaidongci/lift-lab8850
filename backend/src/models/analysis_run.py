from sqlalchemy import ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column
from ..config.database import Base
from ..utils.clock import now

class AnalysisRun(Base):
    __tablename__ = "analysis_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    experiment_id: Mapped[int] = mapped_column(ForeignKey("experiments.id"))
    status: Mapped[str] = mapped_column(default="queued")
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    error: Mapped[str] = mapped_column(default="")
    created_at: Mapped[str] = mapped_column(default=now)
    # 恢复与租约：attempt 同时作为 fencing token，每次被计算进程取走时递增。
    attempt: Mapped[int] = mapped_column(default=0)
    recovery_count: Mapped[int] = mapped_column(default=0)
    recovery_reason: Mapped[str] = mapped_column(default="")
    claimed_by: Mapped[str] = mapped_column(default="")
    lease_expires_at: Mapped[float] = mapped_column(default=0.0)
    heartbeat_at: Mapped[str] = mapped_column(default="")
    stage: Mapped[str] = mapped_column(default="queued")
    updated_at: Mapped[str] = mapped_column(default="")
    completed_at: Mapped[str] = mapped_column(default="")
    artifact_path: Mapped[str] = mapped_column(default="")
    artifact_sha256: Mapped[str] = mapped_column(default="")
    artifact_size: Mapped[int] = mapped_column(default=0)
    queue_signal_at: Mapped[float] = mapped_column(default=0.0)
