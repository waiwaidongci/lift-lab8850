from pydantic import BaseModel
class RunOutput(BaseModel):
    id: int
    experiment_id: int
    status: str
    result: dict
    error: str
    created_at: str
    stage: str = "queued"
    stage_label: str = ""
    progress: int = 0
    attempt: int = 0
    recovery_count: int = 0
    recovery_reason: str = ""
    claimed_by: str = ""
    heartbeat_at: str = ""
    updated_at: str = ""
    completed_at: str = ""
    dispatched: bool = False
    recoverable: bool = False
    viewable: bool = False
    result_ready: bool = False
    artifact_path: str = ""
    artifact_sha256: str = ""
    artifact_size: int = 0
