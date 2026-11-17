-- 分析任务恢复所需字段。使用 PostgreSQL 时执行一次；启动不会自动升级已存在的表。
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS stage varchar NOT NULL DEFAULT 'queued';
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS attempt integer NOT NULL DEFAULT 0;
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS recovery_count integer NOT NULL DEFAULT 0;
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS recovery_reason varchar NOT NULL DEFAULT '';
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS claimed_by varchar NOT NULL DEFAULT '';
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS lease_expires_at double precision NOT NULL DEFAULT 0;
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS heartbeat_at varchar NOT NULL DEFAULT '';
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS updated_at varchar NOT NULL DEFAULT '';
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS completed_at varchar NOT NULL DEFAULT '';
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS artifact_path varchar NOT NULL DEFAULT '';
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS artifact_sha256 varchar NOT NULL DEFAULT '';
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS artifact_size integer NOT NULL DEFAULT 0;
ALTER TABLE analysis_runs ADD COLUMN IF NOT EXISTS queue_signal_at double precision NOT NULL DEFAULT 0;

-- 旧的 running 记录没有租约：恢复线程会在一个租约周期后接管。
CREATE INDEX IF NOT EXISTS analysis_runs_status_idx ON analysis_runs (status, lease_expires_at);
