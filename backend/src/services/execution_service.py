"""单个分析任务的执行步骤，供计算进程与测试共用。"""
import logging
import traceback

from ..models import AnalysisRun, Experiment, Observation
from . import artifact_service, run_service
from .statistics_service import analyze
from ..utils.clock import now as stamp_now

log_ = logging.getLogger("lift.execution")


def execute(claim, db):
    """执行 claim：加载 → 计算 → 原子写出 → 校验 → 登记。

    每个写库动作都带 attempt/owner 条件；被接管后旧进程的写入会被拒绝。
    """
    try:
        run_service.heartbeat(claim, "loading", db)
        run = db.get(AnalysisRun, claim.run_id)
        experiment = db.get(Experiment, run.experiment_id)
        observations = db.query(Observation).filter_by(dataset_id=experiment.dataset_id).all()
        if run_service.finish_is_fenced(claim, db):
            return False
        run_service.heartbeat(claim, "computing", db)
        result = analyze(observations, experiment.alpha, experiment.cuped)
        if run_service.finish_is_fenced(claim, db):
            return False
        run_service.heartbeat(claim, "writing", db)
        artifact = artifact_service.commit(
            claim.run_id, experiment.id, claim.attempt, result, stamp_now()
        )
        run_service.heartbeat(claim, "verifying", db)
        if run_service.finish_is_fenced(claim, db):
            # 已被接管：产物只是本 attempt 的文件，记录仍归接管进程。
            log_.warning("RUN-%s attempt=%s 已被接管，放弃登记", claim.run_id, claim.attempt)
            return False
        if not run_service.finish(claim, result, artifact, db):
            log_.warning("RUN-%s attempt=%s 的迟到结果已被放弃", claim.run_id, claim.attempt)
            return False
        return True
    except Exception as error:
        traceback.print_exc()
        try:
            run_service.fail(claim, str(error), db)
        except Exception:
            log_.exception("登记失败状态时出错 RUN-%s", claim.run_id)
        return False
