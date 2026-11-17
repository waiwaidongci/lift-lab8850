from fastapi import HTTPException
from ..models import Decision, AnalysisRun
from ..utils.serialize import record
from .audit_service import log
from .run_service import is_viewable
def list_decisions(db):
    rows=[record(x) for x in db.query(Decision).order_by(Decision.id.desc())]
    for row in rows:
        run=db.get(AnalysisRun,row["run_id"])
        row["run_viewable"]=bool(run and is_viewable(run))
    return rows
def create_decision(db,data,user):
    run=db.get(AnalysisRun,data.run_id)
    if not run or run.status!="completed": raise HTTPException(409,"只能审议已完成分析")
    if not is_viewable(run): raise HTTPException(409,"分析结果文件缺失或校验失败，暂不能作为决策依据")
    row=Decision(**data.model_dump(),author=user.name)
    db.add(row); db.flush(); log(db,user.email,"decision."+data.outcome,row.id); db.commit()
    return record(row)
