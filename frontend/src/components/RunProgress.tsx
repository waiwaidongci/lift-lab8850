import {Run} from '../types/domain';import {RunStage} from '../constants/enums';import {time} from '../utils/format';

const order=['queued','loading','computing','writing','verifying','completed'];

export function RunProgress({run}:{run:Run}){
  const broken=run.status==='completed'&&!run.viewable;
  const failed=run.status==='failed'||broken;
  const activeIndex=run.status==='failed'?order.length:order.indexOf(run.stage||run.status);
  const recovered=run.recovery_count>0;
  const heading=broken?'结果文件尚未通过完整性校验':(RunStage[(run.stage||run.status) as keyof typeof RunStage]||run.stage_label||run.status);
  return <section className="run-progress" aria-label="分析任务恢复进度">
    <div className="progress-head">
      <div>
        <span className="eyebrow">{failed?'RECOVERY FAILED':'RECOVERY PROGRESS'} / RUN-{run.id}</span>
        <h3>{heading}</h3>
      </div>
      <div className="progress-number"><b>{run.status==='failed'?'—':run.progress}</b><span>{run.status==='failed'?'已停止':'%'}</span></div>
    </div>
    <div className={'progress-track'+(failed?' failed':'')} role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={run.status==='failed'?0:run.progress}>
      <i style={{width:`${failed?100:run.progress}%`}}/>
    </div>
    <ol className="progress-steps">{order.map((stage,index)=><li key={stage} className={index<activeIndex?'done':index===activeIndex&&!failed?'active':''}><span>{RunStage[stage as keyof typeof RunStage]}</span></li>)}</ol>
    <div className="progress-meta">
      <div><span>分析编号</span><b>RUN-{run.id}</b></div>
      <div><span>尝试次数</span><b>{run.attempt} 次 · 自动接管 {run.recovery_count} 次</b></div>
      <div><span>最近心跳</span><b>{run.heartbeat_at?time(run.heartbeat_at):'—'}</b></div>
      <div><span>执行进程</span><b className="mono">{run.claimed_by||'等待领取'}</b></div>
    </div>
    {recovered&&<p className="progress-note">恢复记录：{run.recovery_reason||'任务已自动重新排队。'}{run.status==='queued'?' 已沿用同一分析编号重新入队。':''}</p>}
    {run.status==='failed'&&<p className="progress-error">失败原因：{run.error||'未知错误'}</p>}
    {broken&&<p className="progress-error">失败原因：{run.error||'结果文件与分析记录未能完整对应'}。恢复线程会自动重新计算，校验通过前不开放查看、导出与决策引用。</p>}
    {run.status==='queued'&&!run.dispatched&&<p className="progress-note">队列信号尚未确认，恢复线程会自动补投递，无需重新提交。</p>}
  </section>;
}
