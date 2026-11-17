export interface User {name:string;email:string;role:string}
export interface Dataset {id:number;name:string;source:string;description:string;checksum:string;observations:number}
export interface Observation {subject:string;arm:string;pre_value:number;value:number}
export interface Experiment {id:number;name:string;dataset_id:number;hypothesis:string;alpha:number;cuped:boolean;owner:string}
export interface Comparison {control_mean:number;treatment_mean:number;difference:number;p_value:number;ci_low:number;ci_high:number;n_control:number;n_treatment:number;standard_error:number}
export interface Run {id:number;experiment_id:number;status:string;error:string;created_at:string;stage:string;stage_label:string;progress:number;attempt:number;recovery_count:number;recovery_reason:string;claimed_by:string;heartbeat_at:string;updated_at:string;completed_at:string;dispatched:boolean;recoverable:boolean;viewable:boolean;result_ready:boolean;artifact_path:string;artifact_sha256:string;artifact_size:number;result:{raw?:Comparison;cuped?:Comparison;theta?:number;variance_reduction?:number;significant?:boolean;interpretation?:string;assumptions?:string}}
export interface Decision {id:number;run_id:number;outcome:string;note:string;author:string;created_at:string;run_viewable?:boolean}
export interface Audit {id:number;actor:string;action:string;target:string;created_at:string}
