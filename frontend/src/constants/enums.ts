export const Roles={admin:'实验负责人',analyst:'数据分析师',viewer:'业务观察员'} as const;
export const RunStatus={queued:'排队中',running:'分析中',completed:'已完成',failed:'失败'} as const;
export const RunStage={queued:'等待计算进程领取',loading:'加载实验与观测数据',computing:'计算 Welch / CUPED 统计量',writing:'原子写出结果文件',verifying:'校验结果文件与分析记录',completed:'结果已完整落盘',failed:'计算未能完成'} as const;
export const Outcomes={ship:'同意放量',hold:'继续观察',reject:'暂缓上线'} as const;
