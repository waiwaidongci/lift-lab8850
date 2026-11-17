# 岚桥实验放量台 · LIFT LAB

把用户级随机实验从假设登记、异步统计分析连接到放量会议结论，保留可复算的证据。

## 场景与工作流

模拟客户为岚桥内容科技的 32 人增长团队。原流程从埋点导出 CSV，在表格里手算显著性，再把结论贴到会议纪要。前置收入差异、p 值误读和无法追溯的复算是三个主要痛点。上游为用户级随机分桶观测，下游为产品放量会议；本项目替代中间的分析、证据导出和结论归档。

演示数据为固定随机种子 42 生成的合成数据，2400 位独立用户，1:1 分组，前置和后置各 7 日收入。仅供演示，不是实际客户资料。三个演示假设复用同一数据以便体验，不能视作三个独立实验。

| 角色 | 页面 | 权限 | 典型动作 |
|---|---|---|---|
| admin | 全部六页 | 读、登记、运行、归档结论 | 实验负责人作会议结论 |
| analyst | 全部六页 | 读、登记、运行 | 分析师准备区间与导出 |
| viewer | 全部六页 | 只读与导出 | 产品观察员阅读证据 |

## 快速启动

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
docker compose exec -T backend python -m scripts.seed_demo
```

访问 [前端](http://localhost:18200)；[后端接口文档](http://localhost:19200/docs)。Nginx 将 `/api/` 去掉后代理到后端，直接访问后端不加 `/api` 前缀。

| 账号 | 密码 | 角色 |
|---|---|---|
| admin@lanqiao.demo | Lanqiao2026! | admin |
| analyst@lanqiao.demo | Lanqiao2026! | analyst |
| viewer@lanqiao.demo | Lanqiao2026! | viewer |

首次启动自动 seed（`SEED_DEMO=true`），失败记日志而不阻断服务。再次执行根据用户邮箱、数据集名称、实验名查重，不覆盖已经存在的行。数据库通过 ORM metadata 首次建表；`backend/migrations/001_initial.sql` 提供索引迁移示例，后续结构修改需审查迁移，启动不会自动升级已存在表。

## 一天的研究剧本

1. 09:00 林序以 admin 登录，检查 `lanqiao-paywall-2026-11.csv` 的 2400 条观测和校验和。
2. 09:10 许禾以 analyst 登录，在实验配置登记“短文付费入口 v4”。
3. 选择已冻结的付费入口数据，写明可检验的业务假设。
4. 固定收入指标、α=.05，打开 CUPED 校正，提交分析。
5. 后台 worker 从 Redis 队列取任务，使用 Python 计算 Welch 与 CUPED 结果；中断后自动接管，沿用同一 RUN 编号。
6. 09:15 在分析结果检查 1200/1200 样本量、p 值、区间和方差降低，并导出 CSV。
7. 周澄以 viewer 阅读同一证据；登记接口和归档接口均会拒绝该角色。
8. 10:00 林序记录“同意放量/继续观察/暂缓上线”及至少 8 字依据，在操作留痕追溯动作。

## 计算口径

- **Welch t 检验**：输入用户级后置收入与分组；差异为候选均值减对照均值。标准误 `sqrt(sA²/nA+sB²/nB)`，Welch–Satterthwaite 自由度，使用 SciPy Student t 双侧尾概率和 `1-alpha` 区间。输出两组均值、样本量、差异、自由度、标准误、p 值和上下界。
- **CUPED**：输入分桶前协变量 X 与后置收入 Y；合并样本估计 `theta=cov(Y,X)/var(X)`，计算 `Y_adj=Y-theta*(X-mean(X))` 后再做 Welch。输出原始与校正结果，方差降低口径为 `1-SE_adj²/SE_raw²`（可能为负），不把它解释为业务增长。
- 至少每组 3 人；非有限值、空集、未知分组、组内零方差拒绝；启用 CUPED 时前置指标常量拒绝。固定种子、持久化观测与 SHA256 使结果可复算。算法见 `backend/src/services/statistics_service.py`。
- 假设用户独立随机分桶、协变量在分桶前取得；只支持单个预注册指标，未做序贯检验、多重比较、离群值稳健估计或聚类相关性校正。95% 区间仅在 α=.05 时成立，结果页按实际 alpha 标注。

## 功能与工程结构

六页：实验概览、观测数据（前100条预览）、实验配置、分析结果（真实 SVG 区间图）、决策档案、操作留痕。实体 Dataset / Observation / Experiment / AnalysisRun / Decision 各有数据模型与 API；API、前端 store、组件职责分开。

| 层 | 技术 |
|---|---|
| 后端 | Python 3.11、FastAPI、SQLAlchemy、SciPy / NumPy |
| 前端 | React 18、TypeScript、Vite、Lucide、Noto Serif SC + Noto Sans SC |
| 持久化 | PostgreSQL 16、Redis 7 AOF |
| 交付 | 恰好 5 个 Compose 服务，Nginx SPA 回落 |

```
backend/src/{models,schemas,services,controllers,routes,middlewares,utils,constants,config}
backend/src/{main.py,worker.py}
backend/scripts/seed_demo.py
backend/migrations/001_initial.sql
frontend/src/{api,stores,types,components,hooks,pages,router,utils,constants}
```

后端 JWT 有效期 8 小时，PBKDF2-SHA256 存储密码；角色每次由数据库读取，不信任前端。审计覆盖登录、登记、运行完成、恢复接管与决策。两个独立中间件处理全局错误与登录频率限制（单进程内每 IP 每分钟30次）。文件导出需要认证。

共享枚举：角色位于 `backend/src/constants/roles.py` / `frontend/src/constants/enums.ts`，任务状态位于 `backend/src/constants/status.py` / `frontend/src/constants/enums.ts`；后端服务持久化与接口使用同名值。`ExperimentSummary` 在概览、结果与决策页共用，`DataTable`、`StatusBadge`、`Notice` 提供一致交互。

## 分析任务恢复

一个分析任务从登记到结果开放要跨过四道边界：入队、取走、写出、登记。每一道都可能在进程退出时留下“永久运行中”的记录，恢复机制针对这四类中断自动收敛，并且始终复用同一条 `analysis_runs.id`（页面上的 RUN 编号）。

| 中断点 | 记录状态 | 恢复动作 |
|---|---|---|
| 登记后未入队（Redis 不可用或信号丢失） | `queued`，`queue_signal_at=0` | 恢复扫描补投递；队列用有序集合去重，重复投递不会重复执行 |
| 取走后计算中退出 | `running` 且租约过期 | 任意计算进程可原子接管，`attempt` 递增；原进程晚恢复也无法登记 |
| 结果写出但未登记 | `running`，产物存在但记录未指向 | 结果保持不可见；租约过期后接管重算，旧 attempt 文件被清理 |
| 登记完成但文件损坏/被截断 | `completed` 但校验失败 | 查看/导出/决策全部关闭；恢复扫描自动重新排队并重算 |

机制要点：

- **租约与 fencing token**：领取任务时用条件更新把 `attempt` 加一，同时写入 `claimed_by` 与 `lease_expires_at`。心跳每 10 秒续租（默认租约 30 秒）。所有完成、失败、心跳与释放操作都带 `attempt` 与 `claimed_by` 条件，迟到的旧进程只会写自己的 attempt 文件，无法覆盖接管后的记录。
- **并发接管**：领取条件更新在数据库中原子执行，两个执行进程同时抢同一任务只有一个成功。每个计算进程用一把 Redis 槽位锁（共两把）限制并行度；`docker compose up -d --scale worker=2` 可验证双进程接管（worker 服务未固定容器名）。
- **主动退出**：SIGTERM/SIGINT 会立即把当前任务交回队列并补投递，不必等租约过期；重启后的进程沿用同一编号完成。
- **完整性门禁**：产物先写临时文件 → fsync → 原子重命名，SHA256 与字节数写入分析记录。接口读取时会重新校验文件、哈希、run/experiment/attempt，只有全部一致才返回结果。查看、导出、决策引用与页面均以 `viewable` 为准，半份结果（文件存在但未登记、哈希不符、被截断）对外不可见。
- **恢复进度与失败原因**：记录里保存 `stage`、`progress`、`attempt`、`recovery_count`、`recovery_reason`、`heartbeat_at` 与 `error`，分析结果页展示阶段条、接管次数与原因；旧分析记录仍可查看，历史版本没有产物的完成记录按兼容规则继续可读。
- **尝试上限**：默认 3 次；连续中断后任务转为 `failed` 并写明原因，不会无限重试。相关参数见下表。

## 环境变量与 Docker

| 变量 | 默认值 | 说明 |
|---|---|---|
| COMPOSE_PROJECT_NAME | lift-lab | 容器和网络命名空间 |
| FRONTEND_PORT | 18200 | Nginx 前端 |
| BACKEND_PORT | 19200 | FastAPI |
| DB_PORT | 17700 | PostgreSQL 宿主映射 |
| JWT_SECRET | local-demo-lift-lab-change-this-key | 本地演示签名密钥，部署前更换 |
| SEED_DEMO | true | 后端启动时 seed |
| ARTIFACT_DIR | /artifacts | 结果产物目录，backend/worker 共享 |
| RUN_LEASE_SECONDS | 30 | 计算租约；失联超过该时长可被接管 |
| RUN_HEARTBEAT_SECONDS | 10 | 计算进程续租间隔 |
| RUN_MAX_ATTEMPTS | 3 | 单条分析最多尝试次数，用尽后失败 |
| RUN_HEAL_INTERVAL_SECONDS | 30 | 恢复扫描间隔 |

命名卷 `db-data`、`redis-data`、`artifacts`；backend/worker 共享产物卷。worker 使用2秒轮询、每10秒续租并持续写心跳（健康检查放宽到45秒，避免两个 worker 互相刷新同一心跳文件时误判）；5类服务均有健康检查。后端等待 db/redis healthy，前端等待 backend healthy。停止并清空本项目数据：

```bash
docker compose down -v --remove-orphans
```

## 本地开发与验证

安装 Python 3.11，`pip install -r backend/requirements.txt`；将 `DATABASE_URL` / `REDIS_URL` 指向本机依赖，`cd backend && uvicorn src.main:app --reload`。前端 `cd frontend && npm install && npm run dev`；开发时需为 `/api` 配置反向代理到本地后端。标准启动以 Compose 为准。

恢复回归测试（`cd backend`）：

```bash
pip install -r requirements-dev.txt
pytest -q
```

覆盖登记后未入队、计算中 kill -9 退出、结果写出未登记、旧进程晚返回、双进程并发接管、产物被截断后的门禁，以及 SIGTERM 交回队列后重启沿同一编号完成。

测试脚本、截图和真实结果在项目外 `../../test-artifacts/gb-0001/`。日志与 `.env` 不提交。该应用适合本地合成数据研究；不包含真实文件上传、邮件外发或生产级多实例限流。

## License

MIT，详见 LICENSE。
