# EvalFrog

> CLI-first Enterprise Workflow Runtime

EvalFrog 是一个同时面向 Human Web 与 Agent CLI 的企业级 Workflow Platform。它的目标不是在第一阶段提供大量节点和外围功能，而是先建立一个边界清晰、可恢复、可追踪、可以长期演进的 Workflow 核心。

当前状态：**M1 IR、Node Catalog Contract 与公共契约已实现，下一阶段为 M2 Compiler、DSL、Source Map 与 Control Graph**。第一阶段开发路线与验收门槛见 [项目实施计划](./docs/plans/项目实施计划与验收标准.md)。

## 为什么是 EvalFrog

传统 Workflow 系统通常只服务前端画布，Agent 只能绕过平台生成私有格式；另一类系统则只面向代码和 API，Human 难以理解和维护定义。

EvalFrog 让两类作者使用同一种 IR：

```text
Human Web Canvas ─┐
                  ├→ Canonical JSON IR → Server Compiler → Immutable DSL
evalfrog CLI ─────┘
```

- Human 可以通过 Web Canvas 编辑、测试、发布和运行 Workflow；
- Agent 可以在本地生成具有语义 ID 的 IR，通过 `evalfrog` CLI 上传、校验、测试和发布；
- Runtime 只执行平台 Compiler 生成的不可变 Execution Snapshot；
- Runtime Error 通过 Source Map 定位回 IR Node、Edge 和具体字段。

## 系统架构

![EvalFrog 系统总体架构](./docs/architecture/EvalFrog_系统架构.png)

可编辑源文件：[EvalFrog_系统架构.drawio](./docs/architecture/EvalFrog_系统架构.drawio)

第一阶段采用：

```text
Human Web + evalfrog CLI + Enterprise API
                 ↓
Modular Control Plane（同一代码库与部署单元，可多副本）
                 ↓
PostgreSQL + Scheduling Redis + Cache Redis + Kafka
                 ↓
Builtin Worker Pool + Sandbox Worker Pool
```

Control Plane 是模块化单体，不是一个没有边界的大 Service。Definition、Compiler、Runtime Engine、Scheduler、Attempt Coordinator、Execution Context、Eventing 和 Recovery 拥有独立职责，可以同进程部署，但不能互相绕过状态所有权。

## M0 Quick Start

前置条件：Go `1.26+`、Docker 与 Docker Compose。

Windows 使用一条命令构建并启动 PostgreSQL、Scheduling Redis、Cache Redis、Kafka、Migration、Control Plane 和两个 Worker，随后运行 CLI Doctor：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dev.ps1
```

Linux/macOS：

```bash
./scripts/dev.sh
```

启动成功后：

| 入口 | 地址 |
|---|---|
| Control Plane | `http://localhost:8080` |
| Builtin Worker Health | `http://localhost:8081/health/ready` |
| Sandbox Worker Health | `http://localhost:8082/health/ready` |
| PostgreSQL | `localhost:15432` |
| Scheduling Redis | `localhost:16379` |
| Cache Redis | `localhost:16380` |
| Kafka | `localhost:29092` |

停止进程但保留本地数据：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\down.ps1
```

M0 CLI 命令：

```text
evalfrog version
evalfrog config validate --profile local --config-dir configs
evalfrog doctor --profile local --config-dir configs
```

M0 只提供进程、配置和基础设施闭环，尚未提供 Workflow 创建或运行 API。

## M1 Authoring Contract

M1 已提供 Human Web 与 Agent CLI 共用的作者态契约：

- [IR v1 JSON Schema](./contracts/ir/v1/schema.json)；
- `internal/ir` 中的 Go Model、有界 Draft Parser、Strict Validator、Canonical JSON、SHA-256 Hash 和结构化 Diagnostic；
- `internal/catalog` 中的 `catalog-v1` 及 Start、End、Branch、Code、HTTP、RPC 六类无版本号公共 Node Description；
- [版本化正反例与 Golden Fixture](./contracts/ir/v1/fixtures/manifest.json)。

Draft Parser 允许保存具有合法 IR 外壳但尚未完整的画布；Test/Publish 必须使用显式绑定 Catalog 的 Strict Validator。M1 尚未实现 Draft API、Compiler 或 Workflow 运行。

## 开发检查

```bash
go test ./...
go test -race ./...
go test -count=20 ./contracts/ir ./internal/ir ./internal/catalog
go test ./internal/ir -run='^$' -fuzz=FuzzParser -fuzztime=5s
go test ./internal/ir -run='^$' -fuzz=FuzzLogicalID -fuzztime=5s
go vet ./...
go build ./cmd/...
go test -tags=integration ./tests/integration
```

架构测试会扫描仓库依赖，并拒绝 Domain 导入 Adapter、Worker 导入 PostgreSQL Adapter、Runtime 读取作者态模型、Scheduler 导入 Engine，以及新增 `common/utils/service/pkg` 等逃逸边界的目录。

## 核心架构原则

### 不可变执行定义

- Draft 可以保存不完整 IR，也可以测试；
- 每次保存成功形成不可变 Draft Revision；
- Publish 永远创建并自动激活新的不可变 Published Version；
- Production Run 必须先发布，没有 Published Version 时不能正式调用；
- Test Run 和 Production Run 都只执行不可变 Execution Snapshot；
- Run 创建后不跟随 Draft 或 Active Version 变化。

### IR 与 DSL 分离

| 模型 | 使用者 | 目的 |
|---|---|---|
| IR | Human、Agent、Web、CLI | 编辑、理解、Diff、Copy、错误定位 |
| DSL | Compiler、Runtime | 强约束、版本化、不可变执行语义 |
| Source Map | Control Plane | DSL Runtime Error → IR Node/Edge/Field |

CLI 可以上传 IR，并下载平台生成的 DSL、Source Map 和诊断；不能上传客户端 DSL 或覆盖 Source Map。

### 状态所有权

| 组件 | 负责 |
|---|---|
| Engine | Run/NodeRun 语义推进、Branch、Retry、Skipped、终态 |
| Scheduler | Project 公平准入、创建并派发 Attempt |
| Attempt Coordinator | Claim、Heartbeat、Complete、Lease、Fencing、Output Candidate |
| Execution Context Gateway | 向 Worker 只读装配 Snapshot、Input、Effective Output |
| Worker | 执行一个 Attempt，不修改 Workflow 状态 |

### 数据职责

- PostgreSQL：Definition、Runtime、Output、Outbox/Inbox、权限与审计的唯一权威状态；
- Scheduling Redis：Lane、Credit、Ready Index、Inflight Reservation，可从 PostgreSQL 重建；
- Cache Redis：Execution Snapshot、Run Context、Effective Output、Run Read Model，Cache Miss 时回源；
- Kafka：Builtin Task、Sandbox Task、Runtime Event 的至少一次传输，不承担权威状态。

## 第一阶段节点

### Control Node

- `start`
- `end`
- `branch`

Control Node 由 Compiler 和 Engine 解释，不创建 Worker Attempt，也不进入 Kafka Task Topic。

### Task Node

- `http`：只允许 `connection_ref + relative_path`；
- `rpc`：只允许 `service_ref + operation`；
- `code`：只支持 Python，在 Per-Attempt Sandbox 中执行。

Code Sandbox 使用固定资源规格和 JSON Object 输入输出，不提供 Network、Database 或 Secret 权限。

## 调度与执行

EvalFrog 不使用全局 FIFO。项目公平性的唯一身份是 `project_id`：

- 竞争 Project 之间采用等权 Max-Min Fairness；
- 空闲 Project 的额度可以被其他 Project 借用；
- Project 内按 `priority DESC, ready_at ASC, node_run_id ASC` 稳定选择；
- Kafka 前使用有界 Dispatch Window，禁止深度预派发；
- Scheduling Redis 使用固定 Lane 和批量 Credit，数据库 CAS 是最终裁决。

Worker 只有在本地执行槽可用时才领取 Task：

```text
Consume Task
→ ClaimAttempt
→ queued → running + Lease/Fencing
→ ACK Kafka
→ LoadExecutionContext
→ Execute
→ CompleteAttempt
→ Completion Event
→ Engine 接受 Effective Output
```

## 计划中的 CLI 命令面

以下是已经冻结的目标命令面；在对应里程碑完成前不代表当前仓库已经实现：

```text
evalfrog workflow create|list|get|copy
evalfrog workflow draft pull|push|validate|test
evalfrog workflow publish
evalfrog workflow run start|status|list|cancel
evalfrog node-type list|describe
evalfrog connection list|describe
```

Agent 生成的 Workflow 文件是普通 Canonical JSON IR，不需要理解内部 Catalog Revision、DSL Operation Version、Kafka Topic 或 Worker 拓扑。

## 目标代码结构

```text
evalfrog/
├─ cmd/
│  ├─ evalfrog/
│  ├─ control-plane/
│  ├─ worker-builtin/
│  └─ worker-sandbox/
├─ web/
├─ contracts/
│  ├─ ir/
│  ├─ openapi/
│  └─ messages/
├─ internal/
│  ├─ access/
│  ├─ resources/
│  ├─ definition/
│  ├─ ir/
│  ├─ catalog/
│  ├─ compiler/
│  ├─ runtime/{engine,attempt,context}/
│  ├─ scheduling/
│  ├─ eventing/
│  ├─ recovery/
│  ├─ projection/
│  ├─ worker/{runtime,executor}/
│  └─ adapters/
├─ migrations/
├─ configs/
├─ deployments/
└─ docs/
```

代码依赖只能朝向领域核心：

```text
Interface / Transport Adapter
            ↓
Application Module
            ↓
Domain Model + Port
            ↑
Infrastructure Adapter
```

## 技术栈

- Go：Control Plane、CLI、Worker Runtime 与 Builtin Executor；
- PostgreSQL：权威 Definition/Runtime 数据、Outbox/Inbox；
- Redis：Scheduling Store 与 Cache Store；
- Kafka：TaskBus 与 Runtime Event；
- Python Sandbox：Code Node 隔离执行；
- HTTP/JSON：External API 与第一阶段 Worker API；
- Web Canvas：前端技术选型在 Web 实施阶段确定，不影响 IR/API 契约。

## 可靠性模型

- 所有消息按至少一次处理；
- Outbox、Inbox、CAS、Idempotency Key 和 Fencing 共同保证业务幂等；
- ACK Kafka 不等待节点执行完成，Worker 故障由数据库 Lease 恢复；
- `attempt_seq`、`retry_count`、`recovery_count` 分别记录物理执行、业务重试和基础设施恢复；
- 只有 `effective_attempt_id` 对应的 Output 能被下游读取；
- Redis 丢失不丢 Workflow，Kafka 乱序不改变状态机正确性；
- Engine 不永久拥有 Run，任一实例都能通过数据库状态和 Inbox/CAS 接管。

## 文档

- [项目准则](./项目准则.md)
- [面试复习学习文档](./docs/learning/学习文档.md)
- [核心架构决策](./docs/architecture/decisions/00_核心架构决策基线.md)
- [分布式调度与 Worker 边界](./docs/architecture/decisions/01_分布式调度与Worker执行边界.md)
- [节点模型与执行能力](./docs/architecture/decisions/02_节点模型与执行能力边界.md)
- [IR 编辑态结构](./docs/architecture/decisions/03_IR编辑态结构基线.md)
- [Control Graph 语义](./docs/architecture/decisions/04_ControlGraph语义基线.md)
- [项目实施计划与验收标准](./docs/plans/项目实施计划与验收标准.md)

## 第一阶段不做什么

- DSL Upload 或 DSL→IR Decompiler；
- Artifact Store；
- SQL、LLM、Agent、Human Approval、Plugin Node；
- Loop、Join、Optional Reference、Data Merge；
- Redis Streams、Kafka Delay Topic、Definition Event Topic；
- 提前拆成大量微服务。

这些能力可以后续增加，但不能以破坏 IR/DSL、版本、状态、调度和 Worker 边界为代价。

## 开发状态

M0 已提供四个可构建入口、三套严格配置 Profile、Local Compose、健康检查、Migration Runner、基础可观测性与依赖护栏。M1 已冻结并实现作者态 IR 与 Node Catalog Contract；DSL、Compiler、Definition 持久化和 Runtime 仍是后续阶段，README 不把计划能力描述成当前成果。
