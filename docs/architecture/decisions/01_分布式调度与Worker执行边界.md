# 分布式调度与 Worker 执行边界

> 状态：已冻结的架构决策基线
>
> 范围：最近几轮关于分布式部署、项目公平调度、Kafka、Worker、Attempt 恢复的讨论
>
> 前置基线：[00_核心架构决策基线.md](./00_核心架构决策基线.md)

本文只记录最近几轮新增或修正的决策，不重复 Definition、Draft/Published Version、IR/DSL、Source Map、Run/Node Run 基础生命周期等既有结论。若后续实现细节与本文冲突，以本文中的边界和不变量为准；尚未冻结的参数不得被误写成领域规则。

## 1. 分布式范围与第一阶段部署形态

系统中需要支持分布式的部分包括：

- 无状态 API、Definition Service 和 Compiler 的水平扩展；
- 多 Engine 实例共同推进大量 Workflow Run；
- 多 Scheduler 实例进行项目级公平准入；
- Kafka TaskBus 和运行事件的分布式传输；
- 按资源类独立扩缩容的 Worker Pool；
- 多实例 Outbox Relay、Inbox Consumer、Retry Timer 和 Recovery Scanner；
- Redis 调度共享状态和 Active Run Hot Cache；
- PostgreSQL 作为第一阶段的权威持久层；Redis 和 Kafka 均不承担权威持久化。

第一阶段采用：

```text
模块化 Control Plane（可多副本）
+ 独立 Worker Pool（按资源类扩缩容）
+ PostgreSQL / Scheduling Redis / Cache Redis / Kafka
```

“模块化 Control Plane”表示 API、Definition、Compiler、Engine、Scheduler、Attempt Coordinator、Outbox Relay、Retry/Recovery 等是边界清楚的逻辑模块，但第一阶段允许它们位于同一个部署单元中。是否拆成独立微服务是后续部署选择，不改变模块接口和状态所有权。

项目可执行程序边界冻结为：

```text
evalfrog                    Agent/Human 使用的 CLI
evalfrog-control-plane      External API + Control Plane 逻辑模块
evalfrog-worker-builtin     HTTP/RPC Resource Class
evalfrog-worker-sandbox     Code/Sandbox Resource Class
```

`evalfrog-control-plane` 使用同一套代码和一个镜像。默认部署可以启用全部角色；未来可通过启动 Role 分别运行 API、Runtime Consumer、Scheduler、Relay/Recovery 或 Projection，以便按容量独立扩展，但 Role 选择不得改变模块接口、事务边界和状态所有权，也不要求拆仓库。

Scheduling Redis 与 Cache Redis 是不同逻辑接口，生产环境使用不同 Endpoint 隔离淘汰策略、容量和故障域；本地开发可以共用。不同 Endpoint 不等于必须立即建设 Redis Cluster，第一阶段可以分别使用高可用主从部署，Lane 与 Run/Snapshot Key 设计保证未来能够平滑扩展为 Cluster。

Engine 不永久持有某个 Workflow Run。它是无状态、事件驱动、可恢复的状态推进器；任何实例都可以依据 Inbox、数据库权威状态和 CAS 接管同一个 Run 的后续推进。

## 2. 调度顺序与项目公平性

调度不是全局 FIFO。公平调度的唯一身份是 `project_id`；Node Type、Worker Pool、执行安全边界和 Kafka Topic 均不参与项目公平份额计算。跨 Project 和 Project 内部采用两级规则：

```text
第一级：在有竞争的 Project 之间等权轮转
第二级：在选中的 Project 内选择 Ready Node
```

项目间规则：

- 不设置 `project_priority`、`project_weight` 或固定 30% 配额；
- 每个调度周期根据有 Ready Task 的竞争项目集合，使用等权 Max-Min Fairness 动态计算项目基础额度；
- 当共有 `N` 个有充分需求的竞争项目时，每个项目的基础份额约为总派发容量的 `1/N`；
- 某个项目需求不足时，未使用容量由其他有需求项目等权借用，系统保持 Work-Conserving；
- 新竞争项目出现后，已超出新份额的项目停止获得新额度，但不抢占其正在执行的 Attempt；
- `inflight = queued + claimed + running`；其中 `claimed` 是领取过程，成功 Claim 后持久状态计入 `running`，不增加新的 Node Run 状态；
- `ready` 与 `retry_wait` 不计入 Inflight。

“竞争项目”是当前仍有 Ready Task 需要准入的 Project。只有 Running Task、但没有新 Ready Task 的 Project 不占用新的调度份额，其既有 Inflight 仍计入额度判断。为避免项目集合短暂变化造成额度抖动，Scheduler 使用带 Epoch 版本的周期性快照和可配置 Active TTL；周期长度与 TTL 是容量参数，不是领域语义。

Project 内部选择顺序固定为：

```text
priority DESC
ready_at ASC
node_run_id ASC
```

因此，相同 Priority 下是稳定 FIFO；它只保证派发顺序，不保证节点完成顺序。全局不存在跨 Project FIFO，因为它会让大项目的长队列压制其他项目，违背公平性目标。

多 Scheduler 实例不能各自维护彼此独立的本地项目轮转状态。所有实例共享 Redis 中的项目公平状态，并由数据库 CAS 对 `ready → queued` 作最终裁决。Redis 状态丢失时，从数据库中的 `ready/queued/running` 事实重建后再恢复新准入。

### 2.1 有界 Dispatch Window

Kafka 之前必须设置有界 `dispatch_window`：

```text
admitted = queued + claimed + running
admitted <= dispatch_window
```

`dispatch_window` 只允许接近 Worker 短期消化能力的 Attempt 进入 Kafka，可包含少量传输缓冲；其余 Node Run 保持数据库权威的 `ready` 状态。禁止将某个 Project 的大量 Ready Task 深度预派发到 Kafka，否则后加入的 Project 无法被 Scheduler 重新公平排序，取消和额度调整也会被已经形成的 Kafka 积压架空。

Worker Pool 的可用执行槽仍是派发可行性约束，但不形成第二套公平身份：项目额度跨其全部 Task 统一计算，Task 只有同时具备项目 Credit 和目标 Worker Pool 可用容量时才能准入。

### 2.2 固定 Scheduling Lane 与批量 Credit

Redis 调度状态使用固定数量的逻辑 Scheduling Lane，而不是一个承载所有 Project 的全局有序集合：

```text
lane_id = stable_hash(project_id) % lane_count
```

Lane 是 Redis 调度数据的逻辑分片，不是 Redis 实例，也不是 Kafka Partition。同一个 Project 的 Active 标记、Ready 索引、Inflight Reservation、轮转位置和临时额度始终归入同一个 Lane。`lane_count` 是预先配置且显著多于初始 Redis 主节点数的容量参数；Redis Cluster 通过 Slot 迁移扩容，不改变 Project 到 Lane 的稳定映射。未来确需改变 Lane 数量时必须采用版本化迁移，不能直接取模重映射正在调度的项目。

同一 Lane 的 Key 使用 Redis Cluster Hash Tag 共置，使项目轮转、Credit 扣减和 Reservation 建立可以通过 Lane 内原子操作完成，例如：

```text
sched:{lane-17}:active-projects
sched:{lane-17}:project:{project_id}:ready
sched:{lane-17}:project:{project_id}:inflight
sched:{lane-17}:credits
```

每个 Scheduling Epoch，Control Plane 内的 Credit Balancer 根据各 Lane 汇总的竞争项目数、Ready 需求、既有 Inflight 和全局 `dispatch_window`，以等权 Max-Min Fairness 批量分配 Lane Credit。Lane 内每次准入只访问本 Lane Redis 数据并消耗一个 Credit；跨 Lane 只进行低频汇总、额度发放、回收和再平衡，不在每个 Task 的热路径上访问单个全局 Key。

Credit Balancer 是 Control Plane 内带 Lease 与 Fencing 的逻辑模块，不要求第一阶段拆成独立微服务。未使用 Credit 可在后续 Epoch 回收并分配给仍有需求的 Lane；短周期内允许近似公平，持续竞争时必须不饥饿。

## 3. 四个不得混淆的概念

| 概念 | 回答的问题 | 示例 |
|---|---|---|
| Node Type | 这个节点的业务语义是什么 | Code、HTTP、RPC |
| Resource Class | 需要什么安全边界和运行资源 | `builtin`、`sandbox` |
| Kafka Topic | Task 应路由到哪一类 Worker Pool | Builtin Task Topic、Sandbox Task Topic |
| Kafka Physical Partition | 一个 Topic 如何并行存储和消费 | Partition 0..N-1 |

它们不是一一对应关系：

- 多种 Node Type 可以映射到同一 Resource Class；
- 一个 Resource Class 对应一个逻辑 Task Topic；
- 一个 Topic 可以有多个 Physical Partition；
- 不按 Node Type 或 Project 创建 Topic；
- IR/DSL 不包含 Topic 名称，避免业务定义耦合基础设施拓扑。

平台内部的版本化 Node Catalog 描述 Node Schema 和执行能力要求；Web 与 Agent 只看到无版本号的 Node Description。Runtime Routing Policy 在发布或派发阶段解析 `resource_class` 和 Topic；Task Contract 只携带解析结果及执行身份。第一阶段不建设独立 Manifest 或 Registry 服务。

这里的 Resource Class 只描述 Worker 执行兼容性与安全隔离，不是公平调度维度。无论 Task 最终路由到哪个 Worker Pool，公平身份始终只有 `project_id`。

## 4. 第一阶段 Worker 资源类

第一阶段冻结两个最小资源类：

### 4.1 builtin

用于平台编写、受信任、允许在长生命周期 Worker 进程内运行的 Executor。第一阶段 Builtin Worker 同时支持 `HttpExecutor` 与 `RpcExecutor`。

### 4.2 sandbox

用于 Code Node。Sandbox Worker 的职责是创建、监控和销毁每个 Attempt 的隔离执行环境；用户代码不得直接运行在 Worker 主进程中，也不得继承 Worker 的数据库、网络或 Secret 权限。

资源类依据安全隔离、资源模型、依赖和配额划分，而不是简单依据 Node Type 划分。未来可以增加 `llm`、`io`、`gpu` 等资源类，但这属于 Routing Policy 演进，不修改 IR/DSL 语义。

## 5. Worker Runtime 与 Executor Catalog

Worker 由通用 Runtime 和进程内 Executor Catalog 组成：

```text
Worker Runtime
├─ Task Consumer
├─ Local Concurrency / Backpressure
├─ Attempt Client
├─ Execution Context Client
├─ Timeout / Cancellation
├─ Telemetry
└─ Executor Catalog
   ├─ Builtin Pool: HttpExecutor / RpcExecutor
   └─ Sandbox Pool: CodeExecutor / Sandbox Orchestrator
```

Worker 根据 DSL Operation Type 与 Version 从进程内 Catalog 解析 Executor，不在中心流程中使用不断增长的 Node Type `if/else` 或 `switch-case`。Catalog 只是 Handler Map，不是独立服务或领域实体。

Worker-facing 边界第一阶段使用版本化 HTTP/JSON，至少包含：

```text
ClaimAttempt
HeartbeatAttempt
CompleteAttempt
LoadExecutionContext
```

前三项由 Attempt Coordinator 提供；`LoadExecutionContext` 由 Control Plane 内显式的 Execution Context Gateway 提供。Gateway 统一读取不可变 Snapshot、Run Input 和已经被 `effective_attempt_id` 接受的上游 Output，封装 Cache Redis 优先、PostgreSQL 回源与回填，并根据 Resource Class 裁剪资源信息。它是内部只读模块，不是独立微服务，不得修改 Run、Node Run 或 Attempt 状态。

HTTP/JSON 只是第一阶段 Transport Adapter。Worker Runtime 依赖 Transport-neutral Client Port；未来增加 gRPC 不得改变 Claim、Lease、Fencing、ACK 或 Execution Context 的语义。

第一阶段同一 Resource Class 的 Worker Pool 必须能力同构：每个副本都支持 Routing Policy 可能路由到该 Pool 的全部 Executor。Worker 在 Claim 前仍需上报并校验自身 Resource Class 与 Capability，防止错误部署导致不兼容 Task 进入 `running`。

M7 实现将这条约束固化为三层防线：Worker Executor Catalog 启动时必须覆盖该 Resource Class 的完整 Routing Capability；Worker Heartbeat 注册必须携带同一完整集合；Scheduling Redis 只把能力指纹匹配当前 Routing Policy 且 TTL 未过期的 Slot 计入 Dispatch Window。Attempt Claim 仍逐任务复核 Operation/Resource Class/Capability，作为最终数据库防线。这样滚动发布期间的旧能力 Worker 不会继续扩大新版本调度容量。

只有本地执行槽可用时 Worker 才领取并 Claim Task；本地等待队列必须有界，不能依靠无限预取制造隐藏积压。

Kafka Consumer 在 `Poll → PostgreSQL Claim/Inbox → ACK` 的短临界区使用 Rebalance Gate，防止分区撤销后旧消费者提交 Offset；ACK、NACK 或进程关闭都会释放 Gate。Task ACK 后的 Executor 运行不阻塞 Kafka Rebalance，节点执行可靠性由 Attempt Lease/Reaper/Fencing 承担，而不是依赖消费者长期拥有分区。

## 6. Kafka Topic 与 Physical Partition

Kafka 被选为 TaskBus 和运行事件的传输实现，但不承担权威 Workflow 状态。

第一阶段只建立三个逻辑 Topic：

```text
workflow-task-builtin-v1  → Builtin Worker Pool
workflow-task-sandbox-v1  → Sandbox Worker Pool
workflow-runtime-event-v1 → Engine / Read Model / Web Notification Projector
```

第一阶段不建立 Definition Event Topic。Worker 与 Engine 都不订阅定义发布事件：Worker 只执行 Attempt，Engine 只读取 Run 已固定绑定的 `snapshot_id`；Execution Snapshot 不可变且按 ID 缓存，不需要发布失效事件。未来只有出现明确外部订阅者时才新增 Definition Event。

Topic 内仍需要多个 Physical Partition。即使 Topic 里的任务最终由同一类 Worker 执行，Partition 仍负责 Broker 存储、Leader 吞吐和并行消费扩展；在普通 Consumer Group 下，活跃 Consumer 数也受 Partition 数限制。因此“Worker 同类”不能推出“Topic 不需要分区”。

Task 使用 `attempt_id` 作为 Partition Key，以分散负载；Runtime Event 使用 `run_id`，使同一 Run 的信号尽量保持 Partition Locality。Task 之间没有业务顺序，Runtime Event 顺序也只作为优化；Engine 的正确性来自数据库状态、Inbox 去重和 CAS，而不是 Kafka 分区顺序。

第一阶段全部使用普通 Consumer Group。Share Group 只保留为 TaskBus Adapter 的未来替换能力，不改变 Claim、ACK、幂等和恢复语义。Partition 数量是容量配置，不进入领域模型。

Kafka Message 使用带 `message_version` 的独立 JSON DTO，不直接序列化数据库实体。Task 与 Runtime Event 只携带身份、版本、时间和 Trace 等轻量定位信息，不携带完整 DSL、Workflow Context、Node Output 或 Secret。可识别 `attempt_id` 的 Poison Task 必须先通过 Coordinator 结算 Attempt；完全无法识别身份的消息才进入对应 DLQ，DLQ 只做运维隔离，不驱动业务 Retry。

## 7. Task 领取、ACK 与完成链路

Worker 不直接访问 Workflow 数据库。Control Plane 内的 Attempt Coordinator 对 Worker 暴露三个逻辑操作：

```text
ClaimAttempt
HeartbeatAttempt
CompleteAttempt
```

完整链路为：

```text
Worker 获得本地执行槽
→ 从 Kafka 领取 Task
→ ClaimAttempt
→ 数据库事务：queued → running + Lease/Fencing
→ Claim 成功后 ACK Kafka Task
→ Worker 执行 Executor
→ CompleteAttempt
→ 数据库事务：Attempt 终态 + 带 attempt_id 的 Output Candidate + Completion Outbox
→ Outbox Relay 发布 Completion Event
→ Engine Inbox 去重 + 校验 Current Attempt
→ CAS 推进 Node Run / Workflow Run
→ 成功时设置 effective_attempt_id，Output 才对下游可见
```

ACK 不等待节点执行完成。若一直持有 Kafka 消息直到长任务结束，会把任务 Lease 与消息消费 Lease 错误绑定，并放大 Rebalance、重复投递和超时问题。ACK 之后 Worker 故障，由数据库 Lease、Heartbeat 和 Recovery Scanner 找回 Lost Attempt。

若 Claim 返回“已由当前或其他合法执行者领取、已终结、已取消”等幂等结果，Worker 按 Coordinator 返回的处置结果 ACK 或忽略，不自行猜测状态。

Poison Task 必须先记录持久化错误。只要能够识别 `attempt_id`，就必须通过 Coordinator 将 Attempt 结算为可恢复或终态，再最终 ACK/Reject；禁止让它永久停留在 `queued`。

## 8. Attempt 身份、重试与恢复

三个计数维度必须分开：

| 字段 | 含义 | 是否消耗业务重试预算 |
|---|---|---|
| `attempt_seq` | 每次物理执行的单调序号，也是 Fencing 维度 | 否，仅作执行身份 |
| `retry_count` | 业务错误触发的重试次数 | 是 |
| `recovery_count` | Worker/Lease/基础设施丢失后的恢复次数 | 否，受独立上限约束 |

Lost Attempt 不自动消耗业务重试预算，否则平台故障会错误减少用户配置的业务容错机会。基础设施恢复仍必须受 `max_recoveries`、Attempt Timeout 和 Workflow Deadline 限制，避免无限恢复。

有效完成结果至少需要匹配：

```text
current_attempt_id
+ attempt_seq
+ lease_token / fencing_token
+ expected running state
```

旧 Worker 的迟到 Heartbeat 或 Result 必须被拒绝。业务 Retry 的到期时间保存在数据库中，由 Retry Timer 扫描并重新置为可调度状态；第一阶段不使用 Kafka Delay Topic 充当权威计时器。

## 9. 数据一致性与故障恢复不变量

1. PostgreSQL 保存 Workflow、Node Run、Attempt、Lease、Output Reference、Outbox 和 Inbox 的权威状态。
2. Redis 保存可重建的调度共享状态、Inflight Reservation、Active Run Hot Cache 和 Read Model，不参与最终语义裁决。
3. Kafka 采用至少一次投递假设；不依赖 Kafka Exactly Once 覆盖外部数据库或节点副作用。
4. Scheduler 创建 Attempt 与 NodeTask Outbox 必须处于同一数据库事务。
5. Attempt Coordinator 写 Attempt 终态、带 `attempt_id` 的 Output Candidate 与 Completion Outbox 必须处于同一数据库事务；Engine 接受 Attempt 并设置 `effective_attempt_id` 后 Output 才成为有效值。
6. Engine 先写 Inbox 去重记录，再以数据库状态和 CAS 推进 Run；重复或乱序 Completion Event 不得重复推进。
7. Outbox Relay、Inbox Consumer、Retry Timer 和 Recovery Scanner 均可多实例运行，但必须使用幂等操作及数据库 Claim/Lease，不能依赖单实例内存所有权。
8. Kafka Task 只携带执行身份和定位信息，不携带完整 Workflow Context；输入通过 Execution Context 接口读取。
9. Worker 不持有 Workflow 数据库凭证；执行层故障不能绕过 Control Plane 状态机写库。
10. 外部副作用使用稳定的逻辑幂等键，Attempt 恢复或业务重试不得生成新的业务副作用身份。
11. Outbox 只携带轻量身份和定位信息，消费者重新读取数据库权威状态；Inbox 至少以 `consumer_name + event_id` 唯一去重。
12. Runtime Outbox/Inbox 与 Run 数据同分片，不引入跨项目分布式事务；第一阶段不写无消费者的 Definition Outbox。
13. Effective Output 必须先在 PostgreSQL 中持久化并由 Engine 通过 `effective_attempt_id` 接受，再以 Post-Commit Best-Effort 方式预热 Redis；Cache 失败不回滚状态，也不阻塞后续推进。
14. Workflow 核心权威数据不得 Redis Write-Behind；Scheduling Lane/Credit/Reservation 是可重建协调状态，不是待回写数据库的权威副本。

## 10. 第一阶段容量参数基线

容量配置提供 `local`、`test`、`production-default` 三套 Profile。本节冻结参数名称、生产默认值、参数间硬约束和调优信号；数值属于可配置的首版起点，必须通过目标环境压测校准，不是领域语义、SLO 承诺或固定容量上限，也不得反向渗透进 IR/DSL 或改变状态所有权。

### 10.1 参数间硬约束

```text
heartbeat_interval < lease_duration / 3
lost_after >= lease_duration + reaper_scan_interval
reservation_ttl >= lost_after + reconcile_interval
inbox_retention > kafka_retention + max_manual_replay_window
dispatch_window ≈ healthy_worker_slots × small_buffer_factor
所有应用实例的 DB Pool Max 之和 <= PostgreSQL max_connections × 70%
Task Topic Partition 数 >= 计划中的同组活跃 Consumer 数
```

Redis TTL 仍不承担业务计时语义；这些关系只保证协调状态、恢复扫描和消息重放之间不产生明显时间漏洞。

### 10.2 Kafka `production-default`

| Topic | Partition | Retention |
|---|---:|---:|
| `workflow-task-builtin-v1` | 12 | 24 小时 |
| `workflow-task-sandbox-v1` | 12 | 24 小时 |
| `workflow-runtime-event-v1` | 24 | 72 小时 |
| 对应 DLQ | 不少于 6，建议与源 Topic 相同 | 14 天 |

Topic 与 Broker 侧默认：Replication Factor `3`、Min ISR `2`、`cleanup.policy=delete`、禁止自动创建 Topic、Topic 最大消息 `256 KiB`。应用层进一步将 Task/Runtime Event JSON Envelope 限制为 `64 KiB`，大对象仍通过权威存储按 ID 读取。

Producer 默认：`acks=all`、启用幂等生产、`max.in.flight.requests.per.connection=5`、`compression.type=zstd`、`linger.ms=5`、`batch.size=64 KiB`、Request Timeout `30s`、Delivery Timeout `120s`。

Consumer 默认：关闭自动提交、初始 Offset 使用 `earliest`、Session Timeout `45s`、Heartbeat `3s`、Max Poll Interval `60s`、Cooperative Sticky 分配策略、`fetch.min.bytes=1`。Task Consumer 每次 Poll 的应用层领取上限为 `min(local_free_slots, 32, floor(max_poll_interval / claim_timeout / 2))`，最后一项为 Rebalance Gate 保留 50% 安全余量；Runtime Event Consumer 的 `max.poll.records` 为 `100`。

Partition 是消费并行度上限而不是 Worker 执行槽上限。第一阶段若每个 Worker 进程只有一个 Consumer，则两个 Task Pool 各最多有 `12` 个同组活跃 Consumer；单个 Consumer 可在本进程内向多个本地执行槽分发，但本地队列必须有界。预计活跃 Consumer 超过 Partition 数之前必须先扩 Partition 并验证 Key 分布。

### 10.3 Scheduler 与 Scheduling Redis `production-default`

| 参数 | 默认值 |
|---|---:|
| `lane_count` | 128 |
| Scheduling Epoch | 1000 ms |
| Active Project TTL | 5000 ms |
| Credit Grant Batch | 8 |
| Redis Candidate Batch | 32 |
| Admission Concurrency / Scheduler Instance | 8 |
| Inflight Reservation TTL | 300000 ms |
| Ready Reconcile Interval | 30000 ms |
| Global Dispatch Window | `ceil(total_healthy_worker_slots × 1.2)` |
| Pool Feasibility Window | `ceil(pool_healthy_worker_slots × 1.2)` |
| 每 Epoch 容量上调上限 | 10%；容量下调立即生效，避免向失去健康槽位的 Pool 继续派发 |
| 基础设施错误 Backoff | 50 ms 指数退避，最大 2s，20% Jitter |
| Idle Poll | 100 ms，最大退避至 1s |

Pool Window 只判断路由目标是否有短期消化能力，不建立第二套公平额度。Scheduler 从 Redis 每批最多取 `32` 个候选，并以最多 `8` 路并发执行逐 Attempt 的数据库 CAS；CAS 仍是 `ready → queued` 的最终裁决。

`Credit Grant Batch=8` 是 Redis 中一次发放/扣减的摊销粒度，不是“每个 Lane 每秒最多 8 个 Task”的限流上限；一个 Epoch 可以依据全局 Window 向同一 Lane 发放多个 Batch，但所有 Lane 的可用 Credit 总和不得突破当期可派发容量。

Scheduling Redis 使用 `noeviction`，单次操作 Timeout `200 ms`、最多重试 `1` 次；不可用时按既有 Fail Closed 规则暂停新准入。

### 10.4 Cache Redis `production-default`

Cache Redis 使用 `allkeys-lfu`，单次操作 Timeout `500 ms`、不在请求内重试，失败时回源 PostgreSQL。默认 TTL：

| 数据 | 默认 TTL |
|---|---:|
| Execution Snapshot | 24 小时，并加 ±10% Jitter |
| Active Run Context | 6 小时；接受新 Effective Output 时续期 |
| Terminal Run Context | 1 小时 |
| Active Run Read Model | 15 分钟；活跃时刷新 |
| Terminal Run Read Model | 1 小时 |
| Definition Negative Cache | 30 秒 |

Inflight Reservation 的 `5 分钟` TTL 属于 Scheduling Redis，并在 Attempt 保持 Inflight 时续期，不得与 Cache Store 中的业务读取缓存混淆。

### 10.5 Worker Lease 与恢复 `production-default`

| 参数 | 默认值 |
|---|---:|
| Heartbeat Interval | 15 秒 |
| Lease Duration | 60 秒 |
| Lost 判定阈值 | 75 秒 |
| Recovery Scanner Interval | 10 秒 |
| `max_recoveries` | 3 |
| Claim API Timeout | 5 秒 |
| Complete API Timeout | 10 秒 |
| Recovery Backoff | 5 / 15 / 45 秒 |

Heartbeat、Lease 与 Lost 的数据库时间判断使用服务端时间；Worker 本地时钟不能决定 Attempt 是否失效。业务 Timeout 与 Workflow Deadline 可以早于上述恢复阈值，并始终优先阻止新的恢复执行。

### 10.6 Outbox、Inbox 与扫描器 `production-default`

| 参数 | 默认值 |
|---|---:|
| Outbox Batch | 100 |
| Outbox Active Poll | 100 ms |
| Outbox Idle Poll | 最大退避至 1 秒 |
| Outbox Claim Lease | 30 秒 |
| Publish Concurrency | 8 |
| Publish Retry | 200 ms 指数退避，最大 30 秒，20% Jitter |
| Published Outbox Retention | 7 天 |
| Inbox Retention | `max(7 天, 对应 Kafka Retention + 24 小时)` |
| Inbox/Outbox Cleanup | 30 分钟 |
| Retry Timer | 1 秒 |
| Deadline Scanner | 5 秒 |
| Recovery Scanner | 10 秒 |
| Completion/Run Reconciler | 30 秒 |
| 单次扫描 Batch | 100 |

最老未发布 Outbox 超过 `30 秒`或同一记录连续失败 `10` 次时告警。未发布 Outbox 和 DLQ 消息不得由普通保留任务自动删除。

### 10.7 PostgreSQL `production-default`

Control Plane 每实例连接池默认 `min=5`、`max=20`；Statement Timeout `5s`、Lock Timeout `500ms`、Idle In Transaction Timeout `10s`，默认事务隔离级别 `READ COMMITTED`。部署校验必须保证所有实例连接池 Max 总和不超过数据库 `max_connections` 的 `70%`，剩余连接用于迁移、运维和故障恢复。Worker 不持有 Workflow PostgreSQL 连接。

Scheduler 每个 Attempt 使用短事务执行带 `state_version` 的条件更新；单次从 Redis 获取 `32` 个候选，最多 `8` 路并发。Run 初始化仍是一个原子事务，Node Run 使用每批 `500` 行的批量 Insert 语句，不能为减少事务时长而拆成可见的半初始化状态。

### 10.8 `local` 与 `test` Profile

三个 Profile 使用继承覆盖模型：`production-default` 是完整基线；`test` 和 `local` 只覆盖下表列出的容量值，未列出的消息大小、可靠性语义、状态机边界和超时关系全部继承 `production-default`。Profile 只能在进程启动时选择，运行期间不得热切换；不同 Profile 必须使用隔离的 PostgreSQL Schema/Database、Kafka Topic Prefix 和 Redis Key Prefix，不能对同一批运行数据混用。

Kafka 覆盖：

| 参数 | `local` | `test` | `production-default` |
|---|---:|---:|---:|
| Builtin Task Partitions | 1 | 3 | 12 |
| Sandbox Task Partitions | 1 | 3 | 12 |
| Runtime Event Partitions | 1 | 6 | 24 |
| DLQ Partitions | 1 | 3 | 不少于 6，建议与源 Topic 相同 |
| Replication Factor / Min ISR | 1 / 1 | 1 / 1 | 3 / 2 |
| Task Retention | 2 小时 | 12 小时 | 24 小时 |
| Runtime Event Retention | 4 小时 | 24 小时 | 72 小时 |
| DLQ Retention | 24 小时 | 3 天 | 14 天 |
| Max Manual Replay Window | 1 小时 | 24 小时 | 24 小时 |

所有 Profile 均禁止依赖 Broker 自动建 Topic，由部署脚本显式创建；均保持应用 Envelope `64 KiB` 和 Topic 最大消息 `256 KiB`，防止只在生产环境才暴露消息膨胀问题。

Scheduler、Lease 与数据库覆盖：

| 参数 | `local` | `test` | `production-default` |
|---|---:|---:|---:|
| `lane_count` | 8 | 32 | 128 |
| Scheduling Epoch | 500 ms | 500 ms | 1000 ms |
| Active Project TTL | 3 秒 | 3 秒 | 5 秒 |
| Credit Grant Batch | 4 | 8 | 8 |
| Redis Candidate Batch | 8 | 16 | 32 |
| Admission Concurrency / Instance | 2 | 4 | 8 |
| Reservation TTL | 30 秒 | 60 秒 | 300 秒 |
| Ready Reconcile Interval | 5 秒 | 10 秒 | 30 秒 |
| Heartbeat / Lease / Lost | 2 / 10 / 12 秒 | 5 / 20 / 25 秒 | 15 / 60 / 75 秒 |
| Recovery Scanner | 2 秒 | 2 秒 | 10 秒 |
| Recovery Backoff | 1 / 2 / 4 秒 | 2 / 5 / 10 秒 | 5 / 15 / 45 秒 |
| Outbox Batch / Publish Concurrency | 20 / 2 | 50 / 4 | 100 / 8 |
| Inbox、Published Outbox Retention | 24 小时 | 3 天 | 7 天 |
| PostgreSQL Pool Min / Max（每实例） | 1 / 5 | 2 / 10 | 5 / 20 |

所有 Profile 的 Global/Pool Dispatch Window 均按健康槽位乘 `1.2` 动态计算，`max_recoveries=3`，并继续满足 10.1 的硬约束。`local` 和 `test` 的较短 Lease/Scanner 只用于降低开发和故障测试等待时间，不改变 Lost、Fencing 或 Retry 语义。

Cache TTL 覆盖：

| 数据 | `local` | `test` | `production-default` |
|---|---:|---:|---:|
| Execution Snapshot | 1 小时 | 6 小时 | 24 小时 |
| Active Run Context | 30 分钟 | 2 小时 | 6 小时 |
| Terminal Run Context | 10 分钟 | 30 分钟 | 1 小时 |
| Active Run Read Model | 5 分钟 | 10 分钟 | 15 分钟 |
| Terminal Run Read Model | 10 分钟 | 30 分钟 | 1 小时 |
| Definition Negative Cache | 10 秒 | 15 秒 | 30 秒 |

`local` 允许 Scheduling Store 与 Cache Store 共用一个 Redis Endpoint；共用时必须整体采用 `noeviction`，Cache 仅依靠 TTL 清理，不能使用会淘汰调度 Key 的 `allkeys-lfu`。`test` 默认使用两个独立 Endpoint，以验证与生产一致的故障边界。

### 10.9 压测调优触发条件

以下指标触发调整，而不是凭节点数量预估：

- Kafka Consumer Lag 持续增长：先检查 Worker 执行槽和下游依赖，再评估 Consumer/Partition；
- Ready-to-Queued P95 超过 `1 秒`：检查 Redis/数据库 CAS 后，再提高 Scheduler 实例数或 Admission Concurrency；
- Scheduling Redis CPU 持续超过 `60%`：检查 Hot Lane，并评估分片或 Cluster；
- Execution Context Cache Hit Rate 低于 `80%`：检查 TTL、访问分布与容量；
- PostgreSQL Pool Wait P95 超过 `100 ms`：检查慢事务和实例总连接数，不能直接无限增大 Pool；
- 最老未发布 Outbox 超过 `30 秒`：检查数据库 Claim、Kafka 生产和 Relay 容量；
- Task Topic 积压显著超过有界 Dispatch Window：视为准入或记账缺陷，而不是正常深队列。

Kafka Partition、Redis Lane/TTL、Scheduler Batch/Backoff、Worker Lease/Heartbeat、Outbox/Inbox Polling 和数据库连接池不再分别提升为架构议题。尚余架构讨论只包括接口权限闭环与实现蓝图两组。

第一阶段节点能力和安全边界详见 [02_节点模型与执行能力边界.md](./02_节点模型与执行能力边界.md)。
