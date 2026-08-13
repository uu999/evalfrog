# M8 面试复习：Managed Resource Runtime

M8 的关键不是把网络请求塞进 Worker，而是保留资源边界：IR/DSL 只保存 `connection_ref` 或编译后的稳定资源 ID；Worker 通过 Control Plane 的运行时 Port，以 `project_id + run_id + attempt lease/fencing` 再次校验 execution identity、grant 和资源 revision。这样 Worker 没有数据库凭据，资源撤权可以在运行时生效，且每次 Attempt 都能审计实际使用的 Resource Revision。

HTTP Executor 只解析受管 Connection 的相对路径。拒绝绝对 URL、Host 覆盖、路径逃逸、重定向和超限响应；认证 Header 由 Secret Resolver 在最后一跳注入。外部副作用使用稳定 `run_id:execution_node_id` 作为 Idempotency-Key，避免 Worker Lost 后恢复重试产生重复业务写入。

RPC Executor 只解析 Service Catalog 登记的 `service_id + operation + contract_revision`，服务发现和协议（第一阶段 `http-json`）属于 Executor Port，不进入 IR/DSL。非登记服务、操作或契约 revision 不兼容都在调用前失败。

高频面试题：为什么不让 Worker 直接读数据库？因为那会绕过 Attempt Lease、Project 隔离、Execution Identity 和 Effective Output 规则；为什么 Secret 不进 Context？因为 Context 会被缓存、传输和诊断，短暂凭据应只存在于 Executor 调用栈；为什么资源 revision 要审计？因为受管资源可变，而不可变 Workflow Snapshot 不能假设运行时资源永远不变，审计才能重现“当时实际调用了哪一版资源”。
