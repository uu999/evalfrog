# EvalFrog Web

Human Web Canvas is an independent static artifact introduced in M10. It only
calls the versioned External API through `/api`; it never uploads DSL or Source
Map, never owns runtime state, and edits the same Canonical IR as Agent CLI.

`npm run build` copies the dependency-free static artifact to `web/dist`.
`npm test` verifies the UI stays on the public authoring/runtime surface.

使用时先填写 Project、Workflow、Token 与 API Base URL，再点击“加载 Draft”。Canvas 支持目录示例节点、直接 IR 编辑与拖拽 Layout；所有保存、校验、测试、发布、正式运行和取消都走同一 External API。运行进度通过 SSE wake-up 触发重新读取 Run Read Model，不以浏览器本地状态作为权威。
