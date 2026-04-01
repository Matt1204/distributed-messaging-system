# 实时性能实验设计与执行计划（Ack Latency + End-to-End Delivery Latency）

## 1. 目标与度量定义

本实验只回答两个核心问题：

- 在不同工作负载下，发送端多久收到 `SendMessageAck`？
- 在不同工作负载下，消息多久到达接收端（端到端）？

统一指标定义：

- `Ack latency (ms) = t_ack_recv_sender - t_send_start_sender`
- `E2E delivery latency (ms) = t_inbound_recv_receiver - t_send_start_sender`
- 统计口径：`P50 / P95 / P99`，同时记录失败率和超时率。

说明：`t_send_start_sender`、`t_ack_recv_sender`、`t_inbound_recv_receiver` 全部使用 `System.nanoTime()` 记录，最后转毫秒。

## 2. 建议实验集合（先做这 3 组）

### 实验 A：基线延迟（单会话对）

目的：先拿到系统在低压下的真实性能基线，并验证测量链路正确。

设置：

- 1 对用户（1 sender + 1 receiver）
- 1 msg/s
- 总时长 5 分钟：前 1 分钟 warm-up，后 4 分钟计入结果

产出：

- Ack/E2E 的 P50/P95/P99
- 失败率、超时率

### 实验 B：单对高压速率爬坡（rate sweep）

目的：观察同一会话在逐步加压时的延迟退化趋势，找到拐点。

设置：

- 仍为 1 对用户
- 发送速率分档：5 / 10 / 20 / 40 msg/s
- 每档 4 分钟：前 1 分钟 warm-up，后 3 分钟计入

产出：

- `rate -> Ack/E2E P95/P99` 曲线
- 首个明显恶化点（例如 P95 突增）

### 实验 C：并发会话爬坡（concurrency sweep）

目的：验证“多用户并发”场景下的可扩展性。

设置：

- 会话对数量：1 / 5 / 10 / 20 对
- 每个 sender 固定 5 msg/s
- 每档 6 分钟：前 1 分钟 warm-up，后 5 分钟计入

产出：

- `并发对数 -> Ack/E2E P95/P99` 曲线
- 总吞吐、失败率趋势

## 3. 实验前准备（必须完成）

### 3.1 环境冻结

- 固定 server 配置、实例数、Redis/Cosmos 配置，整组实验不变。
- 固定网络路径与部署区域。
- 每次跑实验记录 git commit hash 与配置快照。

### 3.2 测试账号与数据隔离

- 使用专门压测账号，例如 `perf_sender_01@...`、`perf_receiver_01@...`。
- 每个 run 使用唯一 `run_id`，例如 `run_2026-04-01_expC_p10_r5`。
- 每个 run 写独立输出文件，不覆盖历史结果。

### 3.3 运行目录与结果目录

建议在仓库下固定：

- `results/raw/`：每条消息级别原始事件
- `results/summary/`：每次 run 的汇总指标
- `results/meta/`：本次 run 的配置、环境、时间戳

### 3.4 开跑前烟雾测试

- sender/receiver 均能 login 成功。
- 发送 50 条小样本，确认 ack 回调可达。
- 发送 50 条小样本，确认 inbound 回调可达。
- 发送 50 条小样本，确认同一消息可完成 sender->ack->receiver 的关联。

## 4. 当前 GUI 耦合现状与技术改造方案

你当前客户端入口 `ChatClient` 会直接起 Swing `ChatWindow`，但好消息是：`ChatClientSession` 已经可以脱离 GUI 单独使用。

推荐做法：新增一个 headless 入口，不改现有 GUI 行为。

### 4.1 新增 headless 压测入口

在 `chat-client` 增加 `PerformanceLoadRunner`（单独 main class）：

- 直接创建多个 `ChatClientSession`
- 为每个 session 绑定自定义 `ClientUiListener`
- 由 runner 统一调度发送速率、计时窗口、结果落盘

### 4.2 关键打点缺口（必须补）

当前接口有两个缺口：

- `sendMessage(...)` 内部生成 `clientMsgId`，调用方拿不到“发送开始时间对应的 message id”。
- `onSendAck(...)` 只回传 `clientMsgId/success/code/reason`，拿不到 `serverMsgId`，无法直接与 receiver 侧 `onChatMessage(msgId=serverMsgId)` 关联。

### 4.3 最小改造建议（低风险、可回滚）

建议仅做以下最小改动：

- 在 `ChatClientSession` 增加重载方法：允许调用方传入 `clientMsgId`（或返回该 id）。
- 扩展 `ClientUiListener` 的 ack 回调，增加 `serverMsgId`（可同时带 `conversationId/sequenceId`）。
- 不改 `ChatWindow` 逻辑，只在新字段上做默认空实现或忽略。

推荐方案示例：

- 保留现有 `onSendAck(...)`，新增 `onSendAckDetailed(...)` 默认方法。
- `ServerResponseHandler.handleSendMessageAck(...)` 同时触发详细回调。

这样做的好处：

- GUI 路径不受影响。
- headless runner 可以做稳定的消息级关联。
- 后续做更多实验（重连、catchup）也可复用该打点通道。

## 5. 实验执行步骤（每个矩阵点都一致）

1. 启动服务端，确认健康。
2. 启动 `PerformanceLoadRunner`，加载本次 run 配置。
3. 自动 register/login 测试用户。
4. warm-up 阶段（不计入统计）。
5. measured 阶段按目标速率持续发送。
6. 停止发送，等待 in-flight 消息回补窗口（例如 10 秒）。
7. 落盘原始事件与汇总结果。
8. 保存 run 元数据（配置、commit、开始结束时间）。

## 6. 数据记录方案

### 6.1 原始事件表（逐消息）

建议文件：`results/raw/<run_id>_events.csv`

字段：

- `run_id`
- `scenario_id`
- `pair_id`
- `client_msg_id`
- `server_msg_id`
- `send_start_ns`
- `ack_recv_ns`
- `inbound_recv_ns`
- `ack_success`
- `ack_error_code`
- `ack_error_reason`
- `payload_bytes`

派生字段（后处理计算）：

- `ack_latency_ms`
- `e2e_latency_ms`
- `is_ack_timeout`
- `is_e2e_timeout`

### 6.2 汇总统计表（逐 run）

建议文件：`results/summary/latency_summary.csv`

字段：

- `run_id`
- `scenario_id`
- `pairs`
- `rate_per_sender`
- `attempted_messages`
- `acked_messages`
- `received_messages`
- `ack_fail_count`
- `ack_p50_ms`
- `ack_p95_ms`
- `ack_p99_ms`
- `e2e_p50_ms`
- `e2e_p95_ms`
- `e2e_p99_ms`

## 7. 统计与质量控制

- 首分钟 warm-up 数据不进最终统计。
- 固定消息大小做主实验，再做 1 组大 payload 敏感性对比（可选）。
- 明确超时阈值，例如 ack 5s、e2e 10s。
- 不同日期或不同部署条件的数据不能混在同一条对比曲线中。

## 8. 第一阶段落地计划（本周可完成）

1. 实现 `PerformanceLoadRunner`（先只支持实验 A）。
2. 补齐最小打点改造（`clientMsgId` 可控 + ack 带 `serverMsgId`）。
3. 产出第一份 `events.csv + summary.csv`。
4. 验证统计正确后，扩展到实验 B 和 C。

