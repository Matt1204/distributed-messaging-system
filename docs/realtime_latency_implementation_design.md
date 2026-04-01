# 实时延迟实验工程实现设计（全自动、无 GUI）

## 1. 文档目的

这份文档不是实验结果文档，而是实现文档：明确我们要如何改代码，把 Ack latency 和 End-to-End delivery latency 的实验做成“纯代码可执行流程”，尽量不依赖人工配置、人工触发、人工记录。

最终目标：

- 一条命令启动指定实验（A/B/C）或整组实验。
- 自动完成测试账号准备（注册或登录）。
- 自动压测发送、自动采集原始数据、自动生成汇总。
- 全程不启动 GUI，不需要手动点按钮。

## 2. 现状与关键约束

当前代码现状（已具备的能力）：

- `ChatClientSession` 已经是可编程 API，不依赖 `ChatWindow` 才能发送/登录。
- `login/register/sendMessage` 都可直接从 Java 代码调用。
- `ServerResponseHandler` 会通过 `ClientUiListener` 回调 ack 和 inbound 事件。

当前阻塞点（必须补）：

1. 发送端无法稳定拿到“本条发送使用的 `clientMsgId`”。
- `sendMessage(...)` 内部生成 UUID，调用者不可控。

2. ack 回调信息不足，无法可靠关联 receiver 侧事件。
- `ClientUiListener.onSendAck(...)` 没有 `serverMsgId`。
- receiver 侧 `onChatMessage(..., msgId, ...)` 的 `msgId` 是 `serverMsgId`。
- 若 ack 不返回 `serverMsgId` 到 runner，就无法做 sender->receiver 关联。

## 3. 总体实现策略

采用“最小侵入 + 新增 headless runner”的方式：

- 保留现有 GUI 路径，避免影响课程 demo。
- 在 `chat-client` 中新增 `perf` 子模块（同一 Maven module 内的 Java package）。
- 只对现有核心类做最小扩展，不重构主流程。

架构原则：

- 单 JVM 运行 sender 和 receiver 会话，统一用 `System.nanoTime()`。
- 所有实验配置都来自 CLI 参数或配置文件。
- 实验流程（账号准备、warm-up、measure、落盘）由程序统一调度。

## 4. 代码改造清单（按文件）

### 4.1 `ClientUiListener`：增加详细 ack 回调

文件：`chat-client/src/main/java/com/coen6731/chat/client/ClientUiListener.java`

改动：

- 保留现有 `onSendAck(String clientMsgId, boolean success, String code, String reason)`。
- 新增默认方法（default method）`onSendAckDetailed(...)`，包含：
- `clientMsgId`
- `serverMsgId`
- `conversationId`
- `sequenceId`
- `success`
- `code`
- `reason`

原因：

- 不破坏现有 `ChatWindow` 实现（Java 接口 default method 兼容旧实现）。
- 给 headless runner 足够字段做 sender/receiver 关联。

### 4.2 `ServerResponseHandler`：在 ack 处理处发 detailed 回调

文件：`chat-client/src/main/java/com/coen6731/chat/client/ServerResponseHandler.java`

改动：

- 在 `handleSendMessageAck(SendMessageAck ack)` 里，沿用原有 `notifySendAck(...)`。
- 追加 `notifySendAckDetailed(...)`，传 `ack.getServerMsgId()`、`ack.getConversationId()`、`ack.getSequenceId()`。

原因：

- 保持 UI 行为不变。
- runner 能在 ack 到达时拿到 server 侧主键。

### 4.3 `ChatClientSession`：暴露可控 `clientMsgId` 发送接口

文件：`chat-client/src/main/java/com/coen6731/chat/client/ChatClientSession.java`

改动建议：

- 保留现有：
- `public void sendMessage(String toEmail, String text, String conversationId, String peerUserId)`

- 新增重载（推荐）：
- `public String sendMessageAndReturnClientMsgId(...)`
- 或 `public void sendMessageWithClientMsgId(..., String clientMsgId)`

推荐优先 `sendMessageAndReturnClientMsgId`，因为：

- 对外语义清晰。
- runner 在调用后立即得到 `clientMsgId`，可记录 `t_send_start`。
- 不强制调用方自己造 UUID，减少误用。

附加建议：

- 新增 `awaitConnected(timeout)` 辅助方法（可选），便于 runner 更稳定地启动。

### 4.4 `ChatWindow`：无需功能改造

文件：`chat-client/src/main/java/com/coen6731/chat/client/ChatWindow.java`

改动：

- 不改核心逻辑。
- 若需要，实现/忽略新的 `onSendAckDetailed(...)` 默认回调即可。

## 5. 新增组件设计（headless perf package）

建议新增包：

- `chat-client/src/main/java/com/coen6731/chat/client/perf/`

### 5.1 `PerformanceLoadRunner`（main 入口）

职责：

- 解析 CLI 参数。
- 加载实验配置。
- 调用执行器跑单实验或全矩阵。
- 最终输出 summary。

### 5.2 `PerfConfig`（配置模型）

字段建议：

- `target`
- `isProd`
- `scenario` (`baseline` | `rate-ramp` | `concurrency-ramp` | `all`)
- `pairs`
- `ratePerSender`
- `warmupSec`
- `measureSec`
- `payloadBytes`
- `ackTimeoutMs`
- `e2eTimeoutMs`
- `outputDir`
- `runId`
- `namespace`
- `password`

说明：

- `scenario=all` 时由代码展开为 A/B/C 的参数矩阵。

### 5.3 `AccountProvisioner`（账号自动准备）

职责：

- 为每个 pair 自动生成 sender/receiver 账号。
- 自动执行“先注册，若已存在则登录”。

行为细节：

1. 账号命名模板：
- `perf_{namespace}_p{pairId}_sender@example.test`
- `perf_{namespace}_p{pairId}_receiver@example.test`

2. 认证策略：
- 先 `register(email,password)`。
- 若失败且错误为 `AUTH_EMAIL_ALREADY_EXISTS`，则 `login`。
- 任一步失败都记录并中止当前 run。

### 5.4 `SessionEndpoint`（单客户端会话包装）

职责：

- 持有一个 `ChatClientSession`。
- 绑定 listener 到 `MetricsCollector`。
- 对外暴露 `sendAtFixedRate(...)` 和 `close()`。

### 5.5 `MetricsCollector`（事件采集与关联）

核心数据结构：

- `Map<String, Long> sendStartNsByClientMsgId`
- `Map<String, AckRecord> ackByClientMsgId`
- `Map<String, Long> inboundRecvNsByServerMsgId`
- `Map<String, String> serverMsgIdByClientMsgId`

关联流程：

1. sender 发消息后立刻记录 `send_start_ns` + `client_msg_id`。
2. ack detailed 到达，记录 `client_msg_id -> server_msg_id` 与 `ack_recv_ns`。
3. receiver inbound 到达，记录 `server_msg_id -> inbound_recv_ns`。
4. flush 时 join 三类数据，产出逐消息事件行。

### 5.6 `ScenarioExecutor`（实验编排器）

职责：

- 执行单个实验点：setup -> warm-up -> measure -> drain -> flush。
- 若 `scenario=all`，按矩阵顺序执行全部实验点。

建议状态机：

- `PREPARE`
- `WARMUP`
- `MEASURE`
- `DRAIN`
- `FINALIZE`

### 5.7 `CsvReportWriter` + `SummaryAggregator`

职责：

- 写 `results/raw/<run_id>_events.csv`
- 写 `results/summary/latency_summary.csv`
- 计算 P50/P95/P99、失败率、超时率

## 6. 自动化实验流程（程序内部）

每个实验点统一流程：

1. 创建 run 上下文（runId、目录、metadata）。
2. 构建 N 对 sender/receiver session。
3. `AccountProvisioner` 自动完成注册/登录。
4. warm-up 阶段按目标速率发送（不计入统计）。
5. 清空 warm-up 期间的临时统计缓存。
6. measured 阶段按目标速率发送并记录。
7. 停止发送，等待 `drainWindowSec` 补齐在途消息。
8. 根据 timeout 规则标记未完成事件。
9. 输出 raw + summary + meta。
10. 关闭所有 session。

## 7. 实验参数矩阵（代码内置 preset）

### 7.1 baseline

- `pairs=1`
- `ratePerSender=1`
- `warmupSec=60`
- `measureSec=240`

### 7.2 rate-ramp

固定：

- `pairs=1`
- `warmupSec=60`
- `measureSec=180`

档位：

- `ratePerSender in [5,10,20,40]`

### 7.3 concurrency-ramp

固定：

- `ratePerSender=5`
- `warmupSec=60`
- `measureSec=300`

档位：

- `pairs in [1,5,10,20]`

### 7.4 all

- 顺序执行：baseline -> rate-ramp -> concurrency-ramp
- 每个实验点自动生成唯一子 run id

## 8. 结果文件规范

目录结构建议：

- `results/raw/`
- `results/summary/`
- `results/meta/`

### 8.1 原始事件 CSV（逐消息）

列：

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
- `ack_latency_ms`
- `e2e_latency_ms`
- `is_ack_timeout`
- `is_e2e_timeout`

### 8.2 汇总 CSV（逐实验点）

列：

- `run_id`
- `scenario_id`
- `pairs`
- `rate_per_sender`
- `attempted_messages`
- `acked_messages`
- `received_messages`
- `ack_fail_count`
- `ack_timeout_count`
- `e2e_timeout_count`
- `ack_p50_ms`
- `ack_p95_ms`
- `ack_p99_ms`
- `e2e_p50_ms`
- `e2e_p95_ms`
- `e2e_p99_ms`

### 8.3 元数据 JSON

建议字段：

- `run_id`
- `git_commit`
- `target`
- `is_prod`
- `start_time_utc`
- `end_time_utc`
- `config_snapshot`

## 9. 运行方式（尽量一键化）

## 9.1 编译

```bash
mvn -q -DskipTests package
```

## 9.2 跑单个实验（baseline）

```bash
mvn -q -pl chat-client exec:java \
  -Dexec.mainClass=com.coen6731.chat.client.perf.PerformanceLoadRunner \
  -Dexec.args="--scenario baseline --target <host:port> --isProd <true|false> --namespace exp1 --outputDir results"
```

## 9.3 跑整组实验（all）

```bash
mvn -q -pl chat-client exec:java \
  -Dexec.mainClass=com.coen6731.chat.client.perf.PerformanceLoadRunner \
  -Dexec.args="--scenario all --target <host:port> --isProd <true|false> --namespace exp_full --outputDir results"
```

设计要求：

- 以上命令触发后，不需要人工再做账号创建、发送触发、结果整理。
- 程序跑完后直接得到 raw/summary/meta 文件。

## 10. 实现顺序（建议按这个节奏）

### Phase 1：打点通道打通

- `ClientUiListener` 新增 detailed ack 回调。
- `ServerResponseHandler` 发 detailed ack。
- `ChatClientSession` 暴露可追踪 clientMsgId 的发送接口。

验收：

- 在不启动 GUI 的情况下，单条消息能得到完整链路时间戳。

### Phase 2：最小可用 runner（只跑 baseline）

- 实现 `PerformanceLoadRunner + PerfConfig + MetricsCollector + CsvWriter`。
- 自动创建 1 对账号并跑 baseline。

验收：

- 输出首个 `events.csv` + `summary.csv`。

### Phase 3：矩阵化与鲁棒性

- 增加 `ScenarioExecutor` 支持 rate-ramp / concurrency-ramp / all。
- 增加 timeout、drain、异常恢复与 meta 输出。

验收：

- 一条命令跑完整矩阵，并输出多份汇总。

## 11. 风险与应对

风险 1：不同会话启动顺序导致初期丢样本。

- 应对：加入 `PREPARE` 和 warm-up，measure 前清缓存。

风险 2：账号已存在导致 register 失败。

- 应对：自动 fallback 到 login，不需要人工清理账号。

风险 3：部分消息只收到 ack 未收到 inbound。

- 应对：定义 e2e timeout 并在 summary 中显式统计。

风险 4：实验中断后文件不完整。

- 应对：周期性 flush + 最终 shutdown hook flush。

## 12. 完成标准（Definition of Done）

当以下条件都满足，视为“实验工程化完成”：

- 可以在不启动 GUI 的情况下运行 baseline/rate-ramp/concurrency-ramp。
- 账号准备、实验触发、数据记录全自动。
- 每个实验点均输出 raw + summary + meta。
- summary 中能稳定给出 Ack/E2E 的 P50/P95/P99。

## 13. 分支策略（experiment 与 master 并行）

根据当前项目协作约束：

- `experiment` 分支：承载实验代码、实验脚本、实验数据与实验文档。
- `master` 分支：承载 presentation demo 代码与稳定展示路径。
- `experiment` 不合并回 `master`。

工程执行约束：

- 核心逻辑代码（聊天主流程）在两个分支保持语义一致，避免实验代码污染 demo。
- 实验相关入口与工具优先放在 `chat-client/.../perf/`，与 GUI 主路径解耦。
- 实验数据文件只落在 `experiment` 分支约定目录（如 `results/`），不进入 `master` 演示路径。
- 若实验中发现核心逻辑缺陷，先在 `experiment` 修复并验证，再按需人工最小化同步到 `master`（选择性拣选，不整分支合并）。
