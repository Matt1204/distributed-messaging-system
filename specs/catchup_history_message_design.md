# Catchup and History Synchronization Design (Implemented)

Goal: explain the **implemented** catchup/history behavior in current code, including server constraints and client-side persistence/usage.

Source of truth in current code:
- `chat-proto/src/main/proto/chat.proto`
- `chat-server/src/main/java/com/coen6731/chat/server/MessagingServiceImpl.java`
- `chat-server/src/main/java/com/coen6731/chat/server/CosmosDBHandler.java`
- `chat-client/src/main/java/com/coen6731/chat/client/ChatClientSession.java`
- `chat-client/src/main/java/com/coen6731/chat/client/ServerResponseHandler.java`
- `chat-client/src/main/java/com/coen6731/chat/client/DatabaseManager.java`

Companion document:
- Persistence internals and send pipeline: `specs/message_persistency_design_v2.md`

---

## 1. Core terms first

### 1.1 conversation

`conversation` means one chat thread.

- Each thread has a unique `conversationId`.
- Message ordering is maintained per conversation by `sequenceId`.
- Dynamic loading and sync are all tracked per conversation.

### 1.2 sequenceId

`sequenceId` is the in-conversation order index.

- It is allocated by server per conversation and increases monotonically.
- It has **no global ordering** across conversations.

### 1.3 cursor

`cursor` means client local sync progress for one conversation.

In protocol terms:
- `ConversationCursor.clientLastReceivedSequenceId`

In SQLite terms:
- `local_conversation_cursor.latest_message_sequence_id`

### 1.4 catchup

`catchup` is a breadth-style sync API:

- It checks all conversations accessible to authenticated user.
- It compares client hint cursor vs server latest sequence.
- It returns newest missing window per conversation (bounded by limit).

It is **not** the API for deep scrolling one conversation.

---

## 2. Transport and envelope

All APIs share one bidirectional gRPC stream:

- `MessagingService.Chat(stream ClientEvent) returns (stream ServerEvent)`

This document focuses on:

1. `CatchupRequest -> CatchupResult`
2. `GetMsgHistoryRequest -> MsgHistoryResult`

Related send path is documented in `specs/message_persistency_design_v2.md`.

---

## 3. API 1: Catchup (missing-message synchronization)

### 3.1 Purpose and precondition

Use case:

- after login
- after reconnect (when authenticated state resumes)

Precondition:

- stream must be authenticated (`AUTH_NOT_AUTHENTICATED` otherwise)

### 3.2 Request schema

`CatchupRequest`

| Field | Type | Required | Notes |
|---|---|---|---|
| `cursorHints` | `repeated ConversationCursor` | No | Client local cursors |
| `perConversationLimit` | `int32` | No | Max messages per conversation |

`ConversationCursor`

| Field | Type | Required | Notes |
|---|---|---|---|
| `conversationId` | `string` | Recommended | Conversation ID |
| `clientLastReceivedSequenceId` | `int64` | Recommended | Local cursor |

Implemented normalization:

- `perConversationLimit <= 0` -> default `50`
- `perConversationLimit > 200` -> cap to `200`
- negative cursor hints -> normalized to `0`

### 3.3 Response schema

`CatchupResult`

| Field | Type | Notes |
|---|---|---|
| `conversationResults` | `repeated CatchupConversationResult` | One result per authorized conversation |
| `generatedAtMs` | `int64` | generation timestamp |

`CatchupConversationResult`

| Field | Type | Notes |
|---|---|---|
| `conversationId` | `string` | Conversation ID |
| `conversationLatestSequenceId` | `int64` | Latest durable sequence on server |
| `messages` | `repeated CanonicalMessage` | Newest-first missing window |

Implemented semantics:

1. Server returns all authorized conversations, even if client did not pass them in `cursorHints`.
2. Returned `messages` are ordered by `sequenceId DESC`.
3. Returned count per conversation is bounded by limit.
4. No `appliedClientCursor` field in proto; client derives progress from returned data.

### 3.4 Client-side handling (implemented)

1. Persist each canonical message idempotently into SQLite.
2. For each conversation result, compute `max(sequenceId)` from returned messages.
3. If max > 0, upsert local cursor for `(currentUserId, conversationId)`.
4. Trigger UI refresh.

---

## 4. API 2: GetMsgHistory (in-conversation upward paging)

### 4.1 Purpose and precondition

Use case:

- fetch older messages for one conversation during upward scroll

Precondition:

1. authenticated stream
2. `conversationId` exists
3. caller is a member of that conversation

### 4.2 Request schema

`GetMsgHistoryRequest`

| Field | Type | Required | Notes |
|---|---|---|---|
| `conversationId` | `string` | Yes | Target conversation |
| `beforeSequenceId` | `int64` | Yes | Must be `> 0` |
| `retriveMsgQuantity` | `int32` | Yes | Must be `> 0`, typo kept by proto |

Server-side constraints:

- empty `conversationId` -> `BAD_REQUEST`
- unauthorized conversation -> `CONVERSATION_INVALID`
- `beforeSequenceId <= 0` -> `BAD_REQUEST`
- `retriveMsgQuantity <= 0` -> `BAD_REQUEST`
- `retriveMsgQuantity > 200` -> cap to `200`

### 4.3 Response schema

`MsgHistoryResult`

| Field | Type | Notes |
|---|---|---|
| `conversationId` | `string` | Conversation ID |
| `messages` | `repeated CanonicalMessage` | History page |

Ordering semantics:

- server returns `sequenceId DESC`
- query is inclusive: `sequenceId <= beforeSequenceId`

### 4.4 Client-side handling (implemented)

1. Persist history messages idempotently.
2. Do **not** advance forward-sync cursor based on history page.
3. Notify UI summary callback `onHistoryResultSummary(...)` and refresh UI.

---

## 5. Error handling and boundaries

### 5.1 Catchup/history errors

Catchup/history use `ServerError` path (not send-ack path).

Common codes in these APIs:

1. `AUTH_NOT_AUTHENTICATED`
2. `BAD_REQUEST`
3. `CONVERSATION_INVALID`

### 5.2 Authentication boundary

Catchup/history are blocked until authenticated.

Authentication can be established by:

1. `LoginUser` / `RegisterUser`
2. valid header-based resume (`x-user-id` -> DB hit)

Security note:

- Header resume is implemented but weak; see `specs/login_register_design.md` risk section.

---

## 6. Ordering, idempotency, and consistency

### 6.1 Ordering

1. Catchup returns newest-first per conversation.
2. History returns newest-first page.
3. UI may re-order for display.

### 6.2 Idempotency

Client persistence is idempotent due to local unique indexes and insert-ignore/upsert patterns.

### 6.3 Consistency model in practice

1. Cosmos is durable source of persisted messages.
2. Redis sequence key is fast allocator, backfilled from Cosmos max on miss.
3. If live relay is missed, reconnect catchup repairs missing messages.

---

## 7. Implementation checklist (current code alignment)

1. `chat.proto` includes catchup/history request/response messages and fields.
2. `MessagingServiceImpl` enforces auth + input validation + membership checks.
3. `CosmosDBHandler` provides:
   - newest-after-sequence query (`DESC`)
   - history-by-before-sequence query (`DESC`)
   - max sequence query
4. `ChatClientSession` sends catchup after auth/reconnect and supports history request.
5. `ServerResponseHandler` persists catchup/history idempotently and notifies UI.
6. `DatabaseManager` stores per-conversation cursor and canonical messages.

---

## 8. Known gaps and non-goals in current implementation

1. No explicit catchup continuation token; large gaps require repeated calls.
2. Proto field typo `retriveMsgQuantity` remains for compatibility.
3. No server-side rate limiting for history/catchup requests.
4. No API version field for evolution negotiation.

