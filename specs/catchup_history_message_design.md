# Catchup and History Synchronization Design (Implemented)

Goal: document the implemented catchup/history synchronization behavior for offline recovery and upward history paging.

Source of truth in current code:
- `chat-proto/src/main/proto/chat.proto`
- `chat-server/src/main/java/com/coen6731/chat/server/MessagingServiceImpl.java`
- `chat-server/src/main/java/com/coen6731/chat/server/CosmosDBHandler.java`

Companion document:
- Persistence internals, sequence allocation, and data-store responsibilities are documented in `docs/message_persistency_design_v2.md`.

---

## 1. Core terms first

If you are new to this project, align on these four terms first.

### 1.1 conversation

`conversation` means one chat thread / session.

- DM with user A is one `conversationId`.
- DM with user B is another `conversationId`.
- Each conversation has its own message ordering (`sequenceId`).

For dynamic loading, all scrolling/paging/catchup states must be tracked per conversation.

### 1.2 sequenceId

`sequenceId` is the in-conversation message index; it only increases within the same conversation.

- It starts from 1 and is monotonic within one conversation.
- Do not compare `sequenceId` values across different conversations.
- Example: if A sends 3 messages first, sequence is 1,2,3; then B sends 2, sequence becomes 4,5.
- Higher `sequenceId` is newer in that conversation.

Example:
- in conversation Y, latest message may be `sequenceId=9`
- `120` and `9` have no cross-conversation ordering meaning.

### 1.3 cursor

`cursor` marks up to which message a conversation is locally synchronized.

In this project:
- Client cursor hint field: `clientLastReceivedSequenceId`

You can interpret it as: "which `sequenceId` is already persisted locally."

### 1.4 catchup

`catchup` fills missed persisted messages, mainly used:
- after login
- after reconnect

It is not the API for "older history of one conversation." It is a breadth-style sync across all conversations accessible to current user.

---

## 2. Transport and envelope

All APIs are multiplexed on one bidirectional gRPC stream:

- `MessagingService.Chat(stream ClientEvent) returns (stream ServerEvent)`

Client sends `ClientEvent.oneof payload`; server returns `ServerEvent.oneof payload`.

This document focuses on two synchronization capabilities:
- `CatchupRequest -> CatchupResult`
- `GetMsgHistoryRequest -> MsgHistoryResult`

Related but out of scope here:
- `OutboundMessage -> SendMessageAck` is covered in `docs/message_persistency_design_v2.md`.

---

## 3. API 1: Catchup (missing-message synchronization)

### 3.1 What this API is for

Client purpose: fetch messages that were persisted server-side but not received locally.
Precondition: stream must already be authenticated; otherwise server returns `ServerError` with `AUTH_NOT_AUTHENTICATED`.

Typical UI timing:
- after successful login
- after successful gRPC reconnect

### 3.2 Request schema

`CatchupRequest`

| Field | Type | Required | Notes |
|---|---|---|---|
| `cursorHints` | `repeated ConversationCursor` | No | Client hint map of local cursor per conversation |
| `perConversationLimit` | `int32` | No | Max number of messages returned per conversation |

`ConversationCursor`

| Field | Type | Required | Notes |
|---|---|---|---|
| `conversationId` | `string` | Recommended | Conversation ID |
| `clientLastReceivedSequenceId` | `int64` | Recommended | Local synced sequence for that conversation |

Server limits (implementation details):
- `perConversationLimit <= 0` -> defaults to `50`
- `perConversationLimit > 200` -> capped to `200`
- negative cursor hints are normalized to `0`

### 3.3 Response schema

`CatchupResult`

| Field | Type | Notes |
|---|---|---|
| `conversationResults` | `repeated CatchupConversationResult` | One result per authorized conversation |
| `generatedAtMs` | `int64` | Result generation timestamp |

`CatchupConversationResult`

| Field | Type | Notes |
|---|---|---|
| `conversationId` | `string` | Conversation ID |
| `conversationLatestSequenceId` | `int64` | Latest persisted sequence from server perspective |
| `messages` | `repeated CanonicalMessage` | Newest-first (`DESC`) missing window |

Key semantics (important):
- Server returns all authorized conversations, not only conversations provided in `cursorHints`.
- Server may not return all missing records in one call; each conversation is bounded by `perConversationLimit`.
- Catchup `messages` are **sequence descending (new -> old)**.
- Strategy is breadth-first across conversations, then bounded newest window per conversation.
- If one conversation has more missing messages than limit, only the newest window is returned.
- If `messages` are returned for a conversation, the newest returned record should match current latest for that returned window.

### 3.4 Example: catchup after login

Request (conceptual example):

```json
{
  "catchupRequest": {
    "perConversationLimit": 5,
    "cursorHints": [
      { "conversationId": "conv_alice", "clientLastReceivedSequenceId": 110 }
    ]
  }
}
```

Response (conceptual, simplified):
```json
{
  "catchupResult": {
    "generatedAtMs": 1740672000000,
    "conversationResults": [
      {
        "conversationId": "conv_alice",
        "conversationLatestSequenceId": 126,
        "messages": [
          { "sequenceId": 126, "text": "..." },
          { "sequenceId": 125, "text": "..." },
          { "sequenceId": 124, "text": "..." },
          { "sequenceId": 123, "text": "..." },
          { "sequenceId": 122, "text": "..." }
        ]
      },
      {
        "conversationId": "conv_bob",
        "conversationLatestSequenceId": 80,
        "messages": [
          { "sequenceId": 80, "text": "..." },
          { "sequenceId": 79, "text": "..." }
        ]
      }
    ]
  }
}
```
- For `conv_alice`, there are 16 missing records in total, but with `perConversationLimit = 5`, server returns only the 5 newest.
- `conv_bob` was not in `cursorHints`, but server still includes it because user membership authorizes it.

Client actions:
- Persist `messages` idempotently into local DB (dedupe by `serverMsgId` + `conversationId`).
- Advance local cursor using max sequence returned for that conversation.

---

## 4. API 2: GetMsgHistory (in-conversation upward paging)

### 4.1 What this API is for

Client purpose: fetch older messages inside one conversation.
Precondition: stream must already be authenticated and user must be a conversation member.

It is single-conversation pagination, not global catchup.

### 4.2 Request schema

`GetMsgHistoryRequest`

| Field | Type | Required | Notes |
|---|---|---|---|
| `conversationId` | `string` | Yes | Conversation to page |
| `beforeSequenceId` | `int64` | Yes | Inclusive upper bound |
| `retriveMsgQuantity` | `int32` | Yes | How many older rows to return |

Server constraints:
- empty `conversationId` -> error
- non-member conversation access -> error
- `beforeSequenceId <= 0` -> error
- `retriveMsgQuantity <= 0` -> error
- `retriveMsgQuantity > 200` -> capped to `200`

### 4.3 Response schema

`MsgHistoryResult`

| Field | Type | Notes |
|---|---|---|
| `conversationId` | `string` | Conversation ID |
| `messages` | `repeated CanonicalMessage` | History page |

Ordering semantics:
- Server returns **sequence descending (new -> old)**.
- Query is inclusive at `beforeSequenceId`, then older direction for `retriveMsgQuantity`.

### 4.4 Example: `beforeSequenceId=50`, fetch 5 rows

Request:

```json
{
  "getMsgHistoryRequest": {
    "conversationId": "conv_alice",
    "beforeSequenceId": 50,
    "retriveMsgQuantity": 5
  }
}
```

Response (simplified):

```json
{
  "msgHistoryResult": {
    "conversationId": "conv_alice",
    "messages": [
      { "sequenceId": 50, "text": "..." },
      { "sequenceId": 49, "text": "..." },
      { "sequenceId": 48, "text": "..." },
      { "sequenceId": 47, "text": "..." },
      { "sequenceId": 46, "text": "..." }
    ]
  }
}
```

Client actions:
- Persist in returned order (already new -> old).
- For next request, use `beforeSequenceId = oldestReturnedSequenceId - 1`.
- If empty array is returned, mark local state as history exhausted for this pagination path.

---

## 5. Boundary with message persistence subsystem

This document intentionally excludes full send-path persistence details to avoid overlap.

For those details, refer to:
1. `docs/message_persistency_design_v2.md` section `1.2` (send / persist / delivery flow)
2. `docs/message_persistency_design_v2.md` section `2.2` (Cosmos/Redis data model)
3. `docs/message_persistency_design_v2.md` section `4.2.1` and `4.2.3` (sequence allocation and delivery path)

What this document owns:
1. when and why catchup/history are invoked
2. request/response semantics for synchronization
3. client-side cursor and paging behavior for synchronization UX

## 6. Shared payload note (`CanonicalMessage`)

`CatchupResult.messages[]` and `MsgHistoryResult.messages[]` both use `CanonicalMessage`.

For complete field list and persistence interpretation, use:
1. `chat-proto/src/main/proto/chat.proto` (wire contract)
2. `docs/message_persistency_design_v2.md` (storage interpretation and idempotent write expectations)
