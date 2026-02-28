package com.coen6731.chat.server;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CosmosDBHandler {
  private static final Logger logger = LoggerFactory.getLogger(CosmosDBHandler.class);

  @Value("${azure.cosmos.endpoint}")
  private String endpoint;

  @Value("${azure.cosmos.key}")
  private String key;

  @Value("${azure.cosmos.database}")
  private String databaseName;

  @Value("${container.app.replica.name}")
  private String replicaName;

  private CosmosClient client;
  private CosmosDatabase database;
  private CosmosContainer usersContainer;
  private CosmosContainer messagesContainer;
  private CosmosContainer conversationsContainer;

  /**
   * Responsibility: initialize Cosmos client and container handles used by the
   * service.
   * Input: values injected from Spring configuration.
   * Output: initialized container references for users/messages/conversations.
   */
  @PostConstruct
  public void init() {
    try {
      if (endpoint == null || endpoint.isEmpty() || key == null || key.isEmpty()) {
        logger.error("Cosmos DB credentials not found in configuration");
        return;
      }

      logger.info("Initializing Cosmos DB connection...");
      logger.info("Connection Endpoint: {}", endpoint);
      logger.info("Database Name: {}", databaseName);
      logger.info("Container App Replica Name: {}", replicaName);

      client = new CosmosClientBuilder().endpoint(endpoint).key(key).buildClient();

      database = client.getDatabase(databaseName);
      usersContainer = database.getContainer("users");
      messagesContainer = database.getContainer("messages");
      conversationsContainer = database.getContainer("conversations");

      logger.info("Cosmos DB client initialized successfully");
    } catch (Exception e) {
      logger.error("Failed to initialize Cosmos DB client", e);
    }
  }

  /**
   * Responsibility: create and persist one user record.
   * Input: user email and hashed password.
   * Output: created user payload when write succeeds, empty when write fails.
   */
  public Optional<UserRecord> createUser(String email, String passwordHash) {
    if (usersContainer == null) {
      logger.error("Users container not initialized");
      return Optional.empty();
    }

    String normalizedEmail = normalizeEmail(email);
    String userId = "user-" + UUID.randomUUID();
    long now = System.currentTimeMillis();

    Map<String, Object> userItem = new HashMap<>();
    userItem.put("id", userId);
    userItem.put("userId", userId);
    userItem.put("email", normalizedEmail);
    userItem.put("passwordHash", passwordHash);
    userItem.put("createdAt", now);
    userItem.put("updatedAt", now);

    try {
      usersContainer.upsertItem(userItem, new PartitionKey(userId), null);
      return Optional.of(new UserRecord(userId, normalizedEmail, passwordHash));
    } catch (Exception e) {
      logger.error("Failed to create user in Cosmos DB for email={}", normalizedEmail, e);
      return Optional.empty();
    }
  }

  /**
   * Responsibility: find a user by normalized email.
   * Input: email address from request.
   * Output: matching user identity and password hash, or empty when absent.
   */
  public Optional<UserRecord> findUserByEmail(String email) {
    if (usersContainer == null) {
      logger.error("Users container not initialized");
      return Optional.empty();
    }

    String normalizedEmail = normalizeEmail(email);
    String sqlText = "SELECT TOP 1 c.userId, c.email, c.passwordHash FROM c WHERE c.email = @email";
    SqlQuerySpec querySpec = new SqlQuerySpec(sqlText)
        .setParameters(List.of(new SqlParameter("@email", normalizedEmail)));

    try {
      CosmosPagedIterable<Map> results = usersContainer.queryItems(querySpec, new CosmosQueryRequestOptions(),
          Map.class);
      for (Map record : results) {
        Object userIdObj = record.get("userId");
        Object emailObj = record.get("email");
        Object hashObj = record.get("passwordHash");
        if (userIdObj instanceof String && emailObj instanceof String) {
          return Optional.of(
              new UserRecord(
                  (String) userIdObj,
                  (String) emailObj,
                  hashObj instanceof String ? (String) hashObj : null));
        }
      }
      return Optional.empty();
    } catch (Exception e) {
      logger.error("Failed to query user by email={}", normalizedEmail, e);
      return Optional.empty();
    }
  }

  /**
   * Responsibility: point-read one user by userId.
   * Input: canonical userId.
   * Output: user data if exists, empty otherwise.
   */
  public Optional<UserRecord> findUserByUserId(String userId) {
    if (usersContainer == null) {
      logger.error("Users container not initialized");
      return Optional.empty();
    }

    try {
      CosmosItemResponse<Map> response = usersContainer.readItem(userId, new PartitionKey(userId), Map.class);
      @SuppressWarnings("unchecked")
      Map<String, Object> item = (Map<String, Object>) response.getItem();
      if (item == null) {
        return Optional.empty();
      }
      return Optional.of(
          new UserRecord(asString(item.get("userId")), asString(item.get("email")),
              asString(item.get("passwordHash"))));
    } catch (Exception e) {
      logger.debug("User {} not found in Cosmos DB", userId);
      return Optional.empty();
    }
  }

  /**
   * Responsibility: point-read conversation by conversationId.
   * Input: conversationId generated by server.
   * Output: conversation metadata when found.
   */
  public Optional<ConversationRecord> findConversationById(String conversationId) {
    if (conversationsContainer == null) {
      logger.error("Conversations container not initialized");
      return Optional.empty();
    }

    String sql = "SELECT TOP 1 c.id, c.memberUserIds, c.createdAtMs, c.updatedAtMs, c.lastMessageAtMs "
        + "FROM c WHERE c.id = @id";
    SqlQuerySpec query = new SqlQuerySpec(sql).setParameters(List.of(new SqlParameter("@id", conversationId)));

    try {
      CosmosPagedIterable<Map> results = conversationsContainer.queryItems(query, new CosmosQueryRequestOptions(),
          Map.class);
      for (Map row : results) {
        return Optional.of(mapConversationRecord(row));
      }
      return Optional.empty();
    } catch (Exception e) {
      logger.error("Failed to query conversation by id={}", conversationId, e);
      return Optional.empty();
    }
  }

  /**
   * Responsibility: find or create one conversation and enforce participant
   * membership.
   * Input: optional conversationId, sender/recipient user ids, and current
   * timestamp.
   * Output: existing or newly created conversation record.
   */
  public Optional<ConversationRecord> createConversationIfAbsent(
      String requestConversationId,
      String senderUserId,
      String recipientUserId,
      long nowMs) {
    if (conversationsContainer == null) {
      logger.error("Conversations container not initialized");
      return Optional.empty();
    }
    String normalizedConversationId = requestConversationId == null ? "" : requestConversationId.trim();
    String conversationId = normalizedConversationId.isBlank() ? UUID.randomUUID().toString()
        : normalizedConversationId;

    Optional<ConversationRecord> existingConversation = findConversationById(conversationId);
    if (existingConversation.isPresent()) {
      ConversationRecord current = existingConversation.get();
      if (current.memberUserIds().contains(senderUserId) && current.memberUserIds().contains(recipientUserId)) {
        return existingConversation;
      }
      logger.warn("Conversation {} exists but does not contain both users: sender={}, recipient={}",
          conversationId, senderUserId, recipientUserId);
      return Optional.empty();
    }

    Map<String, Object> item = new HashMap<>();
    item.put("id", conversationId);
    item.put("conversationId", conversationId);
    item.put("memberUserIds", List.of(senderUserId, recipientUserId));
    item.put("createdAtMs", nowMs);
    item.put("updatedAtMs", nowMs);
    item.put("lastMessageAtMs", nowMs);

    try {
      conversationsContainer.createItem(item, new PartitionKey(conversationId), null);
      return Optional.of(mapConversationRecord(item));
    } catch (CosmosException e) {
      if (e.getStatusCode() == 409) {
        Optional<ConversationRecord> conflicted = findConversationById(conversationId);
        if (conflicted.isPresent()
            && conflicted.get().memberUserIds().contains(senderUserId)
            && conflicted.get().memberUserIds().contains(recipientUserId)) {
          return conflicted;
        }
        return Optional.empty();
      }
      logger.error("Failed to create conversation conversationId={}", conversationId, e);
      return Optional.empty();
    }
  }

  /**
   * Responsibility: list all conversations where a user is a participant.
   * Input: authenticated userId.
   * Output: conversation records the user is authorized to access.
   */
  public List<ConversationRecord> findConversationsByMember(String userId) {
    if (conversationsContainer == null) {
      logger.error("Conversations container not initialized");
      return List.of();
    }

    String sql = "SELECT c.id, c.memberUserIds, c.createdAtMs, c.updatedAtMs, c.lastMessageAtMs FROM c "
        + "WHERE ARRAY_CONTAINS(c.memberUserIds, @userId)";
    SqlQuerySpec query = new SqlQuerySpec(sql).setParameters(List.of(new SqlParameter("@userId", userId)));

    List<ConversationRecord> rows = new ArrayList<>();
    try {
      CosmosPagedIterable<Map> results = conversationsContainer.queryItems(query, new CosmosQueryRequestOptions(),
          Map.class);
      for (Map row : results) {
        rows.add(mapConversationRecord(row));
      }
      return rows;
    } catch (Exception e) {
      logger.error("Failed to query conversations for userId={}", userId, e);
      return List.of();
    }
  }

  /**
   * Responsibility: persist one canonical message record with idempotent conflict
   * detection.
   * Input: message fields already validated by service layer.
   * Output: persistence status plus the canonical stored record.
   */
  public PersistResult createMessageIfAbsent(MessageRecord record) {
    if (messagesContainer == null) {
      logger.error("Messages container not initialized");
      return new PersistResult(PersistStatus.FAILED, null, "messages container not initialized");
    }

    Map<String, Object> item = new HashMap<>();
    item.put("id", record.serverMsgId());
    item.put("serverMsgId", record.serverMsgId());
    item.put("clientMsgId", record.clientMsgId());
    item.put("conversationId", record.conversationId());
    item.put("sequenceId", record.sequenceId());
    item.put("senderUserId", record.senderUserId());
    item.put("recipientUserId", record.recipientUserId());
    item.put("text", record.text());
    item.put("sentAtMs", record.sentAtMs());
    item.put("status", record.status());
    item.put("createdAtMs", record.createdAtMs());
    item.put("updatedAtMs", record.updatedAtMs());

    try {
      messagesContainer.createItem(item, new PartitionKey(record.conversationId()), null);
      return new PersistResult(PersistStatus.CREATED, mapMessageRecord(item), null);
    } catch (CosmosException e) {
      if (e.getStatusCode() == 409) {
        return new PersistResult(PersistStatus.ALREADY_EXISTS, null, null);
      }

      logger.error("Failed to persist message serverMsgId={}", record.serverMsgId(), e);
      return new PersistResult(PersistStatus.FAILED, null, e.getMessage());
    }
  }

  /**
   * Responsibility: fetch canonical message by serverMsgId.
   * Input: server message id.
   * Output: message record if found.
   */
  public Optional<MessageRecord> findMessageByServerMsgId(String serverMsgId) {
    if (messagesContainer == null) {
      logger.error("Messages container not initialized");
      return Optional.empty();
    }

    String sql = "SELECT TOP 1 c.serverMsgId, c.clientMsgId, c.conversationId, c.sequenceId, c.senderUserId, c.recipientUserId, "
        + "c.text, c.sentAtMs, c.status, c.createdAtMs, c.updatedAtMs FROM c WHERE c.id = @id";
    SqlQuerySpec query = new SqlQuerySpec(sql).setParameters(List.of(new SqlParameter("@id", serverMsgId)));

    try {
      CosmosPagedIterable<Map> results = messagesContainer.queryItems(query, new CosmosQueryRequestOptions(),
          Map.class);
      for (Map row : results) {
        return Optional.of(mapMessageRecord(row));
      }
      return Optional.empty();
    } catch (Exception e) {
      logger.error("Failed to query message by serverMsgId={}", serverMsgId, e);
      return Optional.empty();
    }
  }

  /**
   * Responsibility: read latest sequence id for a conversation from durable
   * storage.
   * Input: conversation id.
   * Output: max persisted sequence id, or 0 when none exists.
   */
  public long findMaxSequenceId(String conversationId) {
    if (messagesContainer == null) {
      logger.error("Messages container not initialized");
      return 0L;
    }

    String sql = "SELECT VALUE MAX(c.sequenceId) FROM c WHERE c.conversationId = @conversationId";
    SqlQuerySpec query = new SqlQuerySpec(sql)
        .setParameters(List.of(new SqlParameter("@conversationId", conversationId)));

    try {
      CosmosPagedIterable<Long> results = messagesContainer.queryItems(query, new CosmosQueryRequestOptions(),
          Long.class);
      for (Long seq : results) {
        // return 0 if no message with given conversationId found (new conversation)
        return seq == null ? 0L : seq;
      }
    } catch (Exception e) {
      logger.error("Failed to query max sequence id for conversationId={}", conversationId, e);
    }
    return 0L;
  }

  /**
   * Responsibility: fetch missing messages after a cursor in ascending sequence
   * order.
   * Input: conversation id, cursor (exclusive), and max rows.
   * Output: up to limit messages sorted by sequence id asc.
   */
  public List<MessageRecord> listMessagesAfterSequence(String conversationId, long afterSequenceId, int limit) {
    if (messagesContainer == null) {
      logger.error("Messages container not initialized");
      return List.of();
    }

    String sql = "SELECT c.serverMsgId, c.clientMsgId, c.conversationId, c.sequenceId, c.senderUserId, c.recipientUserId, "
        + "c.text, c.sentAtMs, c.status, c.createdAtMs, c.updatedAtMs FROM c "
        + "WHERE c.conversationId = @conversationId AND c.sequenceId > @afterSequenceId "
        + "ORDER BY c.sequenceId ASC OFFSET 0 LIMIT @limit";
    SqlQuerySpec query = new SqlQuerySpec(sql)
        .setParameters(
            List.of(
                new SqlParameter("@conversationId", conversationId),
                new SqlParameter("@afterSequenceId", afterSequenceId),
                new SqlParameter("@limit", limit)));

    List<MessageRecord> rows = new ArrayList<>();
    try {
      CosmosPagedIterable<Map> results = messagesContainer.queryItems(query, new CosmosQueryRequestOptions(),
          Map.class);
      for (Map row : results) {
        rows.add(mapMessageRecord(row));
      }
    } catch (Exception e) {
      logger.error("Failed to list messages after sequence conversationId={} cursor={}", conversationId,
          afterSequenceId, e);
    }
    return rows;
  }

  /**
   * Responsibility: fetch newest missing messages after a client cursor in descending sequence order.
   * Input: conversation id, cursor (exclusive), and max rows.
   * Output: up to limit messages sorted by sequence id desc (newest first).
   */
  public List<MessageRecord> listNewestMessagesAfterSequence(
      String conversationId,
      long afterSequenceId,
      int limit) {
    if (messagesContainer == null) {
      logger.error("Messages container not initialized");
      return List.of();
    }

    String sql =
        "SELECT c.serverMsgId, c.clientMsgId, c.conversationId, c.sequenceId, c.senderUserId, c.recipientUserId, "
            + "c.text, c.sentAtMs, c.status, c.createdAtMs, c.updatedAtMs FROM c "
            + "WHERE c.conversationId = @conversationId AND c.sequenceId > @afterSequenceId "
            + "ORDER BY c.sequenceId DESC OFFSET 0 LIMIT @limit";
    SqlQuerySpec query =
        new SqlQuerySpec(sql)
            .setParameters(
                List.of(
                    new SqlParameter("@conversationId", conversationId),
                    new SqlParameter("@afterSequenceId", afterSequenceId),
                    new SqlParameter("@limit", limit)));

    List<MessageRecord> rows = new ArrayList<>();
    try {
      CosmosPagedIterable<Map> results =
          messagesContainer.queryItems(query, new CosmosQueryRequestOptions(), Map.class);
      for (Map row : results) {
        rows.add(mapMessageRecord(row));
      }
    } catch (Exception e) {
      logger.error(
          "Failed to list newest messages after sequence conversationId={} cursor={}",
          conversationId,
          afterSequenceId,
          e);
    }
    return rows;
  }

  /**
   * Responsibility: count how many messages exist after a given conversation cursor.
   * Input: conversation id and cursor (exclusive).
   * Output: number of missing messages after the cursor.
   */
  public long countMessagesAfterSequence(String conversationId, long afterSequenceId) {
    if (messagesContainer == null) {
      logger.error("Messages container not initialized");
      return 0L;
    }

    String sql =
        "SELECT VALUE COUNT(1) FROM c WHERE c.conversationId = @conversationId AND c.sequenceId > @afterSequenceId";
    SqlQuerySpec query =
        new SqlQuerySpec(sql)
            .setParameters(
                List.of(
                    new SqlParameter("@conversationId", conversationId),
                    new SqlParameter("@afterSequenceId", afterSequenceId)));

    try {
      CosmosPagedIterable<Long> results =
          messagesContainer.queryItems(query, new CosmosQueryRequestOptions(), Long.class);
      for (Long count : results) {
        return count == null ? 0L : count;
      }
    } catch (Exception e) {
      logger.error(
          "Failed to count messages after sequence conversationId={} cursor={}",
          conversationId,
          afterSequenceId,
          e);
    }
    return 0L;
  }

  /**
   * Responsibility: fetch one descending history page starting from a given
   * sequence id (inclusive).
   * Input: conversation id, start sequence id (inclusive), and max rows.
   * Output: up to quantity messages sorted by sequence id desc.
   */
  public List<MessageRecord> listMessagesFromSequenceDescending(
      String conversationId,
      long fromSequenceId,
      int quantity) {
    if (messagesContainer == null) {
      logger.error("Messages container not initialized");
      return List.of();
    }

    String sql = "SELECT c.serverMsgId, c.clientMsgId, c.conversationId, c.sequenceId, c.senderUserId, c.recipientUserId, "
        + "c.text, c.sentAtMs, c.status, c.createdAtMs, c.updatedAtMs FROM c "
        + "WHERE c.conversationId = @conversationId AND c.sequenceId <= @fromSequenceId "
        + "ORDER BY c.sequenceId DESC OFFSET 0 LIMIT @quantity";
    SqlQuerySpec query = new SqlQuerySpec(sql)
        .setParameters(
            List.of(
                new SqlParameter("@conversationId", conversationId),
                new SqlParameter("@fromSequenceId", fromSequenceId),
                new SqlParameter("@quantity", quantity)));

    List<MessageRecord> rows = new ArrayList<>();
    try {
      CosmosPagedIterable<Map> results = messagesContainer.queryItems(query, new CosmosQueryRequestOptions(),
          Map.class);
      for (Map row : results) {
        rows.add(mapMessageRecord(row));
      }
    } catch (Exception e) {
      logger.error(
          "Failed to list history conversationId={} fromSequenceId={} quantity={}",
          conversationId,
          fromSequenceId,
          quantity,
          e);
    }
    return rows;
  }

  /**
   * Responsibility: update conversation activity timestamps after message
   * persistence.
   * Input: conversation id and persisted message timestamp.
   * Output: true when update succeeds; false otherwise.
   */

  // TODO: is lastMessageAtMs in "conversation" needed?
  public boolean touchConversation(String conversationId, long lastMessageAtMs) {
    Optional<ConversationRecord> existing = findConversationById(conversationId);
    if (existing.isEmpty()) {
      return false;
    }

    ConversationRecord current = existing.get();
    Map<String, Object> item = new HashMap<>();
    item.put("id", current.conversationId());
    item.put("conversationId", current.conversationId());
    item.put("memberUserIds", current.memberUserIds());
    item.put("createdAtMs", current.createdAtMs());
    item.put("updatedAtMs", lastMessageAtMs);
    item.put("lastMessageAtMs", lastMessageAtMs);

    try {
      conversationsContainer.upsertItem(item, new PartitionKey(current.conversationId()), null);
      return true;
    } catch (Exception e) {
      logger.error("Failed to update conversation timestamps conversationId={}", conversationId, e);
      return false;
    }
  }

  /**
   * Responsibility: quickly test whether a user id exists.
   * Input: userId.
   * Output: true when found, false when absent.
   */
  public boolean userExistsInDB(String userId) {
    return findUserByUserId(userId).isPresent();
  }

  /**
   * Responsibility: resolve friendly user label for logs/UI.
   * Input: userId.
   * Output: email when available, otherwise userId, null when unknown.
   */
  public String getUserName(String userId) {
    Optional<UserRecord> record = findUserByUserId(userId);
    if (record.isEmpty()) {
      return null;
    }
    if (record.get().email() != null && !record.get().email().isBlank()) {
      return record.get().email();
    }
    return record.get().userId();
  }

  /**
   * Responsibility: normalize email for storage/query consistency.
   * Input: raw email text.
   * Output: trimmed lowercase email string.
   */
  private static String normalizeEmail(String email) {
    return email == null ? "" : email.trim().toLowerCase();
  }

  /**
   * Responsibility: safely cast generic object to string.
   * Input: unknown typed object.
   * Output: string value or null.
   */
  private static String asString(Object value) {
    return value instanceof String ? (String) value : null;
  }

  /**
   * Responsibility: safely cast numeric object to long.
   * Input: unknown typed object.
   * Output: long numeric value or 0.
   */
  private static long asLong(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    return 0L;
  }

  /**
   * Responsibility: map raw conversation members into normalized user id list.
   * Input: untyped member array from Cosmos document.
   * Output: user id list (empty when missing).
   */
  private static List<String> asStringList(Object value) {
    if (!(value instanceof List<?> rawList)) {
      return List.of();
    }
    List<String> out = new ArrayList<>();
    for (Object item : rawList) {
      if (item instanceof String text && !text.isBlank()) {
        out.add(text);
      }
    }
    return out;
  }

  /**
   * Responsibility: map raw Cosmos document fields to typed conversation record.
   * Input: map row returned by Cosmos query/read.
   * Output: typed ConversationRecord.
   */
  private static ConversationRecord mapConversationRecord(Map<?, ?> row) {
    return new ConversationRecord(
        asString(row.get("id")),
        asStringList(row.get("memberUserIds")),
        asLong(row.get("createdAtMs")),
        asLong(row.get("updatedAtMs")),
        asLong(row.get("lastMessageAtMs")));
  }

  /**
   * Responsibility: map raw Cosmos document fields to typed message record.
   * Input: map row returned by Cosmos query/read.
   * Output: typed MessageRecord.
   */
  private static MessageRecord mapMessageRecord(Map<?, ?> row) {
    return new MessageRecord(
        asString(row.get("serverMsgId")),
        asString(row.get("clientMsgId")),
        asString(row.get("conversationId")),
        asLong(row.get("sequenceId")),
        asString(row.get("senderUserId")),
        asString(row.get("recipientUserId")),
        asString(row.get("text")),
        asLong(row.get("sentAtMs")),
        asString(row.get("status")),
        asLong(row.get("createdAtMs")),
        asLong(row.get("updatedAtMs")));
  }

  /**
   * Responsibility: release Cosmos client resources on shutdown.
   * Input: none.
   * Output: closed Cosmos client.
   */
  @PreDestroy
  public void close() {
    if (client != null) {
      client.close();
      logger.info("Cosmos DB client closed");
    }
  }

  public record UserRecord(String userId, String email, String passwordHash) {
  }

  public record ConversationRecord(
      String conversationId,
      List<String> memberUserIds,
      long createdAtMs,
      long updatedAtMs,
      long lastMessageAtMs) {
  }

  public record MessageRecord(
      String serverMsgId,
      String clientMsgId,
      String conversationId,
      long sequenceId,
      String senderUserId,
      String recipientUserId,
      String text,
      long sentAtMs,
      String status,
      long createdAtMs,
      long updatedAtMs) {
  }

  public enum PersistStatus {
    CREATED,
    ALREADY_EXISTS,
    FAILED
  }

  public record PersistResult(PersistStatus status, MessageRecord messageRecord, String errorReason) {
  }
}
