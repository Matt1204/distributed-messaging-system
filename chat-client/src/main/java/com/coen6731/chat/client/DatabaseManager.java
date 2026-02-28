package com.coen6731.chat.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Responsibility: manage local SQLite persistence for user identity, messages, conversations, and cursors.
 * Input: CRUD requests from client session/response handler.
 * Output: persisted rows and query results used by UI rendering.
 */
public class DatabaseManager {
  private final String dbUrl;

  /**
   * Responsibility: initialize DB manager and ensure required schema exists.
   * Input: filesystem path for sqlite database.
   * Output: ready-to-use database manager.
   */
  public DatabaseManager(String dbPath) {
    this.dbUrl = "jdbc:sqlite:" + dbPath;
    ensureSchema();
  }

  /**
   * Responsibility: check if a sqlite DB file already exists.
   * Input: database file path.
   * Output: true when file exists on disk.
   */
  public static boolean databaseExists(String dbPath) {
    return Files.exists(Path.of(dbPath));
  }

  /**
   * Responsibility: initialize sqlite DB from a semicolon-delimited SQL script.
   * Input: target db path and script path.
   * Output: created schema/data described in script.
   */
  public static void initializeDatabase(String dbPath, String initSqlPath) {
    Path db = Path.of(dbPath);
    Path parent = db.getParent();
    try {
      if (parent != null) {
        Files.createDirectories(parent);
      }

      String script = Files.readString(Path.of(initSqlPath));
      try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
          Statement stmt = conn.createStatement()) {
        for (String rawSql : script.split(";")) {
          String sql = rawSql.trim();
          if (!sql.isEmpty()) {
            stmt.execute(sql);
          }
        }
      }
    } catch (IOException | SQLException e) {
      throw new RuntimeException("Failed to initialize database: " + e.getMessage(), e);
    }
  }

  /**
   * Responsibility: create/migrate local tables required by persistency v2.
   * Input: none.
   * Output: normalized schema with messages/conversations/user_state/cursor tables.
   */
  private void ensureSchema() {
    try (Connection conn = DriverManager.getConnection(dbUrl);
        Statement stmt = conn.createStatement()) {
      if (tableExists(conn, "messages") && !hasColumn(conn, "messages", "conversation_id")) {
        stmt.execute("DROP TABLE messages");
      }

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS messages ("
              + "client_msg_id TEXT NOT NULL,"
              + "direction TEXT NOT NULL,"
              + "server_msg_id TEXT,"
              + "conversation_id TEXT NOT NULL,"
              + "sequence_id INTEGER,"
              + "sender_user_id TEXT NOT NULL,"
              + "sender_email TEXT,"
              + "recipient_user_id TEXT NOT NULL,"
              + "recipient_email TEXT,"
              + "content TEXT NOT NULL,"
              + "sent_at_ms INTEGER NOT NULL,"
              + "status TEXT NOT NULL,"
              + "PRIMARY KEY(client_msg_id, direction)"
              + ")");

      if (!hasColumn(conn, "messages", "sequence_id")) {
        stmt.execute("ALTER TABLE messages ADD COLUMN sequence_id INTEGER");
      }

      stmt.execute(
          "CREATE INDEX IF NOT EXISTS idx_messages_conversation_time "
              + "ON messages(conversation_id, sent_at_ms DESC)");
      stmt.execute("CREATE INDEX IF NOT EXISTS idx_messages_server_msg_id ON messages(server_msg_id)");
      stmt.execute(
          "CREATE UNIQUE INDEX IF NOT EXISTS uidx_messages_conversation_server_msg_id "
              + "ON messages(conversation_id, server_msg_id) "
              + "WHERE server_msg_id IS NOT NULL AND server_msg_id <> ''");
      stmt.execute(
          "CREATE UNIQUE INDEX IF NOT EXISTS uidx_messages_conversation_sequence_id "
              + "ON messages(conversation_id, sequence_id) "
              + "WHERE sequence_id IS NOT NULL AND sequence_id > 0");

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS conversations ("
              + "conversation_id TEXT PRIMARY KEY,"
              + "peer_user_id TEXT NOT NULL,"
              + "peer_email TEXT,"
              + "last_message_at INTEGER NOT NULL,"
              + "last_message_preview TEXT"
              + ")");
      stmt.execute(
          "CREATE INDEX IF NOT EXISTS idx_conversations_last_message_at "
              + "ON conversations(last_message_at DESC)");

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS user_state ("
              + "user_id TEXT PRIMARY KEY,"
              + "email TEXT,"
              + "user_name TEXT"
              + ")");
      if (!hasColumn(conn, "user_state", "email")) {
        stmt.execute("ALTER TABLE user_state ADD COLUMN email TEXT");
      }

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS local_conversation_cursor ("
              + "user_id TEXT NOT NULL,"
              + "conversation_id TEXT NOT NULL,"
              + "latest_message_sequence_id INTEGER NOT NULL DEFAULT 0,"
              + "updated_at_ms INTEGER NOT NULL,"
              + "PRIMARY KEY(user_id, conversation_id)"
              + ")");
      if (hasColumn(conn, "local_conversation_cursor", "local_conversation_sequenceId")
          && !hasColumn(conn, "local_conversation_cursor", "latest_message_sequence_id")) {
        stmt.execute(
            "ALTER TABLE local_conversation_cursor RENAME COLUMN local_conversation_sequenceId TO latest_message_sequence_id");
      }
      if (hasColumn(conn, "local_conversation_cursor", "client_last_received_sequence_id")
          && !hasColumn(conn, "local_conversation_cursor", "latest_message_sequence_id")) {
        stmt.execute(
            "ALTER TABLE local_conversation_cursor RENAME COLUMN client_last_received_sequence_id TO latest_message_sequence_id");
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to ensure database schema: " + e.getMessage(), e);
    }
  }

  /**
   * Responsibility: check if a table exists.
   * Input: db connection and table name.
   * Output: true when sqlite master has this table.
   */
  private boolean tableExists(Connection conn, String tableName) throws SQLException {
    try (PreparedStatement pstmt =
            conn.prepareStatement(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1");
        ) {
      pstmt.setString(1, tableName);
      try (ResultSet rs = pstmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Responsibility: check if a table has a specific column name.
   * Input: db connection, table name, and target column.
   * Output: true when column exists.
   */
  private boolean hasColumn(Connection conn, String tableName, String columnName) throws SQLException {
    try (PreparedStatement pstmt = conn.prepareStatement("PRAGMA table_info(" + tableName + ")");
        ResultSet rs = pstmt.executeQuery()) {
      while (rs.next()) {
        String name = rs.getString("name");
        if (columnName.equalsIgnoreCase(name)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Responsibility: insert or refresh provisional outbound row before send ack arrives.
   * Input: provisional message fields from sender action.
   * Output: one OUTBOUND row in messages table.
   */
  public void upsertOutboundProvisional(
      String clientMsgId,
      String conversationId,
      String senderUserId,
      String senderEmail,
      String recipientUserId,
      String recipientEmail,
      String content,
      long sentAtMs) {
    String sql =
        "INSERT INTO messages(client_msg_id, direction, server_msg_id, conversation_id, sequence_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status) "
            + "VALUES(?, 'OUTBOUND', NULL, ?, NULL, ?, ?, ?, ?, ?, ?, 'PENDING_ACK') "
            + "ON CONFLICT(client_msg_id, direction) DO UPDATE SET "
            + "conversation_id=excluded.conversation_id, sender_user_id=excluded.sender_user_id, sender_email=excluded.sender_email, "
            + "recipient_user_id=excluded.recipient_user_id, recipient_email=excluded.recipient_email, content=excluded.content, sent_at_ms=excluded.sent_at_ms, status=excluded.status";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, clientMsgId);
      pstmt.setString(2, conversationId);
      pstmt.setString(3, senderUserId);
      pstmt.setString(4, senderEmail);
      pstmt.setString(5, recipientUserId);
      pstmt.setString(6, recipientEmail);
      pstmt.setString(7, content);
      pstmt.setLong(8, sentAtMs);
      pstmt.executeUpdate();
      if (conversationId != null && !conversationId.isBlank()) {
        upsertConversationSummary(conn, conversationId, recipientUserId, recipientEmail, sentAtMs, content);
      }
    } catch (SQLException e) {
      System.err.println("Error upserting provisional outbound message: " + e.getMessage());
    }
  }

  /**
   * Responsibility: finalize outbound row after successful send ack.
   * Input: ids/status/sequence returned by server ack.
   * Output: outbound row updated with canonical server fields and conversation summary refreshed.
   */
  public void markOutboundAckSuccess(
      String clientMsgId,
      String serverMsgId,
      String conversationId,
      long sequenceId,
      long sentAtMs,
      String status) {
    String sql =
        "UPDATE messages SET server_msg_id=?, conversation_id=?, sequence_id=?, sent_at_ms=?, status=? "
            + "WHERE client_msg_id=? AND direction='OUTBOUND'";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, serverMsgId);
      pstmt.setString(2, conversationId);
      if (sequenceId > 0) {
        pstmt.setLong(3, sequenceId);
      } else {
        pstmt.setNull(3, java.sql.Types.INTEGER);
      }
      pstmt.setLong(4, sentAtMs);
      pstmt.setString(5, status);
      pstmt.setString(6, clientMsgId);
      pstmt.executeUpdate();

      MessageRow outbound = findOutboundByClientMsgId(conn, clientMsgId);
      if (outbound != null) {
        upsertConversationSummary(
            conn,
            outbound.conversationId(),
            outbound.recipientUserId(),
            outbound.recipientEmail(),
            sentAtMs,
            outbound.content());
      }
    } catch (SQLException e) {
      System.err.println("Error marking outbound ack success: " + e.getMessage());
    }
  }

  /**
   * Responsibility: remove provisional outbound row after failed send ack.
   * Input: client message id.
   * Output: deleted OUTBOUND row if present.
   */
  public void deleteOutboundByClientMsgId(String clientMsgId) {
    String sql = "DELETE FROM messages WHERE client_msg_id=? AND direction='OUTBOUND'";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, clientMsgId);
      pstmt.executeUpdate();
    } catch (SQLException e) {
      System.err.println("Error deleting outbound message: " + e.getMessage());
    }
  }

  /**
   * Responsibility: idempotently insert one canonical message from live/catchup/history paths.
   * Input: canonical message fields and local direction/status.
   * Output: inserted or ignored row with conversation summary refresh.
   */
  public void upsertCanonicalMessage(
      String clientMsgId,
      String direction,
      String serverMsgId,
      String conversationId,
      long sequenceId,
      String senderUserId,
      String senderEmail,
      String recipientUserId,
      String recipientEmail,
      String content,
      long sentAtMs,
      String status,
      String peerUserId,
      String peerEmail) {
    String sql =
        "INSERT OR IGNORE INTO messages(client_msg_id, direction, server_msg_id, conversation_id, sequence_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status) "
            + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, clientMsgId);
      pstmt.setString(2, direction);
      pstmt.setString(3, serverMsgId);
      pstmt.setString(4, conversationId);
      if (sequenceId > 0) {
        pstmt.setLong(5, sequenceId);
      } else {
        pstmt.setNull(5, java.sql.Types.INTEGER);
      }
      pstmt.setString(6, senderUserId);
      pstmt.setString(7, senderEmail);
      pstmt.setString(8, recipientUserId);
      pstmt.setString(9, recipientEmail);
      pstmt.setString(10, content);
      pstmt.setLong(11, sentAtMs);
      pstmt.setString(12, status);
      pstmt.executeUpdate();
      upsertConversationSummary(conn, conversationId, peerUserId, peerEmail, sentAtMs, content);
    } catch (SQLException e) {
      System.err.println("Error upserting canonical message: " + e.getMessage());
    }
  }

  /**
   * Responsibility: insert inbound canonical message and refresh conversation summary.
   * Input: inbound payload fields delivered by server.
   * Output: one INBOUND row and updated conversation metadata.
   */
  public void insertInboundMessage(
      String serverMsgId,
      String clientMsgId,
      String conversationId,
      long sequenceId,
      String senderUserId,
      String senderEmail,
      String recipientUserId,
      String recipientEmail,
      String content,
      long sentAtMs,
      String status) {
    upsertCanonicalMessage(
        clientMsgId,
        "INBOUND",
        serverMsgId,
        conversationId,
        sequenceId,
        senderUserId,
        senderEmail,
        recipientUserId,
        recipientEmail,
        content,
        sentAtMs,
        status,
        senderUserId,
        senderEmail);
  }

  /**
   * Responsibility: query conversation summaries sorted by latest message timestamp.
   * Input: none.
   * Output: ordered conversation summary list for left pane.
   */
  public List<ConversationSummary> listConversations() {
    String sql =
        "SELECT conversation_id, peer_user_id, peer_email, last_message_at, last_message_preview "
            + "FROM conversations ORDER BY last_message_at DESC";
    List<ConversationSummary> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery()) {
      while (rs.next()) {
        rows.add(
            new ConversationSummary(
                rs.getString("conversation_id"),
                rs.getString("peer_user_id"),
                rs.getString("peer_email"),
                rs.getLong("last_message_at"),
                rs.getString("last_message_preview")));
      }
    } catch (SQLException e) {
      System.err.println("Error listing conversations: " + e.getMessage());
    }
    return rows;
  }

  /**
   * Responsibility: load latest N messages for a conversation.
   * Input: conversation id and max row count.
   * Output: chronologically ordered message list for current conversation.
   */
  public List<MessageRow> listLatestMessages(String conversationId, int limit) {
    String sql =
        "SELECT client_msg_id, direction, server_msg_id, conversation_id, sequence_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status "
            + "FROM messages WHERE conversation_id = ? ORDER BY sent_at_ms DESC LIMIT ?";
    List<MessageRow> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, conversationId);
      pstmt.setInt(2, limit);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          rows.add(mapMessageRow(rs));
        }
      }
    } catch (SQLException e) {
      System.err.println("Error listing latest messages: " + e.getMessage());
    }
    java.util.Collections.reverse(rows);
    return rows;
  }

  /**
   * Responsibility: load older messages before a sequence cursor for one conversation.
   * Input: conversation id, before-sequence cursor (exclusive), and max row count.
   * Output: chronologically ordered older messages (ascending by sequence_id).
   */
  public List<MessageRow> listMessagesBeforeSequence(
      String conversationId, long beforeSequenceId, int limit) {
    long effectiveBefore = beforeSequenceId <= 0 ? Long.MAX_VALUE : beforeSequenceId;
    int effectiveLimit = limit <= 0 ? 1 : limit;
    String sql =
        "SELECT client_msg_id, direction, server_msg_id, conversation_id, sequence_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status "
            + "FROM messages WHERE conversation_id = ? AND sequence_id > 0 AND sequence_id < ? "
            + "ORDER BY sequence_id DESC LIMIT ?";
    List<MessageRow> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, conversationId);
      pstmt.setLong(2, effectiveBefore);
      pstmt.setInt(3, effectiveLimit);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          rows.add(mapMessageRow(rs));
        }
      }
    } catch (SQLException e) {
      System.err.println("Error listing messages before sequence: " + e.getMessage());
    }
    java.util.Collections.reverse(rows);
    return rows;
  }

  /**
   * Responsibility: list existing positive sequence ids in an inclusive range.
   * Input: conversation id, start sequence, and end sequence.
   * Output: set of sequence ids already stored locally.
   */
  public Set<Long> listExistingSequenceIdsInRange(
      String conversationId, long startSequenceId, long endSequenceId) {
    if (startSequenceId <= 0 || endSequenceId <= 0 || startSequenceId > endSequenceId) {
      return Set.of();
    }
    String sql =
        "SELECT sequence_id FROM messages "
            + "WHERE conversation_id = ? AND sequence_id BETWEEN ? AND ? AND sequence_id > 0";
    Set<Long> sequenceIds = new HashSet<>();
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, conversationId);
      pstmt.setLong(2, startSequenceId);
      pstmt.setLong(3, endSequenceId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          sequenceIds.add(rs.getLong("sequence_id"));
        }
      }
    } catch (SQLException e) {
      System.err.println("Error listing existing sequence ids in range: " + e.getMessage());
    }
    return sequenceIds;
  }

  /**
   * Responsibility: list local messages in one inclusive sequence range.
   * Input: conversation id and target sequence range.
   * Output: messages ordered by sequence asc.
   */
  public List<MessageRow> listMessagesBySequenceRange(
      String conversationId, long startSequenceId, long endSequenceId) {
    if (startSequenceId <= 0 || endSequenceId <= 0 || startSequenceId > endSequenceId) {
      return List.of();
    }
    String sql =
        "SELECT client_msg_id, direction, server_msg_id, conversation_id, sequence_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status "
            + "FROM messages WHERE conversation_id = ? AND sequence_id BETWEEN ? AND ? "
            + "ORDER BY sequence_id ASC";
    List<MessageRow> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, conversationId);
      pstmt.setLong(2, startSequenceId);
      pstmt.setLong(3, endSequenceId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          rows.add(mapMessageRow(rs));
        }
      }
    } catch (SQLException e) {
      System.err.println("Error listing messages by sequence range: " + e.getMessage());
    }
    return rows;
  }

  /**
   * Responsibility: upsert current logged-in user identity.
   * Input: user identity fields.
   * Output: persisted user_state row.
   */
  public void updateUserState(String userId, String email) {
    String sql =
        "INSERT INTO user_state(user_id, email, user_name) VALUES(?, ?, ?) "
            + "ON CONFLICT(user_id) DO UPDATE SET email=excluded.email, user_name=excluded.user_name";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, userId);
      pstmt.setString(2, email);
      pstmt.setString(3, email);
      pstmt.executeUpdate();
    } catch (SQLException e) {
      System.err.println("Error updating user state: " + e.getMessage());
    }
  }

  /**
   * Responsibility: insert or update per-conversation client cursor.
   * Input: user id, conversation id, and applied sequence cursor.
   * Output: stored cursor row for later catchup hints.
   */
  public void upsertConversationCursor(String userId, String conversationId, long latestMessageSequenceId) {
    String sql =
        "INSERT INTO local_conversation_cursor(user_id, conversation_id, latest_message_sequence_id, updated_at_ms) "
            + "VALUES(?, ?, ?, ?) "
            + "ON CONFLICT(user_id, conversation_id) DO UPDATE SET "
            + "latest_message_sequence_id=excluded.latest_message_sequence_id, updated_at_ms=excluded.updated_at_ms";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, userId);
      pstmt.setString(2, conversationId);
      pstmt.setLong(3, Math.max(0L, latestMessageSequenceId));
      pstmt.setLong(4, System.currentTimeMillis());
      pstmt.executeUpdate();
    } catch (SQLException e) {
      System.err.println("Error upserting conversation cursor: " + e.getMessage());
    }
  }

  /**
   * Responsibility: advance per-conversation cursor only when new value is higher.
   * Input: user id, conversation id, and candidate sequence id.
   * Output: cursor row updated iff new sequence is greater than current.
   */
  public void advanceConversationCursorIfHigher(String userId, String conversationId, long candidateSequenceId) {
    if (candidateSequenceId <= 0) {
      return;
    }
    long current = getConversationCursor(userId, conversationId);
    if (candidateSequenceId > current) {
      upsertConversationCursor(userId, conversationId, candidateSequenceId);
    }
  }

  /**
   * Responsibility: read one per-conversation cursor.
   * Input: user id and conversation id.
   * Output: current cursor value, default 0 when no row exists.
   */
  public long getConversationCursor(String userId, String conversationId) {
    String sql =
        "SELECT latest_message_sequence_id FROM local_conversation_cursor WHERE user_id = ? AND conversation_id = ?";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, userId);
      pstmt.setString(2, conversationId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          return rs.getLong("latest_message_sequence_id");
        }
      }
    } catch (SQLException e) {
      System.err.println("Error reading conversation cursor: " + e.getMessage());
    }
    return 0L;
  }

  /**
   * Responsibility: enumerate all local conversation cursors for one user.
   * Input: user id.
   * Output: map of conversation id to cursor value.
   */
  public Map<String, Long> listConversationCursors(String userId) {
    String sql =
        "SELECT conversation_id, latest_message_sequence_id FROM local_conversation_cursor WHERE user_id = ?";
    Map<String, Long> cursors = new HashMap<>();
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, userId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          cursors.put(rs.getString("conversation_id"), rs.getLong("latest_message_sequence_id"));
        }
      }
    } catch (SQLException e) {
      System.err.println("Error listing conversation cursors: " + e.getMessage());
    }
    return cursors;
  }

  /**
   * Responsibility: read the max stored positive sequence id for one conversation.
   * Input: conversation id.
   * Output: max sequence id in local messages table, or 0 when missing.
   */
  public long getMaxStoredSequenceId(String conversationId) {
    if (conversationId == null || conversationId.isBlank()) {
      return 0L;
    }
    String sql =
        "SELECT COALESCE(MAX(sequence_id), 0) AS max_sequence_id "
            + "FROM messages WHERE conversation_id = ? AND sequence_id > 0";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, conversationId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          return rs.getLong("max_sequence_id");
        }
      }
    } catch (SQLException e) {
      System.err.println("Error reading max stored sequence id: " + e.getMessage());
    }
    return 0L;
  }

  /**
   * Responsibility: read current local user id.
   * Input: none.
   * Output: first user id in user_state or null.
   */
  public String getUserId() {
    String sql = "SELECT user_id FROM user_state LIMIT 1";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery()) {
      if (rs.next()) {
        return rs.getString("user_id");
      }
    } catch (SQLException e) {
      System.err.println("Error getting user id: " + e.getMessage());
    }
    return null;
  }

  /**
   * Responsibility: read normalized user email.
   * Input: none.
   * Output: email from user_state, fallback to user_name.
   */
  public String getEmail() {
    String sql = "SELECT COALESCE(email, user_name) AS identity_email FROM user_state LIMIT 1";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery()) {
      if (rs.next()) {
        return rs.getString("identity_email");
      }
    } catch (SQLException e) {
      System.err.println("Error getting email: " + e.getMessage());
    }
    return null;
  }

  /**
   * Responsibility: read user_name fallback field.
   * Input: none.
   * Output: first user_name value.
   */
  public String getUserName() {
    String sql = "SELECT user_name FROM user_state LIMIT 1";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery()) {
      if (rs.next()) {
        return rs.getString("user_name");
      }
    } catch (SQLException e) {
      System.err.println("Error getting user name: " + e.getMessage());
    }
    return null;
  }

  /**
   * Responsibility: upsert one conversation row after send/receive activity.
   * Input: db connection and summary fields.
   * Output: refreshed conversation row keyed by conversation id.
   */
  private void upsertConversationSummary(
      Connection conn,
      String conversationId,
      String peerUserId,
      String peerEmail,
      long lastMessageAt,
      String preview) throws SQLException {
    String sql =
        "INSERT INTO conversations(conversation_id, peer_user_id, peer_email, last_message_at, last_message_preview) "
            + "VALUES(?, ?, ?, ?, ?) "
            + "ON CONFLICT(conversation_id) DO UPDATE SET "
            + "peer_user_id=excluded.peer_user_id, peer_email=excluded.peer_email, "
            + "last_message_at=excluded.last_message_at, last_message_preview=excluded.last_message_preview";
    try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, conversationId);
      pstmt.setString(2, peerUserId);
      pstmt.setString(3, peerEmail);
      pstmt.setLong(4, lastMessageAt);
      pstmt.setString(5, abbreviate(preview, 120));
      pstmt.executeUpdate();
    }
  }

  /**
   * Responsibility: find outbound row by client id inside same transaction/connection.
   * Input: connection and outbound client message id.
   * Output: mapped message row or null.
   */
  private MessageRow findOutboundByClientMsgId(Connection conn, String clientMsgId) throws SQLException {
    String sql =
        "SELECT client_msg_id, direction, server_msg_id, conversation_id, sequence_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status "
            + "FROM messages WHERE client_msg_id = ? AND direction = 'OUTBOUND' LIMIT 1";
    try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, clientMsgId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          return mapMessageRow(rs);
        }
      }
    }
    return null;
  }

  /**
   * Responsibility: map SQL row to strongly typed message row record.
   * Input: result set positioned at one row.
   * Output: message row record.
   */
  private MessageRow mapMessageRow(ResultSet rs) throws SQLException {
    return new MessageRow(
        rs.getString("client_msg_id"),
        rs.getString("direction"),
        rs.getString("server_msg_id"),
        rs.getString("conversation_id"),
        rs.getLong("sequence_id"),
        rs.getString("sender_user_id"),
        rs.getString("sender_email"),
        rs.getString("recipient_user_id"),
        rs.getString("recipient_email"),
        rs.getString("content"),
        rs.getLong("sent_at_ms"),
        rs.getString("status"));
  }

  /**
   * Responsibility: keep preview strings compact for conversation list UI.
   * Input: source text and max length.
   * Output: shortened string with ellipsis when needed.
   */
  private String abbreviate(String value, int maxLen) {
    if (value == null) {
      return "";
    }
    if (value.length() <= maxLen) {
      return value;
    }
    return value.substring(0, maxLen - 1) + "...";
  }

  public record ConversationSummary(
      String conversationId,
      String peerUserId,
      String peerEmail,
      long lastMessageAt,
      String lastMessagePreview) {}

  public record MessageRow(
      String clientMsgId,
      String direction,
      String serverMsgId,
      String conversationId,
      long sequenceId,
      String senderUserId,
      String senderEmail,
      String recipientUserId,
      String recipientEmail,
      String content,
      long sentAtMs,
      String status) {}
}
