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
import java.util.List;

/**
 * Responsibility: manage local SQLite persistence for user identity, messages, and conversation summaries.
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
   * Responsibility: create/migrate local tables required by persistency v1.
   * Input: none.
   * Output: normalized schema with messages/conversations/user_state tables.
   */
  private void ensureSchema() {
    try (Connection conn = DriverManager.getConnection(dbUrl);
        Statement stmt = conn.createStatement()) {
      // Core logic: drop legacy messages table shape once when conversation_id is missing.
      if (tableExists(conn, "messages") && !hasColumn(conn, "messages", "conversation_id")) {
        stmt.execute("DROP TABLE messages");
      }

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS messages ("
              + "client_msg_id TEXT NOT NULL,"
              + "direction TEXT NOT NULL,"
              + "server_msg_id TEXT,"
              + "conversation_id TEXT NOT NULL,"
              + "sender_user_id TEXT NOT NULL,"
              + "sender_email TEXT,"
              + "recipient_user_id TEXT NOT NULL,"
              + "recipient_email TEXT,"
              + "content TEXT NOT NULL,"
              + "sent_at_ms INTEGER NOT NULL,"
              + "status TEXT NOT NULL,"
              + "PRIMARY KEY(client_msg_id, direction)"
              + ")");

      stmt.execute(
          "CREATE INDEX IF NOT EXISTS idx_messages_conversation_time "
              + "ON messages(conversation_id, sent_at_ms DESC)");
      stmt.execute(
          "CREATE INDEX IF NOT EXISTS idx_messages_server_msg_id ON messages(server_msg_id)");

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
              + "user_name TEXT,"
              + "last_sync_sequence_id TEXT"
              + ")");

      if (!hasColumn(conn, "user_state", "email")) {
        stmt.execute("ALTER TABLE user_state ADD COLUMN email TEXT");
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
        "INSERT INTO messages(client_msg_id, direction, server_msg_id, conversation_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status) "
            + "VALUES(?, 'OUTBOUND', NULL, ?, ?, ?, ?, ?, ?, ?, 'PENDING_ACK') "
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
   * Input: ids/status returned by server ack.
   * Output: outbound row updated with canonical server fields and conversation summary refreshed.
   */
  public void markOutboundAckSuccess(
      String clientMsgId,
      String serverMsgId,
      String conversationId,
      long sentAtMs,
      String status) {
    String sql =
        "UPDATE messages SET server_msg_id=?, conversation_id=?, sent_at_ms=?, status=? "
            + "WHERE client_msg_id=? AND direction='OUTBOUND'";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, serverMsgId);
      pstmt.setString(2, conversationId);
      pstmt.setLong(3, sentAtMs);
      pstmt.setString(4, status);
      pstmt.setString(5, clientMsgId);
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
   * Responsibility: insert inbound canonical message and refresh conversation summary.
   * Input: inbound payload fields delivered by server.
   * Output: one INBOUND row and updated conversation metadata.
   */
  public void insertInboundMessage(
      String serverMsgId,
      String clientMsgId,
      String conversationId,
      String senderUserId,
      String senderEmail,
      String recipientUserId,
      String recipientEmail,
      String content,
      long sentAtMs,
      String status) {
    String sql =
        "INSERT OR IGNORE INTO messages(client_msg_id, direction, server_msg_id, conversation_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status) "
            + "VALUES(?, 'INBOUND', ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, clientMsgId);
      pstmt.setString(2, serverMsgId);
      pstmt.setString(3, conversationId);
      pstmt.setString(4, senderUserId);
      pstmt.setString(5, senderEmail);
      pstmt.setString(6, recipientUserId);
      pstmt.setString(7, recipientEmail);
      pstmt.setString(8, content);
      pstmt.setLong(9, sentAtMs);
      pstmt.setString(10, status);
      pstmt.executeUpdate();
      upsertConversationSummary(conn, conversationId, senderUserId, senderEmail, sentAtMs, content);
    } catch (SQLException e) {
      System.err.println("Error inserting inbound message: " + e.getMessage());
    }
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
        "SELECT client_msg_id, direction, server_msg_id, conversation_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status "
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
    // Core logic: reverse descending query result so UI renders oldest->newest in a natural reading order.
    java.util.Collections.reverse(rows);
    return rows;
  }

  /**
   * Responsibility: upsert current logged-in user identity/sync pointer.
   * Input: user identity fields and last synced message id.
   * Output: persisted user_state row.
   */
  public void updateUserState(String userId, String email, String lastSyncSequenceId) {
    String sql =
        "INSERT INTO user_state(user_id, email, user_name, last_sync_sequence_id) VALUES(?, ?, ?, ?) "
            + "ON CONFLICT(user_id) DO UPDATE SET email=excluded.email, user_name=excluded.user_name, "
            + "last_sync_sequence_id=excluded.last_sync_sequence_id";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, userId);
      pstmt.setString(2, email);
      pstmt.setString(3, email);
      pstmt.setString(4, lastSyncSequenceId);
      pstmt.executeUpdate();
    } catch (SQLException e) {
      System.err.println("Error updating user state: " + e.getMessage());
    }
  }

  /**
   * Responsibility: advance user's last synced sequence pointer.
   * Input: user id and last synced message id.
   * Output: updated user_state row.
   */
  public void updateLastSyncSequenceId(String userId, String lastSyncSequenceId) {
    String sql = "UPDATE user_state SET last_sync_sequence_id = ? WHERE user_id = ?";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, lastSyncSequenceId);
      pstmt.setString(2, userId);
      pstmt.executeUpdate();
    } catch (SQLException e) {
      System.err.println("Error updating last sync sequence id: " + e.getMessage());
    }
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
   * Responsibility: read current user's last sync id.
   * Input: user id.
   * Output: stored last sync sequence id or null.
   */
  public String getLastSyncSequenceId(String userId) {
    String sql = "SELECT last_sync_sequence_id FROM user_state WHERE user_id = ?";
    try (Connection conn = DriverManager.getConnection(dbUrl);
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, userId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("last_sync_sequence_id");
        }
      }
    } catch (SQLException e) {
      System.err.println("Error getting last sync id: " + e.getMessage());
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
        "SELECT client_msg_id, direction, server_msg_id, conversation_id, sender_user_id, sender_email, recipient_user_id, recipient_email, content, sent_at_ms, status "
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
      String senderUserId,
      String senderEmail,
      String recipientUserId,
      String recipientEmail,
      String content,
      long sentAtMs,
      String status) {}
}
