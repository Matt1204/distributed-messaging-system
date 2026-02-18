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

public class DatabaseManager {
    private final String dbUrl;

    public DatabaseManager(String dbPath) {
        this.dbUrl = "jdbc:sqlite:" + dbPath;
        ensureSchema();
    }

    public static boolean databaseExists(String dbPath) {
        return Files.exists(Path.of(dbPath));
    }

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

    private void ensureSchema() {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS messages (sequence_id TEXT PRIMARY KEY, sender_id TEXT, content TEXT, created_at INTEGER)");
            stmt.execute("CREATE TABLE IF NOT EXISTS user_state (user_id TEXT PRIMARY KEY, email TEXT, user_name TEXT, last_sync_sequence_id TEXT)");

            if (!hasColumn(conn, "user_state", "email")) {
                stmt.execute("ALTER TABLE user_state ADD COLUMN email TEXT");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to ensure database schema: " + e.getMessage(), e);
        }
    }

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

    public void insertMessage(String sequenceId, String senderId, String content, long createdAt) {
        String sql = "INSERT OR IGNORE INTO messages(sequence_id, sender_id, content, created_at) VALUES(?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, sequenceId);
            pstmt.setString(2, senderId);
            pstmt.setString(3, content);
            pstmt.setLong(4, createdAt);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error inserting message: " + e.getMessage());
        }
    }

    public void updateUserState(String userId, String email, String lastSyncSequenceId) {
        String sql = "INSERT INTO user_state(user_id, email, user_name, last_sync_sequence_id) VALUES(?, ?, ?, ?) " +
                     "ON CONFLICT(user_id) DO UPDATE SET email=excluded.email, user_name=excluded.user_name, last_sync_sequence_id=excluded.last_sync_sequence_id";
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

    public String getUserId() {
        String sql = "SELECT user_id FROM user_state LIMIT 1";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("user_id");
            }
        } catch (SQLException e) {
            System.err.println("Error getting user id: " + e.getMessage());
        }
        return null;
    }

    public String getLastSyncSequenceId(String userId) {
        String sql = "SELECT last_sync_sequence_id FROM user_state WHERE user_id = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("last_sync_sequence_id");
            }
        } catch (SQLException e) {
            System.err.println("Error getting last sync id: " + e.getMessage());
        }
        return null;
    }

    public String getEmail() {
        String sql = "SELECT COALESCE(email, user_name) AS identity_email FROM user_state LIMIT 1";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("identity_email");
            }
        } catch (SQLException e) {
            System.err.println("Error getting email: " + e.getMessage());
        }
        return null;
    }

    public String getUserName() {
        String sql = "SELECT user_name FROM user_state LIMIT 1";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("user_name");
            }
        } catch (SQLException e) {
            System.err.println("Error getting user name: " + e.getMessage());
        }
        return null;
    }
}
