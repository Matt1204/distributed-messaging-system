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

    public void updateUserState(String userId, String userName, String lastSyncSequenceId) {
        String sql = "INSERT INTO user_state(user_id, user_name, last_sync_sequence_id) VALUES(?, ?, ?) " +
                     "ON CONFLICT(user_id) DO UPDATE SET user_name=excluded.user_name, last_sync_sequence_id=excluded.last_sync_sequence_id";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            pstmt.setString(2, userName);
            pstmt.setString(3, lastSyncSequenceId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating user state: " + e.getMessage());
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
