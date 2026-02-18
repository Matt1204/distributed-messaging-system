package com.coen6731.chat.server;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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

            client = new CosmosClientBuilder()
                    .endpoint(endpoint)
                    .key(key)
                    .buildClient();

            database = client.getDatabase(databaseName);
            usersContainer = database.getContainer("users");
            messagesContainer = database.getContainer("messages");

            logger.info("Cosmos DB client initialized successfully!!!");
            logger.info("Database Structure:");
            logger.info(" - Database: {}", database.getId());
            logger.info(" - Container: {} (Partition Key: /userId)", usersContainer.getId());
            logger.info(" - Container: {} (Partition Key: /recipientId or /senderId)", messagesContainer.getId());
        } catch (Exception e) {
            logger.error("Failed to initialize Cosmos DB client", e);
        }
    }

    public Optional<UserRecord> createUser(String email, String passwordHash) {
        if (usersContainer == null) {
            logger.error("Users container not initialized");
            return Optional.empty();
        }

        String normalizedEmail = normalizeEmail(email);
        String userId = "user-" + UUID.randomUUID();

        Map<String, Object> userItem = new HashMap<>();
        userItem.put("id", userId);
        userItem.put("userId", userId);
        userItem.put("email", normalizedEmail);
        userItem.put("passwordHash", passwordHash);
        long now = System.currentTimeMillis();
        userItem.put("createdAt", now);
        userItem.put("updatedAt", now);

        try {
            CosmosItemResponse<Map> response = usersContainer.upsertItem(userItem, new PartitionKey(userId), null);
            logger.info("User created in Cosmos DB: userId={}, email={}, statusCode={}", userId, normalizedEmail, response.getStatusCode());
            return Optional.of(new UserRecord(userId, normalizedEmail, passwordHash));
        } catch (Exception e) {
            logger.error("Failed to create user in Cosmos DB for email={}", normalizedEmail, e);
            return Optional.empty();
        }
    }

    public Optional<UserRecord> findUserByEmail(String email) {
        if (usersContainer == null) {
            logger.error("Users container not initialized");
            return Optional.empty();
        }

        String normalizedEmail = normalizeEmail(email);
        String sqlText = "SELECT TOP 1 c.userId, c.email, c.passwordHash FROM c WHERE c.email = @email";
        SqlQuerySpec querySpec = new SqlQuerySpec(sqlText)
                .setParameters(java.util.List.of(new SqlParameter("@email", normalizedEmail)));

        try {
            CosmosPagedIterable<Map> results = usersContainer.queryItems(
                    querySpec,
                    new CosmosQueryRequestOptions(),
                    Map.class);
            for (Map record : results) {
                Object userIdObj = record.get("userId");
                Object emailObj = record.get("email");
                Object hashObj = record.get("passwordHash");
                if (userIdObj instanceof String && emailObj instanceof String) {
                    return Optional.of(new UserRecord(
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
            return Optional.of(new UserRecord(
                    asString(item.get("userId")),
                    asString(item.get("email")),
                    asString(item.get("passwordHash"))));
        } catch (Exception e) {
            logger.debug("User {} not found in Cosmos DB", userId);
            return Optional.empty();
        }
    }

    public boolean userExistsInDB(String userId) {
        return findUserByUserId(userId).isPresent();
    }

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

    private static String normalizeEmail(String email) {
        return email == null ? "" : email.trim().toLowerCase();
    }

    private static String asString(Object value) {
        return value instanceof String ? (String) value : null;
    }

    @PreDestroy
    public void close() {
        if (client != null) {
            client.close();
            logger.info("Cosmos DB client closed");
        }
    }

    public record UserRecord(String userId, String email, String passwordHash) {}
}
