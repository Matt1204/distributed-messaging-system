package com.coen6731.chat.server;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

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

    public void registerUser(String userId, String userName) {
        if (usersContainer == null) {
            logger.error("Users container not initialized");
            return;
        }

        Map<String, Object> userItem = new HashMap<>();
        userItem.put("id", userId);
        userItem.put("userId", userId);
        userItem.put("userName", userName);
        userItem.put("registeredAt", System.currentTimeMillis());

        try {
            CosmosItemResponse<Map> response = usersContainer.upsertItem(userItem, new PartitionKey(userId), null);
            logger.info("User registered/updated in Cosmos DB: userName={}, userId={}, statusCode={}", userName, userId, response.getStatusCode());
        } catch (Exception e) {
            logger.error("Failed to register user in Cosmos DB: userName={}, userId={}", userName, userId, e);
        }
    }

    public boolean userExistsInDB(String userId) {
        if (usersContainer == null) {
            logger.error("Users container not initialized");
            return false;
        }

        try {
            usersContainer.readItem(userId, new PartitionKey(userId), Map.class);
            return true;
        } catch (Exception e) {
            // Cosmos DB throws an exception (usually 404 Not Found) if the item doesn't exist
            logger.debug("User {} not found in Cosmos DB", userId);
            return false;
        }
    }

    public String getUserName(String userId) {
        if (usersContainer == null) {
            logger.error("Users container not initialized");
            return null;
        }

        try {
            CosmosItemResponse<Map> response = usersContainer.readItem(userId, new PartitionKey(userId), Map.class);
            @SuppressWarnings("unchecked")
            Map<String, Object> userItem = (Map<String, Object>) response.getItem();
            return userItem != null ? (String) userItem.get("userName") : null;
        } catch (Exception e) {
            logger.debug("User {} not found in Cosmos DB while fetching userName", userId);
            return null;
        }
    }

    @PreDestroy
    public void close() {
        if (client != null) {
            client.close();
            logger.info("Cosmos DB client closed");
        }
    }
}
