package com.distributedkv.client;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.config.NodeConfig;
import com.distributedkv.network.client.RpcClient;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;
import com.distributedkv.network.protocol.KVMessage;
import com.distributedkv.common.util.SerializationUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Implementation of the KVClient interface that communicates with the distributed key-value store
 * via RPC. It supports connection pooling, retries, and load balancing among server nodes.
 */
public class KVClientImpl implements KVClient {
    private static final Logger LOGGER = Logger.getLogger(KVClientImpl.class.getName());

    // Configuration
    private final List<NodeConfig> serverNodes;
    private final int connectionPoolSize;
    private final int maxRetries;
    private final long retryDelayMs;
    private final long sessionTimeoutMs;
    private final long operationTimeoutMs;

    // Connection pool of RPC clients
    private final Map<String, List<RpcClient>> connectionPool;

    // Session management
    private final Map<String, Session> activeSessions;

    // Subscription management
    private final Map<String, KeyChangeCallback> subscriptions;

    // Thread pools
    private final ExecutorService asyncExecutor;
    private final ScheduledExecutorService scheduler;

    // Metrics
    private final MetricsRegistry metricsRegistry;

    // Client state
    private final AtomicBoolean isRunning;

    /**
     * Creates a new KVClientImpl with the given configuration.
     *
     * @param serverNodes The list of server nodes to connect to
     * @param connectionPoolSize The size of the connection pool per server
     * @param maxRetries The maximum number of retries for operations
     * @param retryDelayMs The delay between retries in milliseconds
     * @param sessionTimeoutMs The session timeout in milliseconds
     * @param operationTimeoutMs The operation timeout in milliseconds
     */
    public KVClientImpl(List<NodeConfig> serverNodes, int connectionPoolSize, int maxRetries,
                       long retryDelayMs, long sessionTimeoutMs, long operationTimeoutMs) {
        this.serverNodes = new ArrayList<>(serverNodes);
        this.connectionPoolSize = connectionPoolSize;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.operationTimeoutMs = operationTimeoutMs;

        this.connectionPool = new ConcurrentHashMap<>();
        this.activeSessions = new ConcurrentHashMap<>();
        this.subscriptions = new ConcurrentHashMap<>();

        this.asyncExecutor = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "kv-client-async");
            thread.setDaemon(true);
            return thread;
        });

        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread thread = new Thread(r, "kv-client-scheduler");
            thread.setDaemon(true);
            return thread;
        });

        this.metricsRegistry = new MetricsRegistry();
        this.isRunning = new AtomicBoolean(true);

        initializeConnectionPool();
        startBackgroundTasks();
    }

    /**
     * Initializes the connection pool.
     */
    private void initializeConnectionPool() {
        LOGGER.info("Initializing connection pool with " + serverNodes.size() + " servers, " +
                   connectionPoolSize + " connections per server");

        for (NodeConfig nodeConfig : serverNodes) {
            String nodeId = nodeConfig.getNodeId();
            String host = nodeConfig.getHost();
            int port = nodeConfig.getPort();

            List<RpcClient> clients = new ArrayList<>(connectionPoolSize);

            for (int i = 0; i < connectionPoolSize; i++) {
                RpcClient client = new RpcClient(host, port);
                Result<Void> result = client.start();

                if (result.isSuccess()) {
                    clients.add(client);
                    LOGGER.fine("Successfully initialized connection to server " + nodeId + " (" + (i + 1) + "/" + connectionPoolSize + ")");
                } else {
                    LOGGER.warning("Failed to initialize connection to server " + nodeId + ": " + result.getStatus());
                }
            }

            if (!clients.isEmpty()) {
                connectionPool.put(nodeId, clients);
                LOGGER.info("Added " + clients.size() + " connections to server " + nodeId + " to the pool");
            } else {
                LOGGER.warning("No connections established to server " + nodeId);
            }
        }

        if (connectionPool.isEmpty()) {
            LOGGER.severe("Failed to initialize connection pool - no servers available");
        }
    }

    /**
     * Starts background tasks.
     */
    private void startBackgroundTasks() {
        // Schedule periodic health checks for connections
        scheduler.scheduleAtFixedRate(this::checkConnectionHealth,
                                    5000, 10000, TimeUnit.MILLISECONDS);

        // Schedule periodic session renewal
        scheduler.scheduleAtFixedRate(this::renewSessions,
                                    sessionTimeoutMs / 2, sessionTimeoutMs / 2, TimeUnit.MILLISECONDS);

        // Schedule metrics collection
        scheduler.scheduleAtFixedRate(this::collectMetrics,
                                    1000, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * Checks the health of all connections in the pool.
     */
    private void checkConnectionHealth() {
        if (!isRunning.get()) {
            return;
        }

        LOGGER.fine("Checking health of connection pool");

        for (Map.Entry<String, List<RpcClient>> entry : connectionPool.entrySet()) {
            String nodeId = entry.getKey();
            List<RpcClient> clients = entry.getValue();

            synchronized (clients) {
                List<RpcClient> unhealthyClients = new ArrayList<>();

                for (RpcClient client : clients) {
                    if (!client.isHealthy()) {
                        unhealthyClients.add(client);
                        LOGGER.warning("Detected unhealthy connection to server " + nodeId);
                    }
                }

                // Remove and replace unhealthy clients
                if (!unhealthyClients.isEmpty()) {
                    clients.removeAll(unhealthyClients);

                    // Close unhealthy clients
                    for (RpcClient client : unhealthyClients) {
                        client.stop();
                    }

                    // Find server config
                    NodeConfig nodeConfig = null;
                    for (NodeConfig config : serverNodes) {
                        if (config.getNodeId().equals(nodeId)) {
                            nodeConfig = config;
                            break;
                        }
                    }

                    // Create new connections if server config found
                    if (nodeConfig != null) {
                        for (int i = 0; i < unhealthyClients.size(); i++) {
                            RpcClient client = new RpcClient(nodeConfig.getHost(), nodeConfig.getPort());
                            Result<Void> result = client.start();

                            if (result.isSuccess()) {
                                clients.add(client);
                                LOGGER.info("Replaced unhealthy connection to server " + nodeId);
                            } else {
                                LOGGER.warning("Failed to replace unhealthy connection to server " + nodeId + ": " + result.getStatus());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Renews active sessions to prevent expiration.
     */
    private void renewSessions() {
        if (!isRunning.get()) {
            return;
        }

        LOGGER.fine("Renewing active sessions");

        for (Map.Entry<String, Session> entry : activeSessions.entrySet()) {
            String sessionId = entry.getKey();
            Session session = entry.getValue();

            if (System.currentTimeMillis() - session.getLastAccessTime() > sessionTimeoutMs / 2) {
                LOGGER.fine("Renewing session " + sessionId);

                Request request = new Request();
                request.setType(Request.Type.KV);

                KVMessage kvMessage = new KVMessage();
                kvMessage.setOperation(KVMessage.Operation.RENEW_SESSION);
                kvMessage.setSessionId(sessionId);

                request.setKvMessage(kvMessage);

                // Try to send the renewal request
                Result<Response> result = sendRequest(request);

                if (result.isSuccess() && result.getValue().getStatus() == Status.OK) {
                    session.updateLastAccessTime();
                    LOGGER.fine("Successfully renewed session " + sessionId);
                } else {
                    LOGGER.warning("Failed to renew session " + sessionId + ": " +
                                 (result.isSuccess() ? result.getValue().getStatus() : result.getStatus()));
                }
            }
        }
    }

    /**
     * Collects metrics from the client.
     */
    private void collectMetrics() {
        if (!isRunning.get()) {
            return;
        }

        Metrics metrics = new Metrics();

        // Connection pool metrics
        int totalConnections = 0;
        Map<String, Integer> connectionsPerServer = new HashMap<>();

        for (Map.Entry<String, List<RpcClient>> entry : connectionPool.entrySet()) {
            String nodeId = entry.getKey();
            List<RpcClient> clients = entry.getValue();

            int count = clients.size();
            totalConnections += count;
            connectionsPerServer.put(nodeId, count);
        }

        metrics.put("total_connections", totalConnections);
        metrics.put("connections_per_server", connectionsPerServer);

        // Session metrics
        metrics.put("active_sessions", activeSessions.size());

        // Subscription metrics
        metrics.put("active_subscriptions", subscriptions.size());

        metricsRegistry.registerMetrics("client", metrics);
    }

    /**
     * Gets an RPC client from the connection pool.
     *
     * @return A result containing an RPC client or an error
     */
    private Result<RpcClient> getClient() {
        if (connectionPool.isEmpty()) {
            return Result.failure(Status.SERVICE_UNAVAILABLE, "No servers available");
        }

        // Simple round-robin load balancing
        List<String> nodeIds = new ArrayList<>(connectionPool.keySet());
        int randomIndex = (int) (Math.random() * nodeIds.size());
        String nodeId = nodeIds.get(randomIndex);

        List<RpcClient> clients = connectionPool.get(nodeId);
        if (clients == null || clients.isEmpty()) {
            return Result.failure(Status.SERVICE_UNAVAILABLE, "No connections available for server " + nodeId);
        }

        synchronized (clients) {
            // Simple round-robin among connections to the same server
            RpcClient client = clients.remove(0);
            clients.add(client);
            return Result.success(client);
        }
    }

    /**
     * Sends a request to a server with retry logic.
     *
     * @param request The request to send
     * @return A result containing the response or an error
     */
    private Result<Response> sendRequest(Request request) {
        int retries = 0;
        Status lastStatus = null;

        while (retries <= maxRetries) {
            if (retries > 0) {
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Result.failure(Status.INTERNAL_ERROR, "Request interrupted");
                }

                LOGGER.fine("Retrying request (attempt " + (retries + 1) + "/" + (maxRetries + 1) + ")");
            }

            Result<RpcClient> clientResult = getClient();
            if (!clientResult.isSuccess()) {
                lastStatus = clientResult.getStatus();
                retries++;
                continue;
            }

            RpcClient client = clientResult.getValue();
            Result<Response> result = client.sendRequest(request);

            if (result.isSuccess()) {
                Response response = result.getValue();

                // Retry on certain status codes
                if (response.getStatus() == Status.UNAVAILABLE ||
                    response.getStatus() == Status.TIMEOUT) {
                    lastStatus = response.getStatus();
                    retries++;
                    continue;
                }

                return result;
            } else {
                lastStatus = result.getStatus();
                retries++;
            }
        }

        return Result.failure(lastStatus != null ? lastStatus : Status.UNKNOWN,
                            "Request failed after " + (maxRetries + 1) + " attempts");
    }

    /**
     * Sends a request asynchronously.
     *
     * @param request The request to send
     * @return A future that will contain the result
     */
    private CompletableFuture<Result<Response>> sendRequestAsync(Request request) {
        return CompletableFuture.supplyAsync(() -> sendRequest(request), asyncExecutor);
    }

    @Override
    public Result<Session> openSession() {
        LOGGER.fine("Opening new session");

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.OPEN_SESSION);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                KVMessage responseMessage = response.getKvMessage();
                if (responseMessage != null) {
                    String sessionId = responseMessage.getSessionId();

                    Session session = new Session(sessionId);
                    activeSessions.put(sessionId, session);

                    LOGGER.info("Successfully opened session " + sessionId);
                    return Result.success(session);
                } else {
                    return Result.failure(Status.INTERNAL_ERROR, "Invalid response message");
                }
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<Void> closeSession(Session session) {
        if (session == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Session is null");
        }

        String sessionId = session.getSessionId();
        LOGGER.fine("Closing session " + sessionId);

        if (!activeSessions.containsKey(sessionId)) {
            return Result.failure(Status.NOT_FOUND, "Session not found");
        }

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.CLOSE_SESSION);
        kvMessage.setSessionId(sessionId);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        // Remove the session from active sessions regardless of the result
        activeSessions.remove(sessionId);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                LOGGER.info("Successfully closed session " + sessionId);
                return Result.success(null);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<byte[]> get(String key) {
        if (key == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Key is null");
        }

        LOGGER.fine("Getting value for key: " + key);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.GET);
        kvMessage.setKey(key);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                KVMessage responseMessage = response.getKvMessage();
                if (responseMessage != null) {
                    byte[] value = responseMessage.getValue();
                    LOGGER.fine("Successfully got value for key: " + key);
                    return Result.success(value);
                } else {
                    return Result.failure(Status.INTERNAL_ERROR, "Invalid response message");
                }
            } else if (response.getStatus() == Status.NOT_FOUND) {
                LOGGER.fine("Key not found: " + key);
                return Result.failure(Status.NOT_FOUND, "Key not found: " + key);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public CompletableFuture<Result<byte[]>> getAsync(String key) {
        return CompletableFuture.supplyAsync(() -> get(key), asyncExecutor);
    }

    @Override
    public Result<Void> put(String key, byte[] value) {
        if (key == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Key is null");
        }

        if (value == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Value is null");
        }

        LOGGER.fine("Putting value for key: " + key);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.PUT);
        kvMessage.setKey(key);
        kvMessage.setValue(value);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                LOGGER.fine("Successfully put value for key: " + key);
                return Result.success(null);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public CompletableFuture<Result<Void>> putAsync(String key, byte[] value) {
        return CompletableFuture.supplyAsync(() -> put(key, value), asyncExecutor);
    }

    @Override
    public Result<Void> delete(String key) {
        if (key == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Key is null");
        }

        LOGGER.fine("Deleting key: " + key);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.DELETE);
        kvMessage.setKey(key);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                LOGGER.fine("Successfully deleted key: " + key);
                return Result.success(null);
            } else if (response.getStatus() == Status.NOT_FOUND) {
                LOGGER.fine("Key not found: " + key);
                return Result.failure(Status.NOT_FOUND, "Key not found: " + key);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public CompletableFuture<Result<Void>> deleteAsync(String key) {
        return CompletableFuture.supplyAsync(() -> delete(key), asyncExecutor);
    }

    @Override
    public Result<Map<String, byte[]>> multiGet(List<String> keys) {
        if (keys == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Keys is null");
        }

        if (keys.isEmpty()) {
            return Result.success(new HashMap<>());
        }

        LOGGER.fine("Getting values for " + keys.size() + " keys");

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.MULTI_GET);
        kvMessage.setKeys(keys);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                KVMessage responseMessage = response.getKvMessage();
                if (responseMessage != null) {
                    Map<String, byte[]> keyValues = responseMessage.getKeyValues();
                    LOGGER.fine("Successfully got values for " + keyValues.size() + "/" + keys.size() + " keys");
                    return Result.success(keyValues);
                } else {
                    return Result.failure(Status.INTERNAL_ERROR, "Invalid response message");
                }
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public CompletableFuture<Result<Map<String, byte[]>>> multiGetAsync(List<String> keys) {
        return CompletableFuture.supplyAsync(() -> multiGet(keys), asyncExecutor);
    }

    @Override
    public Result<Void> multiPut(Map<String, byte[]> keyValues) {
        if (keyValues == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "KeyValues is null");
        }

        if (keyValues.isEmpty()) {
            return Result.success(null);
        }

        LOGGER.fine("Putting values for " + keyValues.size() + " keys");

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.MULTI_PUT);
        kvMessage.setKeyValues(keyValues);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                LOGGER.fine("Successfully put values for " + keyValues.size() + " keys");
                return Result.success(null);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public CompletableFuture<Result<Void>> multiPutAsync(Map<String, byte[]> keyValues) {
        return CompletableFuture.supplyAsync(() -> multiPut(keyValues), asyncExecutor);
    }

    @Override
    public Result<Boolean> compareAndSet(String key, byte[] expectedValue, byte[] newValue) {
        if (key == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Key is null");
        }

        LOGGER.fine("Performing compare-and-set for key: " + key);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.COMPARE_AND_SET);
        kvMessage.setKey(key);
        kvMessage.setValue(newValue);
        kvMessage.setExpectedValue(expectedValue);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                KVMessage responseMessage = response.getKvMessage();
                if (responseMessage != null) {
                    boolean success = responseMessage.isSuccess();
                    LOGGER.fine("Compare-and-set for key " + key + " " + (success ? "succeeded" : "failed"));
                    return Result.success(success);
                } else {
                    return Result.failure(Status.INTERNAL_ERROR, "Invalid response message");
                }
            } else if (response.getStatus() == Status.NOT_FOUND) {
                LOGGER.fine("Key not found: " + key);
                return Result.failure(Status.NOT_FOUND, "Key not found: " + key);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public CompletableFuture<Result<Boolean>> compareAndSetAsync(String key, byte[] expectedValue, byte[] newValue) {
        return CompletableFuture.supplyAsync(() -> compareAndSet(key, expectedValue, newValue), asyncExecutor);
    }

    @Override
    public Result<String> beginTransaction() {
        LOGGER.fine("Beginning transaction");

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.BEGIN_TRANSACTION);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                KVMessage responseMessage = response.getKvMessage();
                if (responseMessage != null) {
                    String transactionId = responseMessage.getTransactionId();
                    LOGGER.fine("Successfully began transaction: " + transactionId);
                    return Result.success(transactionId);
                } else {
                    return Result.failure(Status.INTERNAL_ERROR, "Invalid response message");
                }
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<Void> commitTransaction(String transactionId) {
        if (transactionId == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Transaction ID is null");
        }

        LOGGER.fine("Committing transaction: " + transactionId);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.COMMIT_TRANSACTION);
        kvMessage.setTransactionId(transactionId);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                LOGGER.fine("Successfully committed transaction: " + transactionId);
                return Result.success(null);
            } else if (response.getStatus() == Status.NOT_FOUND) {
                LOGGER.fine("Transaction not found: " + transactionId);
                return Result.failure(Status.NOT_FOUND, "Transaction not found: " + transactionId);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<Void> abortTransaction(String transactionId) {
        if (transactionId == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Transaction ID is null");
        }

        LOGGER.fine("Aborting transaction: " + transactionId);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.ABORT_TRANSACTION);
        kvMessage.setTransactionId(transactionId);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                LOGGER.fine("Successfully aborted transaction: " + transactionId);
                return Result.success(null);
            } else if (response.getStatus() == Status.NOT_FOUND) {
                LOGGER.fine("Transaction not found: " + transactionId);
                return Result.failure(Status.NOT_FOUND, "Transaction not found: " + transactionId);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<byte[]> getWithinTransaction(String transactionId, String key) {
        if (transactionId == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Transaction ID is null");
        }

        if (key == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Key is null");
        }

        LOGGER.fine("Getting value for key within transaction: " + key + ", " + transactionId);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.GET_WITHIN_TRANSACTION);
        kvMessage.setTransactionId(transactionId);
        kvMessage.setKey(key);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                KVMessage responseMessage = response.getKvMessage();
                if (responseMessage != null) {
                    byte[] value = responseMessage.getValue();
                    LOGGER.fine("Successfully got value for key within transaction: " + key);
                    return Result.success(value);
                } else {
                    return Result.failure(Status.INTERNAL_ERROR, "Invalid response message");
                }
            } else if (response.getStatus() == Status.NOT_FOUND) {
                LOGGER.fine("Key not found within transaction: " + key);
                return Result.failure(Status.NOT_FOUND, "Key not found: " + key);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<Void> putWithinTransaction(String transactionId, String key, byte[] value) {
        if (transactionId == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Transaction ID is null");
        }

        if (key == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Key is null");
        }

        if (value == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Value is null");
        }

        LOGGER.fine("Putting value for key within transaction: " + key + ", " + transactionId);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.PUT_WITHIN_TRANSACTION);
        kvMessage.setTransactionId(transactionId);
        kvMessage.setKey(key);
        kvMessage.setValue(value);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                LOGGER.fine("Successfully put value for key within transaction: " + key);
                return Result.success(null);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<Map<String, byte[]>> getRange(String startKey, String endKey) {
        if (startKey == null || endKey == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Start key or end key is null");
        }

        LOGGER.fine("Getting range of keys: " + startKey + " to " + endKey);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.GET_RANGE);
        kvMessage.setStartKey(startKey);
        kvMessage.setEndKey(endKey);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                KVMessage responseMessage = response.getKvMessage();
                if (responseMessage != null) {
                    Map<String, byte[]> keyValues = responseMessage.getKeyValues();
                    LOGGER.fine("Successfully got " + keyValues.size() + " keys in range");
                    return Result.success(keyValues);
                } else {
                    return Result.failure(Status.INTERNAL_ERROR, "Invalid response message");
                }
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public CompletableFuture<Result<Map<String, byte[]>>> getRangeAsync(String startKey, String endKey) {
        return CompletableFuture.supplyAsync(() -> getRange(startKey, endKey), asyncExecutor);
    }

    @Override
    public Result<Map<String, byte[]>> getByPrefix(String prefix) {
        if (prefix == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Prefix is null");
        }

        LOGGER.fine("Getting keys with prefix: " + prefix);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.GET_BY_PREFIX);
        kvMessage.setPrefix(prefix);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                KVMessage responseMessage = response.getKvMessage();
                if (responseMessage != null) {
                    Map<String, byte[]> keyValues = responseMessage.getKeyValues();
                    LOGGER.fine("Successfully got " + keyValues.size() + " keys with prefix");
                    return Result.success(keyValues);
                } else {
                    return Result.failure(Status.INTERNAL_ERROR, "Invalid response message");
                }
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public CompletableFuture<Result<Map<String, byte[]>>> getByPrefixAsync(String prefix) {
        return CompletableFuture.supplyAsync(() -> getByPrefix(prefix), asyncExecutor);
    }

    @Override
    public Result<String> subscribe(String key, KeyChangeCallback callback) {
        if (key == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Key is null");
        }

        if (callback == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Callback is null");
        }

        LOGGER.fine("Subscribing to changes for key: " + key);

        // Generate a unique subscription ID
        String subscriptionId = UUID.randomUUID().toString();

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.SUBSCRIBE);
        kvMessage.setKey(key);
        kvMessage.setSubscriptionId(subscriptionId);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                // Store the callback
                subscriptions.put(subscriptionId, callback);

                // Start a background thread to listen for notifications
                asyncExecutor.submit(() -> listenForNotifications(subscriptionId, key, callback));

                LOGGER.fine("Successfully subscribed to changes for key: " + key + " with ID: " + subscriptionId);
                return Result.success(subscriptionId);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<Void> unsubscribe(String subscriptionId) {
        if (subscriptionId == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Subscription ID is null");
        }

        if (!subscriptions.containsKey(subscriptionId)) {
            return Result.failure(Status.NOT_FOUND, "Subscription not found: " + subscriptionId);
        }

        LOGGER.fine("Unsubscribing from changes with ID: " + subscriptionId);

        Request request = new Request();
        request.setType(Request.Type.KV);

        KVMessage kvMessage = new KVMessage();
        kvMessage.setOperation(KVMessage.Operation.UNSUBSCRIBE);
        kvMessage.setSubscriptionId(subscriptionId);

        request.setKvMessage(kvMessage);

        Result<Response> result = sendRequest(request);

        // Remove the subscription regardless of the result
        subscriptions.remove(subscriptionId);

        if (result.isSuccess()) {
            Response response = result.getValue();

            if (response.getStatus() == Status.OK) {
                LOGGER.fine("Successfully unsubscribed from changes with ID: " + subscriptionId);
                return Result.success(null);
            } else {
                return Result.failure(response.getStatus(), response.getMessage());
            }
        } else {
            return Result.failure(result.getStatus(), result.getMessage());
        }
    }

    @Override
    public Result<Void> close() {
        if (!isRunning.getAndSet(false)) {
            return Result.success(null); // Already closed
        }

        LOGGER.info("Closing KV client");

        try {
            // Shutdown the schedulers
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            asyncExecutor.shutdown();
            try {
                if (!asyncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Close all sessions
            for (String sessionId : new ArrayList<>(activeSessions.keySet())) {
                Session session = activeSessions.get(sessionId);
                if (session != null) {
                    try {
                        closeSession(session);
                    } catch (Exception e) {
                        LOGGER.warning("Failed to close session " + sessionId + ": " + e.getMessage());
                    }
                }
            }
            activeSessions.clear();

            // Unsubscribe from all subscriptions
            for (String subscriptionId : new ArrayList<>(subscriptions.keySet())) {
                try {
                    unsubscribe(subscriptionId);
                } catch (Exception e) {
                    LOGGER.warning("Failed to unsubscribe " + subscriptionId + ": " + e.getMessage());
                }
            }
            subscriptions.clear();

            // Close all connections
            for (List<RpcClient> clients : connectionPool.values()) {
                for (RpcClient client : clients) {
                    try {
                        client.stop();
                    } catch (Exception e) {
                        LOGGER.warning("Failed to close RPC client: " + e.getMessage());
                    }
                }
            }
            connectionPool.clear();

            LOGGER.info("KV client closed successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to close KV client: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, "Failed to close KV client: " + e.getMessage());
        }
    }

    /**
     * Listens for notifications about key changes.
     *
     * @param subscriptionId The subscription ID
     * @param key The key to listen for
     * @param callback The callback to invoke
     */
    private void listenForNotifications(String subscriptionId, String key, KeyChangeCallback callback) {
        while (isRunning.get() && subscriptions.containsKey(subscriptionId)) {
            try {
                // Send a poll request
                Request request = new Request();
                request.setType(Request.Type.KV);

                KVMessage kvMessage = new KVMessage();
                kvMessage.setOperation(KVMessage.Operation.POLL_NOTIFICATIONS);
                kvMessage.setSubscriptionId(subscriptionId);

                request.setKvMessage(kvMessage);

                Result<Response> result = sendRequest(request);

                if (result.isSuccess()) {
                    Response response = result.getValue();

                    if (response.getStatus() == Status.OK) {
                        KVMessage responseMessage = response.getKvMessage();
                        if (responseMessage != null && responseMessage.isHasNotification()) {
                            byte[] newValue = responseMessage.getValue();

                            // Invoke the callback
                            try {
                                callback.onKeyChange(key, newValue);
                            } catch (Exception e) {
                                LOGGER.warning("Error in key change callback: " + e.getMessage());
                            }
                        }
                    } else if (response.getStatus() == Status.NOT_FOUND) {
                        // Subscription was removed on the server side
                        LOGGER.warning("Subscription not found on server: " + subscriptionId);
                        subscriptions.remove(subscriptionId);
                        break;
                    }
                }

                // Sleep before polling again
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warning("Notification listener interrupted");
                break;
            } catch (Exception e) {
                LOGGER.warning("Error in notification listener: " + e.getMessage());

                // Exponential backoff
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        LOGGER.fine("Notification listener stopped for subscription: " + subscriptionId);
    }
}
