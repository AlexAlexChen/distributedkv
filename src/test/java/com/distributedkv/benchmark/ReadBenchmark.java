package com.distributedkv.benchmark;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.distributedkv.common.Result;
import com.distributedkv.config.ClusterConfig;
import com.distributedkv.config.NodeConfig;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.config.StorageConfig;
import com.distributedkv.network.client.KVClient;
import com.distributedkv.network.client.KVClientImpl;
import com.distributedkv.server.KVServer;

/**
 * Benchmark for read performance in different scenarios.
 * Tests ReadIndex and FollowerRead optimizations.
 */
public class ReadBenchmark {

    // Configuration
    private static final int NUM_NODES = 3;
    private static final int BASE_PORT = 20000;
    private static final int VALUE_SIZE = 1024; // 1KB
    private static final int NUM_KEYS = 10000;
    private static final int NUM_CLIENTS = 8;
    private static final int OPS_PER_CLIENT = 1000;

    // Track performance metrics
    private static class Metrics {
        long startTimeMs;
        long endTimeMs;
        AtomicLong totalOps = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);
        AtomicLong failedOps = new AtomicLong(0);
        AtomicLong totalLatencyNanos = new AtomicLong(0);

        public void start() {
            startTimeMs = System.currentTimeMillis();
        }

        public void end() {
            endTimeMs = System.currentTimeMillis();
        }

        public void recordSuccess(long latencyNanos) {
            totalOps.incrementAndGet();
            successfulOps.incrementAndGet();
            totalLatencyNanos.addAndGet(latencyNanos);
        }

        public void recordFailure() {
            totalOps.incrementAndGet();
            failedOps.incrementAndGet();
        }

        public double getSuccessRate() {
            return (double) successfulOps.get() / totalOps.get() * 100.0;
        }

        public double getThroughput() {
            long durationMs = endTimeMs - startTimeMs;
            return (double) successfulOps.get() / (durationMs / 1000.0);
        }

        public double getAvgLatencyMs() {
            return (double) totalLatencyNanos.get() / (successfulOps.get() * 1_000_000.0);
        }

        @Override
        public String toString() {
            return String.format(
                "Results: %d ops, %.2f%% success rate, %.2f ops/sec, %.2f ms avg latency",
                totalOps.get(), getSuccessRate(), getThroughput(), getAvgLatencyMs()
            );
        }
    }

    public static void main(String[] args) throws Exception {
        // Setup directories
        List<Path> dataPaths = setupDataDirectories();

        try {
            // Create and start the cluster
            List<KVServer> servers = startCluster(dataPaths);
            List<KVClient> clients = createClients();

            // Wait for cluster to initialize
            Thread.sleep(5000);

            // Prepare test data
            System.out.println("Preparing test data...");
            prepareTestData(clients.get(0));

            // Run benchmarks
            System.out.println("\n------ Standard Read Test (Leader only) ------");
            Metrics standardReadMetrics = runStandardReadTest(clients);
            System.out.println(standardReadMetrics);

            // Enable ReadIndex optimization
            System.out.println("\n------ Enabling ReadIndex optimization ------");
            updateRaftConfig(servers, true, false);
            Thread.sleep(2000);

            System.out.println("\n------ ReadIndex Read Test ------");
            Metrics readIndexMetrics = runReadIndexTest(clients);
            System.out.println(readIndexMetrics);

            // Enable FollowerRead optimization
            System.out.println("\n------ Enabling FollowerRead optimization ------");
            updateRaftConfig(servers, true, true);
            Thread.sleep(2000);

            System.out.println("\n------ FollowerRead Test ------");
            Metrics followerReadMetrics = runFollowerReadTest(clients);
            System.out.println(followerReadMetrics);

            // Compare results
            System.out.println("\n------ Performance Comparison ------");
            System.out.println("Standard Read: " + standardReadMetrics.getThroughput() + " ops/sec, " +
                              standardReadMetrics.getAvgLatencyMs() + " ms avg latency");
            System.out.println("ReadIndex: " + readIndexMetrics.getThroughput() + " ops/sec, " +
                              readIndexMetrics.getAvgLatencyMs() + " ms avg latency");
            System.out.println("FollowerRead: " + followerReadMetrics.getThroughput() + " ops/sec, " +
                              followerReadMetrics.getAvgLatencyMs() + " ms avg latency");

            // Calculate improvement percentages
            double readIndexThroughputImprovement =
                (readIndexMetrics.getThroughput() / standardReadMetrics.getThroughput() - 1.0) * 100.0;
            double followerReadThroughputImprovement =
                (followerReadMetrics.getThroughput() / standardReadMetrics.getThroughput() - 1.0) * 100.0;

            double readIndexLatencyImprovement =
                (1.0 - readIndexMetrics.getAvgLatencyMs() / standardReadMetrics.getAvgLatencyMs()) * 100.0;
            double followerReadLatencyImprovement =
                (1.0 - followerReadMetrics.getAvgLatencyMs() / standardReadMetrics.getAvgLatencyMs()) * 100.0;

            System.out.println("\nReadIndex throughput improvement: " +
                              String.format("%.2f%%", readIndexThroughputImprovement));
            System.out.println("ReadIndex latency improvement: " +
                              String.format("%.2f%%", readIndexLatencyImprovement));
            System.out.println("FollowerRead throughput improvement: " +
                              String.format("%.2f%%", followerReadThroughputImprovement));
            System.out.println("FollowerRead latency improvement: " +
                              String.format("%.2f%%", followerReadLatencyImprovement));

            // Shutdown
            for (KVClient client : clients) {
                client.close();
            }

            for (KVServer server : servers) {
                server.stop();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Path> setupDataDirectories() throws Exception {
        List<Path> dataPaths = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            Path dataPath = Path.of("benchmark-data/node-" + i);
            java.nio.file.Files.createDirectories(dataPath);
            dataPaths.add(dataPath);
        }
        return dataPaths;
    }

    private static List<KVServer> startCluster(List<Path> dataPaths) throws Exception {
        List<KVServer> servers = new ArrayList<>();

        // Create configurations for all nodes
        List<NodeConfig> nodeConfigs = new ArrayList<>();

        for (int i = 0; i < NUM_NODES; i++) {
            NodeConfig nodeConfig = new NodeConfig();
            nodeConfig.setNodeId("node" + i);
            nodeConfig.setHost("localhost");
            nodeConfig.setPort(BASE_PORT + i);
            nodeConfig.setDataDir(dataPaths.get(i).toString());

            nodeConfigs.add(nodeConfig);
        }

        // Create cluster configuration
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setNodes(nodeConfigs);

        // Start all servers
        for (int i = 0; i < NUM_NODES; i++) {
            NodeConfig nodeConfig = nodeConfigs.get(i);

            RaftConfig raftConfig = new RaftConfig();
            raftConfig.setElectionTimeoutMs(1000);
            raftConfig.setHeartbeatIntervalMs(200);
            // Initially disable read optimizations
            raftConfig.setReadIndexOption(false);
            raftConfig.setFollowerReadOption(false);
            raftConfig.setAsyncApply(true);

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(dataPaths.get(i).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(clusterConfig);
            server.start();

            servers.add(server);
        }

        return servers;
    }

    private static List<KVClient> createClients() throws Exception {
        List<KVClient> clients = new ArrayList<>();

        for (int i = 0; i < NUM_NODES; i++) {
            KVClient client = new KVClientImpl("localhost", BASE_PORT + i);
            client.connect();
            clients.add(client);
        }

        return clients;
    }

    private static void prepareTestData(KVClient client) throws Exception {
        byte[] value = new byte[VALUE_SIZE];
        for (int i = 0; i < VALUE_SIZE; i++) {
            value[i] = (byte)(i % 256);
        }

        // Insert the test data
        for (int i = 0; i < NUM_KEYS; i++) {
            String key = "benchmark-" + i;
            Result<Void> result = client.put(key.getBytes(), value)
                .get(5, TimeUnit.SECONDS);

            if (!result.isOk()) {
                System.err.println("Failed to insert key: " + key);
            }

            if (i % 1000 == 0) {
                System.out.println("Inserted " + i + " keys...");
            }
        }

        System.out.println("Inserted " + NUM_KEYS + " keys.");
    }

    private static void updateRaftConfig(List<KVServer> servers,
                                        boolean enableReadIndex,
                                        boolean enableFollowerRead) {
        for (KVServer server : servers) {
            RaftConfig raftConfig = server.getRaftConfig();
            raftConfig.setReadIndexOption(enableReadIndex);
            raftConfig.setFollowerReadOption(enableFollowerRead);
            server.updateRaftConfig(raftConfig);
        }
    }

    private static Metrics runStandardReadTest(List<KVClient> clients) throws Exception {
        return runReadTest(clients, "standard", false);
    }

    private static Metrics runReadIndexTest(List<KVClient> clients) throws Exception {
        return runReadTest(clients, "readindex", false);
    }

    private static Metrics runFollowerReadTest(List<KVClient> clients) throws Exception {
        return runReadTest(clients, "followerread", true);
    }

    private static Metrics runReadTest(List<KVClient> clients,
                                     String testName,
                                     boolean useAllNodes) throws Exception {
        // Find the leader for leader-only tests
        int leaderIndex = findLeaderIndex(clients);

        // For non-useAllNodes tests, we use only the leader client
        int effectiveNumClients = useAllNodes ? NUM_CLIENTS : NUM_CLIENTS / 2;

        Metrics metrics = new Metrics();
        ExecutorService executor = Executors.newFixedThreadPool(effectiveNumClients);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(effectiveNumClients);

        // Create and start client threads
        for (int i = 0; i < effectiveNumClients; i++) {
            final int clientId = i;

            executor.submit(() -> {
                try {
                    // Wait for the start signal
                    startLatch.await();

                    // Select the client
                    KVClient client;
                    if (useAllNodes) {
                        // Use all clients in round-robin for follower read
                        client = clients.get(clientId % clients.size());
                    } else {
                        // Use only the leader client for standard/readindex tests
                        client = clients.get(leaderIndex);
                    }

                    // Perform read operations
                    for (int j = 0; j < OPS_PER_CLIENT; j++) {
                        // Select a key
                        int keyIndex = (clientId * OPS_PER_CLIENT + j) % NUM_KEYS;
                        String key = "benchmark-" + keyIndex;

                        try {
                            long startNanos = System.nanoTime();
                            Result<byte[]> result = client.get(key.getBytes())
                                .get(5, TimeUnit.SECONDS);
                            long endNanos = System.nanoTime();

                            if (result.isOk() && result.getValue().length == VALUE_SIZE) {
                                metrics.recordSuccess(endNanos - startNanos);
                            } else {
                                metrics.recordFailure();
                            }
                        } catch (Exception e) {
                            metrics.recordFailure();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start the test
        metrics.start();
        startLatch.countDown();

        // Wait for completion
        completionLatch.await(10, TimeUnit.MINUTES);
        metrics.end();

        executor.shutdown();
        return metrics;
    }

    private static int findLeaderIndex(List<KVClient> clients) throws Exception {
        // Simple leader detection by trying to write a key
        String testKey = "leader-detection";
        byte[] testValue = "test".getBytes();

        for (int i = 0; i < clients.size(); i++) {
            try {
                Result<Void> result = clients.get(i).put(testKey.getBytes(), testValue)
                    .get(1, TimeUnit.SECONDS);

                if (result.isOk()) {
                    return i; // This is likely the leader
                }
            } catch (Exception e) {
                // Not the leader or failed, try next
            }
        }

        // Default to first client if we can't detect leader
        return 0;
    }
}
