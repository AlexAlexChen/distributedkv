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
 * Benchmark for write performance in different scenarios.
 * Tests AsyncApply optimization for write throughput improvement.
 */
public class WriteBenchmark {

    // Configuration
    private static final int NUM_NODES = 3;
    private static final int BASE_PORT = 20000;
    private static final int[] VALUE_SIZES = {256, 1024, 4096, 16384}; // 256B, 1KB, 4KB, 16KB
    private static final int NUM_CLIENTS = 8;
    private static final int OPS_PER_CLIENT = 500;

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
            // Run benchmarks for each value size
            for (int valueSize : VALUE_SIZES) {
                System.out.println("\n========== Benchmarking Value Size: " + valueSize + " bytes ==========");

                // Create and start the cluster without AsyncApply
                System.out.println("\n------ Standard Write Test (Sync Apply) ------");
                List<KVServer> servers = startCluster(dataPaths, false);
                List<KVClient> clients = createClients();

                // Wait for cluster to initialize
                Thread.sleep(5000);

                // Run standard write test
                Metrics standardWriteMetrics = runWriteTest(clients, valueSize);
                System.out.println(standardWriteMetrics);

                // Shutdown the cluster
                for (KVClient client : clients) {
                    client.close();
                }

                for (KVServer server : servers) {
                    server.stop();
                }

                // Create a new cluster with AsyncApply enabled
                System.out.println("\n------ Async Apply Write Test ------");
                servers = startCluster(dataPaths, true);
                clients = createClients();

                // Wait for cluster to initialize
                Thread.sleep(5000);

                // Run async apply write test
                Metrics asyncApplyMetrics = runWriteTest(clients, valueSize);
                System.out.println(asyncApplyMetrics);

                // Calculate improvement
                double throughputImprovement =
                    (asyncApplyMetrics.getThroughput() / standardWriteMetrics.getThroughput() - 1.0) * 100.0;
                double latencyImprovement =
                    (1.0 - asyncApplyMetrics.getAvgLatencyMs() / standardWriteMetrics.getAvgLatencyMs()) * 100.0;

                System.out.println("\n------ Performance Comparison ------");
                System.out.println("Standard Write: " + standardWriteMetrics.getThroughput() + " ops/sec, " +
                                  standardWriteMetrics.getAvgLatencyMs() + " ms avg latency");
                System.out.println("Async Apply: " + asyncApplyMetrics.getThroughput() + " ops/sec, " +
                                  asyncApplyMetrics.getAvgLatencyMs() + " ms avg latency");
                System.out.println("Throughput improvement: " + String.format("%.2f%%", throughputImprovement));
                System.out.println("Latency improvement: " + String.format("%.2f%%", latencyImprovement));

                // Shutdown the cluster
                for (KVClient client : clients) {
                    client.close();
                }

                for (KVServer server : servers) {
                    server.stop();
                }

                // Wait before the next test
                Thread.sleep(5000);
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

    private static List<KVServer> startCluster(List<Path> dataPaths, boolean enableAsyncApply) throws Exception {
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
            raftConfig.setAsyncApply(enableAsyncApply);

            if (enableAsyncApply) {
                raftConfig.setAsyncApplyThreads(4);
                raftConfig.setAsyncApplyBatchSize(32);
            }

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

    private static Metrics runWriteTest(List<KVClient> clients, int valueSize) throws Exception {
        // Find the leader for writes
        int leaderIndex = findLeaderIndex(clients);
        KVClient leaderClient = clients.get(leaderIndex);

        // Prepare test data
        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = (byte)(i % 256);
        }

        Metrics metrics = new Metrics();
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CLIENTS);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(NUM_CLIENTS);

        // Create and start client threads
        for (int i = 0; i < NUM_CLIENTS; i++) {
            final int clientId = i;

            executor.submit(() -> {
                try {
                    // Wait for the start signal
                    startLatch.await();

                    // Perform write operations
                    for (int j = 0; j < OPS_PER_CLIENT; j++) {
                        // Create a unique key
                        String key = "benchmark-" + valueSize + "-" + clientId + "-" + j;

                        try {
                            long startNanos = System.nanoTime();
                            Result<Void> result = leaderClient.put(key.getBytes(), value)
                                .get(10, TimeUnit.SECONDS);
                            long endNanos = System.nanoTime();

                            if (result.isOk()) {
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
        completionLatch.await(30, TimeUnit.MINUTES); // Longer timeout for larger values
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
