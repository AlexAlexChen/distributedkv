package com.distributedkv.integration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.ClusterConfig;
import com.distributedkv.config.NodeConfig;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.config.StorageConfig;
import com.distributedkv.network.client.KVClient;
import com.distributedkv.network.client.KVClientImpl;
import com.distributedkv.server.KVServer;

/**
 * Performance tests for the distributed KV store.
 * Tests throughput, latency, and scalability under various workloads.
 */
public class PerformanceTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private List<KVServer> servers;
    private List<KVClient> clients;
    private List<Path> dataPaths;
    private int numNodes = 3;
    private int basePort = 10000;
    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        servers = new ArrayList<>();
        clients = new ArrayList<>();
        dataPaths = new ArrayList<>();
        executor = Executors.newFixedThreadPool(32); // Large thread pool for performance tests

        // Create configurations for all nodes
        List<NodeConfig> nodeConfigs = new ArrayList<>();

        for (int i = 0; i < numNodes; i++) {
            Path dataPath = tempFolder.newFolder("node-" + i).toPath();
            dataPaths.add(dataPath);

            NodeConfig nodeConfig = new NodeConfig();
            nodeConfig.setNodeId("node" + i);
            nodeConfig.setHost("localhost");
            nodeConfig.setPort(basePort + i);
            nodeConfig.setDataDir(dataPath.toString());

            nodeConfigs.add(nodeConfig);
        }

        // Create cluster configuration
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setNodes(nodeConfigs);

        // Start all servers
        for (int i = 0; i < numNodes; i++) {
            NodeConfig nodeConfig = nodeConfigs.get(i);

            RaftConfig raftConfig = new RaftConfig();
            raftConfig.setElectionTimeoutMs(1000);
            raftConfig.setHeartbeatIntervalMs(200);
            raftConfig.setSnapshotThreshold(1000);
            raftConfig.setApplyBatchSize(32);
            // Enable read optimizations
            raftConfig.setReadIndexOption(true);
            raftConfig.setFollowerReadOption(true);
            // Enable async apply for better write performance
            raftConfig.setAsyncApply(true);
            raftConfig.setAsyncApplyThreads(4);

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(dataPaths.get(i).resolve("db").toString());
            storageConfig.setEngine("rocksdb"); // Use RocksDB for best performance

            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(clusterConfig);
            server.start();

            servers.add(server);

            // Create a client for each node
            KVClient client = new KVClientImpl("localhost", basePort + i);
            client.connect();
            clients.add(client);
        }

        // Wait for cluster to initialize and elect a leader
        Thread.sleep(5000);
    }

    @After
    public void tearDown() throws Exception {
        for (KVClient client : clients) {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }

        for (KVServer server : servers) {
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }

        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testWriteThroughput() throws Exception {
        // Number of concurrent clients
        int numClients = 10;
        // Operations per client
        int opsPerClient = 1000;
        // Value size
        int valueSize = 1024; // 1KB

        // Prepare test data
        byte[][] values = new byte[opsPerClient][valueSize];
        for (int i = 0; i < opsPerClient; i++) {
            values[i] = new byte[valueSize];
            // Fill with some pattern
            for (int j = 0; j < valueSize; j++) {
                values[i][j] = (byte)((i + j) % 256);
            }
        }

        // Start throughput measurement
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numClients);
        AtomicLong totalLatencyNanos = new AtomicLong(0);

        // Create and start client threads
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;

            executor.submit(() -> {
                try {
                    // Wait for the start signal
                    startLatch.await();

                    // Get a client - distribute across available servers
                    KVClient client = clients.get(clientId % clients.size());

                    // Perform write operations
                    for (int j = 0; j < opsPerClient; j++) {
                        String key = "perf-write-" + clientId + "-" + j;
                        byte[] value = values[j];

                        long startNanos = System.nanoTime();
                        Result<Void> result = client.put(key.getBytes(), value)
                            .get(5, TimeUnit.SECONDS);
                        long endNanos = System.nanoTime();

                        assertTrue("Write operation should succeed", result.isOk());

                        // Accumulate latency
                        totalLatencyNanos.addAndGet(endNanos - startNanos);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start the test (all threads start simultaneously)
        long startTime = System.currentTimeMillis();
        startLatch.countDown();

        // Wait for all operations to complete
        completionLatch.await(5, TimeUnit.MINUTES);
        long endTime = System.currentTimeMillis();

        // Calculate results
        long totalTime = endTime - startTime;
        int totalOperations = numClients * opsPerClient;
        double operationsPerSecond = (double) totalOperations / (totalTime / 1000.0);
        double avgLatencyMillis = (double) totalLatencyNanos.get() / (totalOperations * 1_000_000.0);

        System.out.println("Write Performance Results:");
        System.out.println("Total operations: " + totalOperations);
        System.out.println("Total time: " + totalTime + "ms");
        System.out.println("Throughput: " + operationsPerSecond + " ops/sec");
        System.out.println("Average latency: " + avgLatencyMillis + "ms");

        // Verify minimal performance expectations
        assertTrue("Throughput should meet minimum expectations", operationsPerSecond > 1000);
        assertTrue("Latency should be reasonable", avgLatencyMillis < 50);
    }

    @Test
    public void testReadThroughput() throws Exception {
        // First insert some data
        int numKeys = 10000;
        int valueSize = 1024; // 1KB

        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = (byte)(i % 256);
        }

        // Find the leader for consistent writes
        int leaderIndex = findLeaderIndex();
        KVClient leaderClient = clients.get(leaderIndex);

        System.out.println("Preparing test data...");

        // Insert the test data
        for (int i = 0; i < numKeys; i++) {
            String key = "perf-read-" + i;
            Result<Void> result = leaderClient.put(key.getBytes(), value)
                .get(5, TimeUnit.SECONDS);

            assertTrue("Write operation should succeed during setup", result.isOk());

            if (i % 1000 == 0) {
                System.out.println("Inserted " + i + " keys...");
            }
        }

        System.out.println("Test data preparation complete.");

        // Number of concurrent clients
        int numClients = 10;
        // Operations per client
        int opsPerClient = 1000;

        // Start throughput measurement
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numClients);
        AtomicLong totalLatencyNanos = new AtomicLong(0);

        // Create and start client threads
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;

            executor.submit(() -> {
                try {
                    // Wait for the start signal
                    startLatch.await();

                    // Get a client - distribute across available servers to test FollowerRead
                    KVClient client = clients.get(clientId % clients.size());

                    // Perform read operations
                    for (int j = 0; j < opsPerClient; j++) {
                        // Randomly select a key from the pre-inserted data
                        int keyIndex = (clientId * opsPerClient + j) % numKeys;
                        String key = "perf-read-" + keyIndex;

                        long startNanos = System.nanoTime();
                        Result<byte[]> result = client.get(key.getBytes())
                            .get(5, TimeUnit.SECONDS);
                        long endNanos = System.nanoTime();

                        assertTrue("Read operation should succeed", result.isOk());
                        assertEquals("Read should return correct value size", valueSize, result.getValue().length);

                        // Accumulate latency
                        totalLatencyNanos.addAndGet(endNanos - startNanos);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start the test (all threads start simultaneously)
        long startTime = System.currentTimeMillis();
        startLatch.countDown();

        // Wait for all operations to complete
        completionLatch.await(5, TimeUnit.MINUTES);
        long endTime = System.currentTimeMillis();

        // Calculate results
        long totalTime = endTime - startTime;
        int totalOperations = numClients * opsPerClient;
        double operationsPerSecond = (double) totalOperations / (totalTime / 1000.0);
        double avgLatencyMillis = (double) totalLatencyNanos.get() / (totalOperations * 1_000_000.0);

        System.out.println("Read Performance Results:");
        System.out.println("Total operations: " + totalOperations);
        System.out.println("Total time: " + totalTime + "ms");
        System.out.println("Throughput: " + operationsPerSecond + " ops/sec");
        System.out.println("Average latency: " + avgLatencyMillis + "ms");

        // Verify minimal performance expectations
        assertTrue("Read throughput should meet minimum expectations", operationsPerSecond > 2000);
        assertTrue("Read latency should be reasonable", avgLatencyMillis < 20);
    }

    @Test
    public void testMixedWorkload() throws Exception {
        // First insert some initial data
        int numInitialKeys = 5000;
        int valueSize = 1024; // 1KB

        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = (byte)(i % 256);
        }

        // Find the leader for consistent writes
        int leaderIndex = findLeaderIndex();
        KVClient leaderClient = clients.get(leaderIndex);

        System.out.println("Preparing initial test data...");

        // Insert the initial test data
        for (int i = 0; i < numInitialKeys; i++) {
            String key = "mixed-" + i;
            Result<Void> result = leaderClient.put(key.getBytes(), value)
                .get(5, TimeUnit.SECONDS);

            assertTrue("Write operation should succeed during setup", result.isOk());

            if (i % 1000 == 0) {
                System.out.println("Inserted " + i + " keys...");
            }
        }

        System.out.println("Initial test data preparation complete.");

        // Number of concurrent clients
        int numClients = 20;
        // Operations per client
        int opsPerClient = 500;
        // Read/write ratio (80% reads, 20% writes)
        double readRatio = 0.8;

        // Start throughput measurement
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numClients);
        AtomicLong totalReadLatencyNanos = new AtomicLong(0);
        AtomicLong totalWriteLatencyNanos = new AtomicLong(0);
        AtomicLong readCount = new AtomicLong(0);
        AtomicLong writeCount = new AtomicLong(0);

        // Create and start client threads
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;

            executor.submit(() -> {
                try {
                    // Wait for the start signal
                    startLatch.await();

                    // Get a client - distribute across available servers
                    KVClient client = clients.get(clientId % clients.size());

                    // Perform mixed operations
                    for (int j = 0; j < opsPerClient; j++) {
                        // Determine if this is a read or write based on the ratio
                        boolean isRead = Math.random() < readRatio;

                        if (isRead) {
                            // Read operation - select a random key from initial data
                            int keyIndex = (int)(Math.random() * numInitialKeys);
                            String key = "mixed-" + keyIndex;

                            long startNanos = System.nanoTime();
                            Result<byte[]> result = client.get(key.getBytes())
                                .get(5, TimeUnit.SECONDS);
                            long endNanos = System.nanoTime();

                            assertTrue("Read operation should succeed", result.isOk());
                            assertEquals("Read should return correct value size", valueSize,
                                       result.getValue().length);

                            // Accumulate read latency
                            totalReadLatencyNanos.addAndGet(endNanos - startNanos);
                            readCount.incrementAndGet();
                        } else {
                            // Write operation - use a unique key
                            String key = "mixed-write-" + clientId + "-" + j;

                            long startNanos = System.nanoTime();
                            Result<Void> result = client.put(key.getBytes(), value)
                                .get(5, TimeUnit.SECONDS);
                            long endNanos = System.nanoTime();

                            assertTrue("Write operation should succeed", result.isOk());

                            // Accumulate write latency
                            totalWriteLatencyNanos.addAndGet(endNanos - startNanos);
                            writeCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start the test (all threads start simultaneously)
        long startTime = System.currentTimeMillis();
        startLatch.countDown();

        // Wait for all operations to complete
        completionLatch.await(5, TimeUnit.MINUTES);
        long endTime = System.currentTimeMillis();

        // Calculate results
        long totalTime = endTime - startTime;
        long totalReadOps = readCount.get();
        long totalWriteOps = writeCount.get();
        int totalOperations = (int)(totalReadOps + totalWriteOps);

        double totalOpsPerSecond = (double) totalOperations / (totalTime / 1000.0);
        double readOpsPerSecond = (double) totalReadOps / (totalTime / 1000.0);
        double writeOpsPerSecond = (double) totalWriteOps / (totalTime / 1000.0);

        double avgReadLatencyMillis = totalReadOps > 0 ?
            (double) totalReadLatencyNanos.get() / (totalReadOps * 1_000_000.0) : 0;
        double avgWriteLatencyMillis = totalWriteOps > 0 ?
            (double) totalWriteLatencyNanos.get() / (totalWriteOps * 1_000_000.0) : 0;

        System.out.println("Mixed Workload Performance Results:");
        System.out.println("Total operations: " + totalOperations +
                          " (" + totalReadOps + " reads, " + totalWriteOps + " writes)");
        System.out.println("Total time: " + totalTime + "ms");
        System.out.println("Overall throughput: " + totalOpsPerSecond + " ops/sec");
        System.out.println("Read throughput: " + readOpsPerSecond + " reads/sec");
        System.out.println("Write throughput: " + writeOpsPerSecond + " writes/sec");
        System.out.println("Average read latency: " + avgReadLatencyMillis + "ms");
        System.out.println("Average write latency: " + avgWriteLatencyMillis + "ms");

        // Verify minimal performance expectations
        assertTrue("Overall throughput should meet minimum expectations", totalOpsPerSecond > 1500);
        assertTrue("Read latency should be reasonable", avgReadLatencyMillis < 20);
        assertTrue("Write latency should be reasonable", avgWriteLatencyMillis < 50);
    }

    @Test
    public void testLinearScaling() throws Exception {
        // This test measures how performance scales with increasing cluster size
        // We'll simulate this by using an increasing number of clients against our fixed-size cluster

        int valueSize = 1024; // 1KB
        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = (byte)(i % 256);
        }

        // Test with different client counts to simulate increased load
        int[] clientCounts = {1, 2, 4, 8, 16};
        int opsPerClient = 500;

        // Store results for analysis
        double[] throughputResults = new double[clientCounts.length];

        for (int testIdx = 0; testIdx < clientCounts.length; testIdx++) {
            int numClients = clientCounts[testIdx];
            System.out.println("\nRunning scaling test with " + numClients + " clients");

            // Start throughput measurement
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(numClients);

            // Create and start client threads
            for (int i = 0; i < numClients; i++) {
                final int clientId = i;

                executor.submit(() -> {
                    try {
                        // Wait for the start signal
                        startLatch.await();

                        // Get a client - distribute across available servers
                        KVClient client = clients.get(clientId % clients.size());

                        // Perform write operations
                        for (int j = 0; j < opsPerClient; j++) {
                            String key = "scale-" + testIdx + "-" + clientId + "-" + j;

                            Result<Void> result = client.put(key.getBytes(), value)
                                .get(5, TimeUnit.SECONDS);

                            assertTrue("Write operation should succeed", result.isOk());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }

            // Start the test (all threads start simultaneously)
            long startTime = System.currentTimeMillis();
            startLatch.countDown();

            // Wait for all operations to complete
            completionLatch.await(5, TimeUnit.MINUTES);
            long endTime = System.currentTimeMillis();

            // Calculate results
            long totalTime = endTime - startTime;
            int totalOperations = numClients * opsPerClient;
            double operationsPerSecond = (double) totalOperations / (totalTime / 1000.0);

            System.out.println("Throughput with " + numClients + " clients: " +
                              operationsPerSecond + " ops/sec");

            throughputResults[testIdx] = operationsPerSecond;

            // Brief pause between tests
            Thread.sleep(5000);
        }

        // Analyze scaling efficiency
        System.out.println("\nScaling Analysis:");
        for (int i = 1; i < clientCounts.length; i++) {
            int clientRatio = clientCounts[i] / clientCounts[0];
            double throughputRatio = throughputResults[i] / throughputResults[0];
            double scalingEfficiency = throughputRatio / clientRatio * 100.0;

            System.out.println(clientCounts[i] + " clients: " +
                              scalingEfficiency + "% scaling efficiency");

            // A perfect linear scaling would be 100% efficiency
            // In practice, we expect some degradation with higher load
            if (clientCounts[i] <= 8) {
                // Expect reasonable scaling up to 8x clients
                assertTrue("Scaling efficiency should be reasonable for " + clientCounts[i] + " clients",
                          scalingEfficiency > 50.0);
            }
        }
    }

    @Test
    public void testHighConcurrency() throws Exception {
        // Test behavior under very high concurrency
        int numClients = 50;
        int opsPerClient = 100;
        int valueSize = 512;

        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = (byte)(i % 256);
        }

        // Start concurrency test
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numClients);

        // Track statistics
        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failureCount = new AtomicLong(0);
        AtomicLong timeoutCount = new AtomicLong(0);

        // Create and start client threads
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;

            executor.submit(() -> {
                try {
                    // Wait for the start signal
                    startLatch.await();

                    // Get a client - distribute across available servers
                    KVClient client = clients.get(clientId % clients.size());

                    // Perform operations
                    for (int j = 0; j < opsPerClient; j++) {
                        String key = "concurrent-" + clientId + "-" + j;

                        try {
                            Result<Void> result = client.put(key.getBytes(), value)
                                .get(2, TimeUnit.SECONDS); // Shorter timeout to detect congestion

                            if (result.isOk()) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        } catch (java.util.concurrent.TimeoutException e) {
                            timeoutCount.incrementAndGet();
                        } catch (Exception e) {
                            failureCount.incrementAndGet();
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
        long startTime = System.currentTimeMillis();
        startLatch.countDown();

        // Wait for all operations to complete
        completionLatch.await(10, TimeUnit.MINUTES);
        long endTime = System.currentTimeMillis();

        // Calculate results
        long totalTime = endTime - startTime;
        long totalAttempts = numClients * opsPerClient;
        long totalSuccesses = successCount.get();
        long totalFailures = failureCount.get();
        long totalTimeouts = timeoutCount.get();

        double successRate = (double) totalSuccesses / totalAttempts * 100.0;
        double throughput = (double) totalSuccesses / (totalTime / 1000.0);

        System.out.println("High Concurrency Test Results:");
        System.out.println("Total operations attempted: " + totalAttempts);
        System.out.println("Successful operations: " + totalSuccesses +
                          " (" + successRate + "% success rate)");
        System.out.println("Failed operations: " + totalFailures);
        System.out.println("Timeouts: " + totalTimeouts);
        System.out.println("Total time: " + totalTime + "ms");
        System.out.println("Throughput: " + throughput + " ops/sec");

        // Verify the system maintained reasonable stability under high load
        assertTrue("Success rate should be reasonable under high concurrency",
                  successRate > 90.0);
        assertTrue("Throughput should be reasonable under high concurrency",
                  throughput > 500.0);
    }

    private int findLeaderIndex() throws Exception {
        for (int i = 0; i < servers.size(); i++) {
            if (servers.get(i).isLeader()) {
                return i;
            }
        }
        return -1;
    }
}
