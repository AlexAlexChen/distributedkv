package com.distributedkv.integration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
 * Integration test for a single-node KV server deployment.
 * Tests basic operations without the complexity of a cluster.
 */
public class SingleNodeTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private KVServer server;
    private KVClient client;
    private Path dataPath;

    @Before
    public void setUp() throws Exception {
        // Create temporary directory for the test data
        dataPath = tempFolder.newFolder("single-node-test").toPath();

        // Setup configurations
        NodeConfig nodeConfig = new NodeConfig();
        nodeConfig.setNodeId("node1");
        nodeConfig.setHost("localhost");
        nodeConfig.setPort(10000);
        nodeConfig.setDataDir(dataPath.toString());

        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutMs(500);
        raftConfig.setHeartbeatIntervalMs(100);
        raftConfig.setSnapshotThreshold(1000);
        raftConfig.setApplyBatchSize(32);

        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setDbPath(dataPath.resolve("db").toString());
        storageConfig.setEngine("rocksdb"); // Use RocksDB for this test

        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setNodes(List.of(nodeConfig)); // Single node cluster

        // Initialize and start the server
        server = new KVServer(nodeConfig, raftConfig, storageConfig);
        server.start();

        // Initialize the client
        client = new KVClientImpl("localhost", 10000);
        client.connect();

        // Wait for server to fully initialize
        Thread.sleep(1000);
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }

        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testBasicPutGet() throws Exception {
        // Define test data
        String key = "test-key";
        String value = "test-value";

        // Put the key-value pair
        Result<Void> putResult = client.put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);
        assertTrue("Put operation should succeed", putResult.isOk());

        // Get the value back
        Result<byte[]> getResult = client.get(key.getBytes()).get(5, TimeUnit.SECONDS);
        assertTrue("Get operation should succeed", getResult.isOk());

        // Verify the value matches
        assertArrayEquals("Retrieved value should match", value.getBytes(), getResult.getValue());
    }

    @Test
    public void testDelete() throws Exception {
        // Define test data
        String key = "delete-test-key";
        String value = "delete-test-value";

        // Put the key-value pair
        client.put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);

        // Verify it exists
        Result<byte[]> getResult = client.get(key.getBytes()).get(5, TimeUnit.SECONDS);
        assertTrue("Key should exist before deletion", getResult.isOk());

        // Delete the key
        Result<Void> deleteResult = client.delete(key.getBytes()).get(5, TimeUnit.SECONDS);
        assertTrue("Delete operation should succeed", deleteResult.isOk());

        // Verify it's gone
        getResult = client.get(key.getBytes()).get(5, TimeUnit.SECONDS);
        assertFalse("Key should not exist after deletion", getResult.isOk());
        assertEquals("Get should return KEY_NOT_FOUND status", Status.KEY_NOT_FOUND().getCode(), getResult.getStatus().getCode());
    }

    @Test
    public void testMultipleOperations() throws Exception {
        // Define test data
        int count = 100;
        List<CompletableFuture<Result<Void>>> putFutures = new ArrayList<>();

        // Put multiple key-value pairs
        for (int i = 0; i < count; i++) {
            String key = "multi-key-" + i;
            String value = "multi-value-" + i;
            putFutures.add(client.put(key.getBytes(), value.getBytes()));
        }

        // Wait for all puts to complete
        for (CompletableFuture<Result<Void>> future : putFutures) {
            Result<Void> result = future.get(10, TimeUnit.SECONDS);
            assertTrue("Put operation should succeed", result.isOk());
        }

        // Get and verify each value
        for (int i = 0; i < count; i++) {
            String key = "multi-key-" + i;
            String expectedValue = "multi-value-" + i;

            Result<byte[]> getResult = client.get(key.getBytes()).get(5, TimeUnit.SECONDS);
            assertTrue("Get operation should succeed for key " + key, getResult.isOk());
            assertArrayEquals("Retrieved value should match for key " + key,
                             expectedValue.getBytes(), getResult.getValue());
        }
    }

    @Test
    public void testConcurrentOperations() throws Exception {
        // Define test data
        int numThreads = 10;
        int opsPerThread = 50;

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        String key = "thread-" + threadId + "-key-" + j;
                        String value = "thread-" + threadId + "-value-" + j;

                        // Put
                        Result<Void> putResult = client.put(key.getBytes(), value.getBytes())
                            .get(5, TimeUnit.SECONDS);
                        assertTrue("Put operation should succeed", putResult.isOk());

                        // Get and verify
                        Result<byte[]> getResult = client.get(key.getBytes())
                            .get(5, TimeUnit.SECONDS);
                        assertTrue("Get operation should succeed", getResult.isOk());
                        assertArrayEquals("Retrieved value should match",
                                         value.getBytes(), getResult.getValue());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Wait for all operations to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .get(30, TimeUnit.SECONDS);

        // Verify all keys exist
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < opsPerThread; j++) {
                String key = "thread-" + i + "-key-" + j;
                String expectedValue = "thread-" + i + "-value-" + j;

                Result<byte[]> getResult = client.get(key.getBytes())
                    .get(5, TimeUnit.SECONDS);
                assertTrue("Key should exist after concurrent operations: " + key,
                          getResult.isOk());
                assertArrayEquals("Retrieved value should match for key " + key,
                                 expectedValue.getBytes(), getResult.getValue());
            }
        }
    }

    @Test
    public void testServerRestart() throws Exception {
        // Define test data
        String key = "restart-test-key";
        String value = "restart-test-value";

        // Put the key-value pair
        client.put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);

        // Restart the server
        server.stop();
        Thread.sleep(1000); // Wait for server to fully stop

        // Start a new server instance with the same configuration
        NodeConfig nodeConfig = new NodeConfig();
        nodeConfig.setNodeId("node1");
        nodeConfig.setHost("localhost");
        nodeConfig.setPort(10000);
        nodeConfig.setDataDir(dataPath.toString());

        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutMs(500);
        raftConfig.setHeartbeatIntervalMs(100);

        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setDbPath(dataPath.resolve("db").toString());
        storageConfig.setEngine("rocksdb");

        server = new KVServer(nodeConfig, raftConfig, storageConfig);
        server.start();

        // Reconnect the client
        client.close();
        client = new KVClientImpl("localhost", 10000);
        client.connect();

        // Wait for server to fully initialize
        Thread.sleep(1000);

        // Verify the data persisted
        Result<byte[]> getResult = client.get(key.getBytes()).get(5, TimeUnit.SECONDS);
        assertTrue("Data should persist after server restart", getResult.isOk());
        assertArrayEquals("Retrieved value should match after restart",
                         value.getBytes(), getResult.getValue());
    }

    @Test
    public void testLargeValueSupport() throws Exception {
        // Define test data with a large value
        String key = "large-value-key";
        byte[] largeValue = new byte[1024 * 1024]; // 1MB

        // Fill with a pattern
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte)(i % 256);
        }

        // Put the large value
        Result<Void> putResult = client.put(key.getBytes(), largeValue)
            .get(10, TimeUnit.SECONDS);
        assertTrue("Put operation should succeed for large value", putResult.isOk());

        // Get the value back
        Result<byte[]> getResult = client.get(key.getBytes())
            .get(10, TimeUnit.SECONDS);
        assertTrue("Get operation should succeed for large value", getResult.isOk());

        // Verify the value matches
        assertArrayEquals("Retrieved large value should match",
                         largeValue, getResult.getValue());
    }

    @Test
    public void testReadPerformance() throws Exception {
        // Prepare data
        int count = 1000;
        for (int i = 0; i < count; i++) {
            String key = "perf-key-" + i;
            String value = "perf-value-" + i;
            client.put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Measure read performance
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            String key = "perf-key-" + i;
            Result<byte[]> getResult = client.get(key.getBytes())
                .get(5, TimeUnit.SECONDS);
            assertTrue("Get operation should succeed", getResult.isOk());
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        double avgLatency = (double) duration / count;
        System.out.println("Average read latency: " + avgLatency + "ms");

        // We don't assert on exact performance numbers as they vary by system,
        // but we can check that performance is reasonable
        assertTrue("Read latency should be reasonable", avgLatency < 50);
    }

    @Test
    public void testWritePerformance() throws Exception {
        // Measure write performance
        int count = 1000;
        long startTime = System.currentTimeMillis();

        List<CompletableFuture<Result<Void>>> futures = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            String key = "write-perf-key-" + i;
            String value = "write-perf-value-" + i;
            futures.add(client.put(key.getBytes(), value.getBytes()));
        }

        // Wait for all writes to complete
        for (CompletableFuture<Result<Void>> future : futures) {
            Result<Void> result = future.get(10, TimeUnit.SECONDS);
            assertTrue("Put operation should succeed", result.isOk());
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        double avgLatency = (double) duration / count;
        System.out.println("Average write latency: " + avgLatency + "ms");

        // Check that performance is reasonable
        assertTrue("Write latency should be reasonable", avgLatency < 50);
    }

    @Test
    public void testNonExistentKey() throws Exception {
        // Try to get a key that doesn't exist
        String key = "non-existent-key";

        Result<byte[]> getResult = client.get(key.getBytes()).get(5, TimeUnit.SECONDS);

        // The result should not be OK
        assertFalse("Result should not be OK for non-existent key", getResult.isOk());

        // Status should be KEY_NOT_FOUND
        assertEquals("Status should be KEY_NOT_FOUND",
                    Status.KEY_NOT_FOUND().getCode(), getResult.getStatus().getCode());
    }
}
