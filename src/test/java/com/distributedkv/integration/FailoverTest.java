package com.distributedkv.integration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Integration test for failover scenarios in a distributed KV store.
 * Tests system behavior during node failures, network partitions, and recovery.
 */
public class FailoverTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private List<KVServer> servers;
    private List<KVClient> clients;
    private List<Path> dataPaths;
    private int numNodes = 5; // Larger cluster for more complex failover scenarios
    private int basePort = 10000;
    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        servers = new ArrayList<>();
        clients = new ArrayList<>();
        dataPaths = new ArrayList<>();
        executor = Executors.newFixedThreadPool(10);

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

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(dataPaths.get(i).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

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
    public void testMinorityNodeFailure() throws Exception {
        // Insert some initial data
        int numKeys = 100;
        for (int i = 0; i < numKeys; i++) {
            String key = "minority-fail-key-" + i;
            String value = "minority-fail-value-" + i;
            clients.get(0).put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Find the leader
        int leaderIndex = findLeaderIndex();
        assertTrue("Should have identified a leader", leaderIndex >= 0);

        // Choose nodes to fail (minority of the cluster - 2 out of 5)
        List<Integer> nodesToFail = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            // Avoid failing the leader for this test
            int nodeIdx = (leaderIndex + i + 1) % numNodes;
            nodesToFail.add(nodeIdx);
        }

        // Stop the chosen nodes
        for (int nodeIdx : nodesToFail) {
            servers.get(nodeIdx).stop();
            System.out.println("Stopped node " + nodeIdx);
        }

        // Wait for the cluster to detect failures
        Thread.sleep(2000);

        // Verify the cluster can still process writes
        for (int i = 0; i < 50; i++) {
            String key = "after-minority-fail-key-" + i;
            String value = "after-minority-fail-value-" + i;

            // Use the leader client directly
            Result<Void> putResult = clients.get(leaderIndex).put(key.getBytes(), value.getBytes())
                .get(5, TimeUnit.SECONDS);

            assertTrue("Put operation should succeed with minority failure", putResult.isOk());
        }

        // Verify reads still work from all available nodes
        for (int i = 0; i < 50; i++) {
            String key = "after-minority-fail-key-" + i;
            String expectedValue = "after-minority-fail-value-" + i;

            for (int nodeIdx = 0; nodeIdx < numNodes; nodeIdx++) {
                if (nodesToFail.contains(nodeIdx)) continue; // Skip failed nodes

                Result<byte[]> getResult = clients.get(nodeIdx).get(key.getBytes())
                    .get(5, TimeUnit.SECONDS);

                assertTrue("Get operation should succeed on node " + nodeIdx, getResult.isOk());
                assertArrayEquals("Retrieved value should match on node " + nodeIdx,
                                 expectedValue.getBytes(), getResult.getValue());
            }
        }

        // Restart the failed nodes
        for (int nodeIdx : nodesToFail) {
            // Re-create the node configuration
            NodeConfig nodeConfig = new NodeConfig();
            nodeConfig.setNodeId("node" + nodeIdx);
            nodeConfig.setHost("localhost");
            nodeConfig.setPort(basePort + nodeIdx);
            nodeConfig.setDataDir(dataPaths.get(nodeIdx).toString());

            RaftConfig raftConfig = new RaftConfig();
            raftConfig.setElectionTimeoutMs(1000);
            raftConfig.setHeartbeatIntervalMs(200);
            raftConfig.setReadIndexOption(true);
            raftConfig.setFollowerReadOption(true);

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(dataPaths.get(nodeIdx).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

            // Create cluster configuration
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            for (int i = 0; i < numNodes; i++) {
                NodeConfig config = new NodeConfig();
                config.setNodeId("node" + i);
                config.setHost("localhost");
                config.setPort(basePort + i);
                nodeConfigs.add(config);
            }

            ClusterConfig clusterConfig = new ClusterConfig();
            clusterConfig.setNodes(nodeConfigs);

            // Start the node
            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(clusterConfig);
            server.start();

            servers.set(nodeIdx, server);

            // Reconnect the client
            clients.get(nodeIdx).close();
            KVClient client = new KVClientImpl("localhost", basePort + nodeIdx);
            client.connect();
            clients.set(nodeIdx, client);

            System.out.println("Restarted node " + nodeIdx);
        }

        // Wait for nodes to catch up
        Thread.sleep(5000);

        // Verify the restarted nodes have the data
        for (int nodeIdx : nodesToFail) {
            for (int i = 0; i < 50; i++) {
                String key = "after-minority-fail-key-" + i;
                String expectedValue = "after-minority-fail-value-" + i;

                // Allow a few retries as nodes might still be catching up
                Result<byte[]> getResult = null;
                for (int attempt = 0; attempt < 5; attempt++) {
                    try {
                        getResult = clients.get(nodeIdx).get(key.getBytes())
                            .get(5, TimeUnit.SECONDS);
                        if (getResult.isOk()) break;
                    } catch (Exception e) {
                        Thread.sleep(1000);
                    }
                }

                assertNotNull("Should eventually get a result from restarted node", getResult);
                assertTrue("Get operation should succeed on restarted node " + nodeIdx, getResult.isOk());
                assertArrayEquals("Retrieved value should match on restarted node " + nodeIdx,
                                 expectedValue.getBytes(), getResult.getValue());
            }
        }
    }

    @Test
    public void testMajorityNodeFailure() throws Exception {
        // Insert some initial data
        int numKeys = 100;
        for (int i = 0; i < numKeys; i++) {
            String key = "majority-fail-key-" + i;
            String value = "majority-fail-value-" + i;
            clients.get(0).put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Find the leader
        int leaderIndex = findLeaderIndex();
        assertTrue("Should have identified a leader", leaderIndex >= 0);

        // Choose nodes to fail (majority of the cluster - 3 out of 5)
        List<Integer> nodesToFail = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            int nodeIdx = (i + 1) % numNodes; // Include leader in failed nodes
            nodesToFail.add(nodeIdx);
        }

        // Stop the chosen nodes
        for (int nodeIdx : nodesToFail) {
            servers.get(nodeIdx).stop();
            System.out.println("Stopped node " + nodeIdx);
        }

        // Wait for the cluster to detect failures
        Thread.sleep(2000);

        // Try to perform a write - should fail due to no quorum
        String key = "should-fail-key";
        String value = "should-fail-value";

        boolean writeSucceeded = false;
        try {
            // Try all available clients
            for (int i = 0; i < numNodes; i++) {
                if (nodesToFail.contains(i)) continue;

                Result<Void> putResult = clients.get(i).put(key.getBytes(), value.getBytes())
                    .get(5, TimeUnit.SECONDS);

                if (putResult.isOk()) {
                    writeSucceeded = true;
                    break;
                }
            }
        } catch (Exception e) {
            // Expected timeout or failure
        }

        assertFalse("Write should fail with majority node failure", writeSucceeded);

        // Reads for existing keys might still work on surviving nodes
        for (int i = 0; i < numNodes; i++) {
            if (nodesToFail.contains(i)) continue; // Skip failed nodes

            // Try to read a pre-existing key
            String existingKey = "majority-fail-key-0";

            try {
                Result<byte[]> getResult = clients.get(i).get(existingKey.getBytes())
                    .get(5, TimeUnit.SECONDS);

                // In a ReadIndex implementation, this might fail too without quorum
                // But with FollowerRead it might succeed
                if (getResult.isOk()) {
                    String expectedValue = "majority-fail-value-0";
                    assertArrayEquals("Retrieved value should match if read succeeds",
                                     expectedValue.getBytes(), getResult.getValue());
                }
            } catch (Exception e) {
                // Expected in some implementations
            }
        }

        // Restart enough nodes to form a quorum (2 nodes, leaving 1 still down)
        for (int i = 0; i < 2; i++) {
            int nodeIdx = nodesToFail.get(i);

            // Re-create the node configuration
            NodeConfig nodeConfig = new NodeConfig();
            nodeConfig.setNodeId("node" + nodeIdx);
            nodeConfig.setHost("localhost");
            nodeConfig.setPort(basePort + nodeIdx);
            nodeConfig.setDataDir(dataPaths.get(nodeIdx).toString());

            RaftConfig raftConfig = new RaftConfig();
            raftConfig.setElectionTimeoutMs(1000);
            raftConfig.setHeartbeatIntervalMs(200);
            raftConfig.setReadIndexOption(true);
            raftConfig.setFollowerReadOption(true);

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(dataPaths.get(nodeIdx).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

            // Create cluster configuration
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            for (int j = 0; j < numNodes; j++) {
                NodeConfig config = new NodeConfig();
                config.setNodeId("node" + j);
                config.setHost("localhost");
                config.setPort(basePort + j);
                nodeConfigs.add(config);
            }

            ClusterConfig clusterConfig = new ClusterConfig();
            clusterConfig.setNodes(nodeConfigs);

            // Start the node
            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(clusterConfig);
            server.start();

            servers.set(nodeIdx, server);

            // Reconnect the client
            clients.get(nodeIdx).close();
            KVClient client = new KVClientImpl("localhost", basePort + nodeIdx);
            client.connect();
            clients.set(nodeIdx, client);

            System.out.println("Restarted node " + nodeIdx);
        }

        // Wait for cluster to recover and elect a new leader
        Thread.sleep(5000);

        // Verify the cluster can process new writes
        boolean writeSucceeded = false;

        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                // Find the new leader
                int newLeaderIndex = findLeaderIndex();
                if (newLeaderIndex >= 0) {
                    String newKey = "after-recovery-key";
                    String newValue = "after-recovery-value";

                    Result<Void> putResult = clients.get(newLeaderIndex).put(newKey.getBytes(), newValue.getBytes())
                        .get(5, TimeUnit.SECONDS);

                    if (putResult.isOk()) {
                        writeSucceeded = true;
                        break;
                    }
                }
            } catch (Exception e) {
                // Try again after a delay
                Thread.sleep(1000);
            }
        }

        assertTrue("Cluster should recover and accept writes after quorum is restored", writeSucceeded);
    }

    @Test
    public void testRollingRestartWithContinuousTraffic() throws Exception {
        // Start continuous background traffic
        AtomicBoolean stopTraffic = new AtomicBoolean(false);
        AtomicInteger successfulOps = new AtomicInteger(0);
        AtomicInteger failedOps = new AtomicInteger(0);
        ConcurrentMap<String, String> expectedValues = new ConcurrentHashMap<>();

        // Start traffic generator thread
        CompletableFuture<Void> trafficFuture = CompletableFuture.runAsync(() -> {
            int opCounter = 0;

            while (!stopTraffic.get()) {
                try {
                    // Pick a random client that's connected
                    List<Integer> availableClients = new ArrayList<>();
                    for (int i = 0; i < clients.size(); i++) {
                        try {
                            // Quick test if client is responsive
                            clients.get(i).get("test-key".getBytes())
                                .get(1, TimeUnit.SECONDS);
                            availableClients.add(i);
                        } catch (Exception e) {
                            // Skip this client
                        }
                    }

                    if (availableClients.isEmpty()) {
                        Thread.sleep(100);
                        continue;
                    }

                    int clientIdx = availableClients.get(opCounter % availableClients.size());

                    // Perform write operation
                    String key = "rolling-key-" + opCounter;
                    String value = "rolling-value-" + opCounter;

                    Result<Void> putResult = clients.get(clientIdx).put(key.getBytes(), value.getBytes())
                        .get(5, TimeUnit.SECONDS);

                    if (putResult.isOk()) {
                        expectedValues.put(key, value);
                        successfulOps.incrementAndGet();
                    } else {
                        failedOps.incrementAndGet();
                    }

                    // Occasionally perform reads to verify consistency
                    if (opCounter % 10 == 0 && !expectedValues.isEmpty()) {
                        String[] keys = expectedValues.keySet().toArray(new String[0]);
                        String readKey = keys[opCounter % keys.length];
                        String expectedValue = expectedValues.get(readKey);

                        try {
                            Result<byte[]> getResult = clients.get(clientIdx).get(readKey.getBytes())
                                .get(5, TimeUnit.SECONDS);

                            if (getResult.isOk() && Arrays.equals(expectedValue.getBytes(), getResult.getValue())) {
                                successfulOps.incrementAndGet();
                            } else {
                                failedOps.incrementAndGet();
                            }
                        } catch (Exception e) {
                            failedOps.incrementAndGet();
                        }
                    }

                    opCounter++;
                    Thread.sleep(10); // Don't overwhelm the system
                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    try { Thread.sleep(100); } catch (InterruptedException ie) {}
                }
            }
        }, executor);

        // Let traffic run for a bit
        Thread.sleep(2000);

        // Perform rolling restart of all nodes
        for (int i = 0; i < numNodes; i++) {
            System.out.println("Restarting node " + i + " in rolling restart");

            // Stop the node
            servers.get(i).stop();

            // Wait briefly
            Thread.sleep(1000);

            // Restart the node
            NodeConfig nodeConfig = new NodeConfig();
            nodeConfig.setNodeId("node" + i);
            nodeConfig.setHost("localhost");
            nodeConfig.setPort(basePort + i);
            nodeConfig.setDataDir(dataPaths.get(i).toString());

            RaftConfig raftConfig = new RaftConfig();
            raftConfig.setElectionTimeoutMs(1000);
            raftConfig.setHeartbeatIntervalMs(200);
            raftConfig.setReadIndexOption(true);
            raftConfig.setFollowerReadOption(true);

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(dataPaths.get(i).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

            // Create cluster configuration
            List<NodeConfig> nodeConfigs = new ArrayList<>();
            for (int j = 0; j < numNodes; j++) {
                NodeConfig config = new NodeConfig();
                config.setNodeId("node" + j);
                config.setHost("localhost");
                config.setPort(basePort + j);
                nodeConfigs.add(config);
            }

            ClusterConfig clusterConfig = new ClusterConfig();
            clusterConfig.setNodes(nodeConfigs);

            // Start the node
            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(clusterConfig);
            server.start();

            servers.set(i, server);

            // Reconnect the client
            clients.get(i).close();
            KVClient client = new KVClientImpl("localhost", basePort + i);
            client.connect();
            clients.set(i, client);

            // Wait for a bit before moving to the next node
            Thread.sleep(2000);
        }

        // Let traffic continue for a bit more
        Thread.sleep(2000);

        // Stop the traffic
        stopTraffic.set(true);
        trafficFuture.get(10, TimeUnit.SECONDS);

        System.out.println("Rolling restart test results:");
        System.out.println("Successful operations: " + successfulOps.get());
        System.out.println("Failed operations: " + failedOps.get());

        // Verify cluster is still functional
        int leaderIndex = findLeaderIndex();
        assertTrue("Should have a leader after rolling restart", leaderIndex >= 0);

        String finalKey = "after-rolling-restart-key";
        String finalValue = "after-rolling-restart-value";

        Result<Void> putResult = clients.get(leaderIndex).put(finalKey.getBytes(), finalValue.getBytes())
            .get(5, TimeUnit.SECONDS);

        assertTrue("Cluster should accept writes after rolling restart", putResult.isOk());

        // Verify consistent reads from all nodes
        for (int i = 0; i < numNodes; i++) {
            Result<byte[]> getResult = clients.get(i).get(finalKey.getBytes())
                .get(5, TimeUnit.SECONDS);

            assertTrue("All nodes should return consistent data after rolling restart", getResult.isOk());
            assertArrayEquals("Value should match on all nodes",
                             finalValue.getBytes(), getResult.getValue());
        }

        // Verify success rate is reasonable
        double successRate = (double) successfulOps.get() / (successfulOps.get() + failedOps.get());
        System.out.println("Success rate during rolling restart: " + (successRate * 100) + "%");

        assertTrue("Success rate should be reasonable during rolling restart", successRate > 0.7);
    }

    @Test
    public void testNetworkPartition() throws Exception {
        // This test simulates a network partition by stopping direct communication
        // between two groups of nodes while maintaining internal communication within groups

        // For simplicity, we'll simulate this by creating two separate clusters
        // In a real test, you would use network tools to create actual partitions

        // First, create some data
        int numKeys = 100;
        for (int i = 0; i < numKeys; i++) {
            String key = "partition-key-" + i;
            String value = "partition-value-" + i;
            clients.get(0).put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Shutdown the existing cluster
        tearDown();

        // Create two separate clusters with the same configuration
        int partitionSize = 3; // First partition with 3 nodes (majority)

        // Setup first partition (nodes 0, 1, 2)
        List<KVServer> partition1Servers = new ArrayList<>();
        List<KVClient> partition1Clients = new ArrayList<>();
        List<Path> partition1Paths = new ArrayList<>();
        List<NodeConfig> partition1Configs = new ArrayList<>();

        for (int i = 0; i < partitionSize; i++) {
            Path dataPath = tempFolder.newFolder("partition1-node-" + i).toPath();
            partition1Paths.add(dataPath);

            NodeConfig nodeConfig = new NodeConfig();
            nodeConfig.setNodeId("node" + i);
            nodeConfig.setHost("localhost");
            nodeConfig.setPort(basePort + i);
            nodeConfig.setDataDir(dataPath.toString());

            partition1Configs.add(nodeConfig);
        }

        // Create cluster config for first partition
        ClusterConfig partition1ClusterConfig = new ClusterConfig();
        partition1ClusterConfig.setNodes(partition1Configs);

        // Start servers in first partition
        for (int i = 0; i < partitionSize; i++) {
            NodeConfig nodeConfig = partition1Configs.get(i);

            RaftConfig raftConfig = new RaftConfig();
            raftConfig.setElectionTimeoutMs(1000);
            raftConfig.setHeartbeatIntervalMs(200);

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(partition1Paths.get(i).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(partition1ClusterConfig);
            server.start();

            partition1Servers.add(server);

            // Create client
            KVClient client = new KVClientImpl("localhost", basePort + i);
            client.connect();
            partition1Clients.add(client);
        }

        // Setup second partition (nodes 3, 4) - minority
        List<KVServer> partition2Servers = new ArrayList<>();
        List<KVClient> partition2Clients = new ArrayList<>();
        List<Path> partition2Paths = new ArrayList<>();
        List<NodeConfig> partition2Configs = new ArrayList<>();

        for (int i = 0; i < numNodes - partitionSize; i++) {
            Path dataPath = tempFolder.newFolder("partition2-node-" + i).toPath();
            partition2Paths.add(dataPath);

            NodeConfig nodeConfig = new NodeConfig();
            nodeConfig.setNodeId("node" + (i + partitionSize));
            nodeConfig.setHost("localhost");
            nodeConfig.setPort(basePort + i + partitionSize);
            nodeConfig.setDataDir(dataPath.toString());

            partition2Configs.add(nodeConfig);
        }

        // Create cluster config for second partition
        ClusterConfig partition2ClusterConfig = new ClusterConfig();
        partition2ClusterConfig.setNodes(partition2Configs);

        // Start servers in second partition
        for (int i = 0; i < numNodes - partitionSize; i++) {
            NodeConfig nodeConfig = partition2Configs.get(i);

            RaftConfig raftConfig = new RaftConfig();
            raftConfig.setElectionTimeoutMs(1000);
            raftConfig.setHeartbeatIntervalMs(200);

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(partition2Paths.get(i).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(partition2ClusterConfig);
            server.start();

            partition2Servers.add(server);

            // Create client
            KVClient client = new KVClientImpl("localhost", basePort + i + partitionSize);
            client.connect();
            partition2Clients.add(client);
        }

        // Wait for both partitions to initialize
        Thread.sleep(5000);

        // Insert data in the first partition (which should have a quorum)
        String key1 = "partition1-key";
        String value1 = "partition1-value";

        boolean partition1WriteSucceeded = false;
        for (KVClient client : partition1Clients) {
            try {
                Result<Void> putResult = client.put(key1.getBytes(), value1.getBytes())
                    .get(5, TimeUnit.SECONDS);

                if (putResult.isOk()) {
                    partition1WriteSucceeded = true;
                    break;
                }
            } catch (Exception e) {
                // Try next client
            }
        }

        assertTrue("First partition (majority) should accept writes", partition1WriteSucceeded);

        // Try to insert data in the second partition (which should not have a quorum)
        String key2 = "partition2-key";
        String value2 = "partition2-value";

        boolean partition2WriteSucceeded = false;
        for (KVClient client : partition2Clients) {
            try {
                Result<Void> putResult = client.put(key2.getBytes(), value2.getBytes())
                    .get(5, TimeUnit.SECONDS);

                if (putResult.isOk()) {
                    partition2WriteSucceeded = true;
                    break;
                }
            } catch (Exception e) {
                // Expected timeout or failure
            }
        }

        assertFalse("Second partition (minority) should not accept writes", partition2WriteSucceeded);

        // Verify data is available in the first partition
        boolean readSucceeded = false;
        for (KVClient client : partition1Clients) {
            try {
                Result<byte[]> getResult = client.get(key1.getBytes())
                    .get(5, TimeUnit.SECONDS);

                if (getResult.isOk() && Arrays.equals(value1.getBytes(), getResult.getValue())) {
                    readSucceeded = true;
                    break;
                }
            } catch (Exception e) {
                // Try next client
            }
        }

        assertTrue("Data should be available in the first partition", readSucceeded);

        // Clean up
        for (KVClient client : partition1Clients) {
            client.close();
        }

        for (KVClient client : partition2Clients) {
            client.close();
        }

        for (KVServer server : partition1Servers) {
            server.stop();
        }

        for (KVServer server : partition2Servers) {
            server.stop();
        }

        // Recreate the original cluster for proper tearDown
        servers = new ArrayList<>();
        clients = new ArrayList<>();
        dataPaths = new ArrayList<>();
    }

    private int findLeaderIndex() throws Exception {
        for (int i = 0; i < servers.size(); i++) {
            if (servers.get(i) != null && servers.get(i).isLeader()) {
                return i;
            }
        }
        return -1;
    }
}
