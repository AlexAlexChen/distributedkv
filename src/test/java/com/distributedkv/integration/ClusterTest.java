package com.distributedkv.integration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.distributedkv.server.ClusterManager;

/**
 * Integration test for a multi-node KV server cluster.
 * Tests distributed operations and consensus.
 */
public class ClusterTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private List<KVServer> servers;
    private KVClient client;
    private List<Path> dataPaths;
    private int numNodes = 3;
    private int basePort = 10000;

    @Before
    public void setUp() throws Exception {
        servers = new ArrayList<>();
        dataPaths = new ArrayList<>();

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

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(dataPaths.get(i).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(clusterConfig);
            server.start();

            servers.add(server);
        }

        // Wait for cluster to initialize and elect a leader
        Thread.sleep(5000);

        // Initialize the client connected to the first node
        client = new KVClientImpl("localhost", basePort);
        client.connect();
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }

        // Stop all servers
        for (KVServer server : servers) {
            if (server != null) {
                server.stop();
            }
        }
    }

    @Test
    public void testBasicOperationsWithConsensus() throws Exception {
        // Define test data
        String key = "consensus-key";
        String value = "consensus-value";

        // Put the key-value pair
        Result<Void> putResult = client.put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);
        assertTrue("Put operation should succeed", putResult.isOk());

        // Get the value from each node to verify consensus
        for (int i = 0; i < numNodes; i++) {
            // Create a client connected to this node
            try (KVClient nodeClient = new KVClientImpl("localhost", basePort + i)) {
                nodeClient.connect();

                // Get the value
                Result<byte[]> getResult = nodeClient.get(key.getBytes()).get(5, TimeUnit.SECONDS);
                assertTrue("Get operation should succeed on node " + i, getResult.isOk());

                // Verify the value matches
                assertArrayEquals("Retrieved value should match on node " + i,
                                 value.getBytes(), getResult.getValue());
            }
        }
    }

    @Test
    public void testNodeFailure() throws Exception {
        // Define test data
        String key = "failure-key";
        String value = "failure-value";

        // Put the key-value pair
        client.put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);

        // Determine which node is the leader
        int leaderIndex = findLeaderIndex();
        assertTrue("Should have identified a leader", leaderIndex >= 0);

        // Shutdown a follower node (not the leader)
        int followerIndex = (leaderIndex + 1) % numNodes;
        servers.get(followerIndex).stop();

        // Wait for the cluster to detect the failure
        Thread.sleep(2000);

        // Put a new key-value pair
        String newKey = "after-failure-key";
        String newValue = "after-failure-value";
        Result<Void> putResult = client.put(newKey.getBytes(), newValue.getBytes()).get(5, TimeUnit.SECONDS);
        assertTrue("Put operation should succeed after follower failure", putResult.isOk());

        // Get the value from each remaining node to verify consensus
        for (int i = 0; i < numNodes; i++) {
            if (i == followerIndex) continue; // Skip the failed node

            // Create a client connected to this node
            try (KVClient nodeClient = new KVClientImpl("localhost", basePort + i)) {
                nodeClient.connect();

                // Get the value
                Result<byte[]> getResult = nodeClient.get(newKey.getBytes()).get(5, TimeUnit.SECONDS);
                assertTrue("Get operation should succeed on node " + i, getResult.isOk());

                // Verify the value matches
                assertArrayEquals("Retrieved value should match on node " + i,
                                 newValue.getBytes(), getResult.getValue());
            }
        }

        // Restart the failed node
        NodeConfig nodeConfig = new NodeConfig();
        nodeConfig.setNodeId("node" + followerIndex);
        nodeConfig.setHost("localhost");
        nodeConfig.setPort(basePort + followerIndex);
        nodeConfig.setDataDir(dataPaths.get(followerIndex).toString());

        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutMs(1000);
        raftConfig.setHeartbeatIntervalMs(200);

        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setDbPath(dataPaths.get(followerIndex).resolve("db").toString());
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

        KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
        server.setClusterConfig(clusterConfig);
        server.start();

        servers.set(followerIndex, server);

        // Wait for the node to catch up
        Thread.sleep(5000);

        // Verify the restarted node has the data
        try (KVClient nodeClient = new KVClientImpl("localhost", basePort + followerIndex)) {
            nodeClient.connect();

            // Get the value (allowing a few retries as the node might still be catching up)
            Result<byte[]> getResult = null;
            for (int attempt = 0; attempt < 5; attempt++) {
                getResult = nodeClient.get(newKey.getBytes()).get(5, TimeUnit.SECONDS);
                if (getResult.isOk()) break;
                Thread.sleep(1000);
            }

            assertTrue("Get operation should eventually succeed on restarted node", getResult.isOk());
            assertArrayEquals("Retrieved value should match on restarted node",
                             newValue.getBytes(), getResult.getValue());
        }
    }

    @Test
    public void testLeaderFailure() throws Exception {
        // Define test data
        String key = "leader-failure-key";
        String value = "leader-failure-value";

        // Put the key-value pair
        client.put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);

        // Determine which node is the leader
        int leaderIndex = findLeaderIndex();
        assertTrue("Should have identified a leader", leaderIndex >= 0);

        // Create a client connected to a follower
        int followerIndex = (leaderIndex + 1) % numNodes;
        KVClient followerClient = new KVClientImpl("localhost", basePort + followerIndex);
        followerClient.connect();

        // Shutdown the leader node
        servers.get(leaderIndex).stop();

        // Wait for a new leader to be elected
        Thread.sleep(5000);

        // Put a new key-value pair through the follower client
        String newKey = "after-leader-failure-key";
        String newValue = "after-leader-failure-value";

        // This might take a few attempts as the election and leadership transfer occurs
        Result<Void> putResult = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                putResult = followerClient.put(newKey.getBytes(), newValue.getBytes())
                    .get(5, TimeUnit.SECONDS);
                if (putResult.isOk()) break;
            } catch (Exception e) {
                // Retry on timeout or other exceptions
                Thread.sleep(1000);
            }
        }

        assertNotNull("Put operation should eventually succeed after leader failure", putResult);
        assertTrue("Put operation should eventually succeed after leader failure", putResult.isOk());

        // Get the value from each remaining node to verify consensus
        for (int i = 0; i < numNodes; i++) {
            if (i == leaderIndex) continue; // Skip the failed node

            // Create a client connected to this node
            try (KVClient nodeClient = new KVClientImpl("localhost", basePort + i)) {
                nodeClient.connect();

                // Get the value
                Result<byte[]> getResult = nodeClient.get(newKey.getBytes()).get(5, TimeUnit.SECONDS);
                assertTrue("Get operation should succeed on node " + i, getResult.isOk());

                // Verify the value matches
                assertArrayEquals("Retrieved value should match on node " + i,
                                 newValue.getBytes(), getResult.getValue());
            }
        }

        followerClient.close();
    }

    @Test
    public void testReadScalability() throws Exception {
        // Insert some initial data
        int numKeys = 1000;
        for (int i = 0; i < numKeys; i++) {
            String key = "read-scale-key-" + i;
            String value = "read-scale-value-" + i;
            client.put(key.getBytes(), value.getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Create clients for each node
        List<KVClient> clients = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            KVClient nodeClient = new KVClientImpl("localhost", basePort + i);
            nodeClient.connect();
            clients.add(nodeClient);
        }

        // Set up read workload
        int readsPerClient = 200;
        long totalTimeMs = 0;

        // Perform reads from all clients concurrently
        List<CompletableFuture<Long>> readFutures = new ArrayList<>();

        for (KVClient nodeClient : clients) {
            readFutures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    long startTime = System.currentTimeMillis();

                    for (int i = 0; i < readsPerClient; i++) {
                        int keyIdx = i % numKeys;
                        String key = "read-scale-key-" + keyIdx;
                        String expectedValue = "read-scale-value-" + keyIdx;

                        Result<byte[]> getResult = nodeClient.get(key.getBytes())
                            .get(5, TimeUnit.SECONDS);

                        assertTrue("Read should succeed", getResult.isOk());
                        assertArrayEquals("Value should match",
                                         expectedValue.getBytes(), getResult.getValue());
                    }

                    long endTime = System.currentTimeMillis();
                    return endTime - startTime;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Gather results
        for (CompletableFuture<Long> future : readFutures) {
            totalTimeMs += future.get(60, TimeUnit.SECONDS);
        }

        // Calculate metrics
        double avgLatency = (double) totalTimeMs / (numNodes * readsPerClient);
        double throughput = (numNodes * readsPerClient * 1000.0) / totalTimeMs;

        System.out.println("Read scalability test:");
        System.out.println("Average read latency: " + avgLatency + "ms");
        System.out.println("Read throughput: " + throughput + " ops/sec");

        // Close the clients
        for (KVClient nodeClient : clients) {
            nodeClient.close();
        }

        // A reasonable expectation for read performance with follower reads
        assertTrue("Read throughput should be reasonable", throughput > 100.0);
    }

    @Test
    public void testWriteScalability() throws Exception {
        // Create clients for each node
        List<KVClient> clients = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            KVClient nodeClient = new KVClientImpl("localhost", basePort + i);
            nodeClient.connect();
            clients.add(nodeClient);
        }

        // Set up write workload
        int writesPerClient = 100;
        long totalTimeMs = 0;

        // Perform writes from all clients concurrently
        List<CompletableFuture<Long>> writeFutures = new ArrayList<>();

        for (int clientIdx = 0; clientIdx < clients.size(); clientIdx++) {
            final int idx = clientIdx;
            KVClient nodeClient = clients.get(idx);

            writeFutures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    long startTime = System.currentTimeMillis();

                    for (int i = 0; i < writesPerClient; i++) {
                        String key = "write-scale-key-" + idx + "-" + i;
                        String value = "write-scale-value-" + idx + "-" + i;

                        Result<Void> putResult = nodeClient.put(key.getBytes(), value.getBytes())
                            .get(5, TimeUnit.SECONDS);

                        assertTrue("Write should succeed", putResult.isOk());
                    }

                    long endTime = System.currentTimeMillis();
                    return endTime - startTime;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Gather results
        for (CompletableFuture<Long> future : writeFutures) {
            totalTimeMs += future.get(60, TimeUnit.SECONDS);
        }

        // Calculate metrics
        double avgLatency = (double) totalTimeMs / (numNodes * writesPerClient);
        double throughput = (numNodes * writesPerClient * 1000.0) / totalTimeMs;

        System.out.println("Write scalability test:");
        System.out.println("Average write latency: " + avgLatency + "ms");
        System.out.println("Write throughput: " + throughput + " ops/sec");

        // Verify data consistency
        for (int clientIdx = 0; clientIdx < clients.size(); clientIdx++) {
            for (int i = 0; i < writesPerClient; i++) {
                String key = "write-scale-key-" + clientIdx + "-" + i;
                String expectedValue = "write-scale-value-" + clientIdx + "-" + i;

                Result<byte[]> getResult = client.get(key.getBytes())
                    .get(5, TimeUnit.SECONDS);

                assertTrue("All writes should be consistently available", getResult.isOk());
                assertArrayEquals("Value should match",
                                 expectedValue.getBytes(), getResult.getValue());
            }
        }

        // Close the clients
        for (KVClient nodeClient : clients) {
            nodeClient.close();
        }
    }

    @Test
    public void testDataShardingAndMigration() throws Exception {
        // This test is simpler if we create a fresh cluster with sharding enabled
        // Cleanup existing servers
        tearDown();

        // Configure a new cluster with sharding
        numNodes = 3;
        servers = new ArrayList<>();
        dataPaths = new ArrayList<>();

        // Create configurations for all nodes
        List<NodeConfig> nodeConfigs = new ArrayList<>();

        for (int i = 0; i < numNodes; i++) {
            Path dataPath = tempFolder.newFolder("shard-node-" + i).toPath();
            dataPaths.add(dataPath);

            NodeConfig nodeConfig = new NodeConfig();
            nodeConfig.setNodeId("node" + i);
            nodeConfig.setHost("localhost");
            nodeConfig.setPort(basePort + i);
            nodeConfig.setDataDir(dataPath.toString());

            nodeConfigs.add(nodeConfig);
        }

        // Create cluster configuration with sharding
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setNodes(nodeConfigs);
        clusterConfig.setEnableSharding(true);
        clusterConfig.setNumShards(6); // 2 shards per node initially

        // Start all servers
        for (int i = 0; i < numNodes; i++) {
            NodeConfig nodeConfig = nodeConfigs.get(i);

            RaftConfig raftConfig = new RaftConfig();
            raftConfig.setElectionTimeoutMs(1000);
            raftConfig.setHeartbeatIntervalMs(200);

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setDbPath(dataPaths.get(i).resolve("db").toString());
            storageConfig.setEngine("rocksdb");

            KVServer server = new KVServer(nodeConfig, raftConfig, storageConfig);
            server.setClusterConfig(clusterConfig);
            server.start();

            servers.add(server);
        }

        // Wait for cluster to initialize
        Thread.sleep(5000);

        // Initialize the client
        client = new KVClientImpl("localhost", basePort);
        client.connect();

        // Insert data that should be distributed across shards
        int keysPerShard = 50;
        int totalKeys = keysPerShard * clusterConfig.getNumShards();

        for (int i = 0; i < totalKeys; i++) {
            String key = "shard-key-" + i;
            String value = "shard-value-" + i;

            Result<Void> putResult = client.put(key.getBytes(), value.getBytes())
                .get(5, TimeUnit.SECONDS);
            assertTrue("Put operation should succeed", putResult.isOk());
        }

        // Verify data distribution across nodes
        int[] keysPerNode = new int[numNodes];

        for (int i = 0; i < totalKeys; i++) {
            String key = "shard-key-" + i;

            // Try to find the key on each node
            for (int nodeIdx = 0; nodeIdx < numNodes; nodeIdx++) {
                try (KVClient nodeClient = new KVClientImpl("localhost", basePort + nodeIdx)) {
                    nodeClient.connect();

                    Result<byte[]> getResult = nodeClient.get(key.getBytes())
                        .get(1, TimeUnit.SECONDS);

                    if (getResult.isOk()) {
                        keysPerNode[nodeIdx]++;
                        String expectedValue = "shard-value-" + i;
                        assertArrayEquals("Value should match",
                                         expectedValue.getBytes(), getResult.getValue());
                    }
                } catch (Exception e) {
                    // Key not found on this node is acceptable
                }
            }
        }

        // Data should be somewhat evenly distributed
        for (int i = 0; i < numNodes; i++) {
            System.out.println("Node " + i + " has " + keysPerNode[i] + " keys");
            assertTrue("Each node should have a reasonable number of keys",
                      keysPerNode[i] > totalKeys / (numNodes * 2));
        }

        // Now add a new node to trigger shard migration
        Path newDataPath = tempFolder.newFolder("shard-node-3").toPath();
        dataPaths.add(newDataPath);

        NodeConfig newNodeConfig = new NodeConfig();
        newNodeConfig.setNodeId("node3");
        newNodeConfig.setHost("localhost");
        newNodeConfig.setPort(basePort + 3);
        newNodeConfig.setDataDir(newDataPath.toString());

        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutMs(1000);
        raftConfig.setHeartbeatIntervalMs(200);

        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setDbPath(newDataPath.resolve("db").toString());
        storageConfig.setEngine("rocksdb");

        // Update cluster configuration to include the new node
        nodeConfigs.add(newNodeConfig);
        clusterConfig.setNodes(nodeConfigs);

        // Start the new node
        KVServer newServer = new KVServer(newNodeConfig, raftConfig, storageConfig);
        newServer.setClusterConfig(clusterConfig);
        newServer.start();

        servers.add(newServer);

        // First, notify the cluster of the new node
        for (KVServer server : servers) {
            server.getClusterManager().updateClusterConfig(clusterConfig);
        }

        // Wait for shard rebalancing
        Thread.sleep(10000);

        // Verify the data has been redistributed
        int[] newKeysPerNode = new int[numNodes + 1];

        for (int i = 0; i < totalKeys; i++) {
            String key = "shard-key-" + i;

            // Try to find the key on each node
            for (int nodeIdx = 0; nodeIdx < numNodes + 1; nodeIdx++) {
                try (KVClient nodeClient = new KVClientImpl("localhost", basePort + nodeIdx)) {
                    nodeClient.connect();

                    Result<byte[]> getResult = nodeClient.get(key.getBytes())
                        .get(1, TimeUnit.SECONDS);

                    if (getResult.isOk()) {
                        newKeysPerNode[nodeIdx]++;
                        String expectedValue = "shard-value-" + i;
                        assertArrayEquals("Value should match after redistribution",
                                         expectedValue.getBytes(), getResult.getValue());
                    }
                } catch (Exception e) {
                    // Key not found on this node is acceptable
                }
            }
        }

        // Data should now include the new node
        for (int i = 0; i < numNodes + 1; i++) {
            System.out.println("Node " + i + " has " + newKeysPerNode[i] + " keys after rebalancing");
            assertTrue("New node should have received some keys", newKeysPerNode[numNodes] > 0);
        }

        // Verify all keys are still accessible from any node
        for (int i = 0; i < totalKeys; i++) {
            String key = "shard-key-" + i;
            String expectedValue = "shard-value-" + i;

            Result<byte[]> getResult = client.get(key.getBytes())
                .get(5, TimeUnit.SECONDS);

            assertTrue("All keys should be accessible after rebalancing", getResult.isOk());
            assertArrayEquals("Value should match after rebalancing",
                             expectedValue.getBytes(), getResult.getValue());
        }
    }

    private int findLeaderIndex() throws Exception {
        for (int i = 0; i < numNodes; i++) {
            boolean isLeader = servers.get(i).isLeader();
            if (isLeader) {
                return i;
            }
        }
        return -1;
    }
}
