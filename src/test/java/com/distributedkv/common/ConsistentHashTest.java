package com.distributedkv.common;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Tests for the ConsistentHash implementation.
 */
public class ConsistentHashTest {

    private ConsistentHash<String> consistentHash;
    private final List<String> nodes = Arrays.asList("node1", "node2", "node3", "node4");

    @Before
    public void setUp() {
        // Create a new ConsistentHash with 100 virtual nodes per physical node
        consistentHash = new ConsistentHash<>(100, nodes);
    }

    @Test
    public void testGetNode() {
        // Test that keys are routed to nodes
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        // The node returned should be one of the nodes in the ring
        String node1 = consistentHash.getNode(key1);
        String node2 = consistentHash.getNode(key2);
        String node3 = consistentHash.getNode(key3);

        assertTrue("Node for key1 should be in the node list", nodes.contains(node1));
        assertTrue("Node for key2 should be in the node list", nodes.contains(node2));
        assertTrue("Node for key3 should be in the node list", nodes.contains(node3));
    }

    @Test
    public void testConsistency() {
        // Test that the same key is consistently routed to the same node
        String key = "testKey";
        String node = consistentHash.getNode(key);

        // Check that the same key maps to the same node multiple times
        for (int i = 0; i < 10; i++) {
            assertEquals("Same key should map to same node", node, consistentHash.getNode(key));
        }
    }

    @Test
    public void testNodeAddition() {
        // Test that when a node is added, only a small fraction of keys are remapped

        // First, map 1000 keys and record which node they map to
        Map<String, String> keyToNodeMap = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            keyToNodeMap.put(key, consistentHash.getNode(key));
        }

        // Add a new node
        consistentHash.addNode("node5");

        // Check how many keys got remapped
        int remappedCount = 0;
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String newNode = consistentHash.getNode(key);
            if (!newNode.equals(keyToNodeMap.get(key))) {
                remappedCount++;
            }
        }

        // Only a small fraction of keys should be remapped (ideally around 20%)
        // But we'll be generous with the upper bound to avoid flaky tests
        assertTrue("Too many keys were remapped: " + remappedCount, remappedCount < 300);

        // Some keys should be remapped (at least 10% ideally)
        assertTrue("Too few keys were remapped: " + remappedCount, remappedCount > 100);
    }

    @Test
    public void testNodeRemoval() {
        // Test that when a node is removed, only a small fraction of keys are remapped

        // First, map 1000 keys and record which node they map to
        Map<String, String> keyToNodeMap = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            keyToNodeMap.put(key, consistentHash.getNode(key));
        }

        // Remove a node
        consistentHash.removeNode("node1");

        // Count keys that were mapped to the removed node
        int nodeKeysCount = 0;
        for (String node : keyToNodeMap.values()) {
            if (node.equals("node1")) {
                nodeKeysCount++;
            }
        }

        // Count keys that got remapped
        int remappedCount = 0;
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String newNode = consistentHash.getNode(key);

            if (keyToNodeMap.get(key).equals("node1")) {
                // Keys previously mapped to node1 should now map to other nodes
                assertNotEquals("Key should not map to removed node", "node1", newNode);
                remappedCount++;
            } else if (!newNode.equals(keyToNodeMap.get(key))) {
                // Other keys should ideally not be remapped
                remappedCount++;
            }
        }

        // All keys mapped to the removed node should be remapped
        assertEquals("All keys from removed node should be remapped", nodeKeysCount, remappedCount);

        // The remapped keys should be approximately 1/4 of the total (as we removed 1 of 4 nodes)
        // But we'll be generous with the bounds to avoid flaky tests
        assertTrue("Too many keys were remapped: " + remappedCount, remappedCount < 350);
        assertTrue("Too few keys were remapped: " + remappedCount, remappedCount > 150);
    }

    @Test
    public void testDistribution() {
        // Test that keys are evenly distributed across nodes

        // Map 10000 keys and count how many go to each node
        Map<String, Integer> nodeCounts = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            String key = "key" + i;
            String node = consistentHash.getNode(key);

            nodeCounts.put(node, nodeCounts.getOrDefault(node, 0) + 1);
        }

        // Check that all nodes received some keys
        assertEquals("All nodes should have received keys", nodes.size(), nodeCounts.size());

        // Calculate expected count per node
        int expectedCount = 10000 / nodes.size();

        // Check that the distribution is reasonably even (within 15% of expected)
        for (String node : nodes) {
            int count = nodeCounts.get(node);
            double ratio = (double) count / expectedCount;

            assertTrue("Node " + node + " got too few keys: " + count, ratio > 0.85);
            assertTrue("Node " + node + " got too many keys: " + count, ratio < 1.15);
        }
    }

    @Test
    public void testEmptyRing() {
        // Test behavior with an empty ring
        ConsistentHash<String> emptyHash = new ConsistentHash<>(100, Arrays.asList());

        // Adding a node to an empty ring
        emptyHash.addNode("node1");
        assertEquals("node1", emptyHash.getNode("key"));

        // Removing the only node
        emptyHash.removeNode("node1");
        try {
            emptyHash.getNode("key");
            fail("Should throw exception when getting node from empty ring");
        } catch (IllegalStateException e) {
            // Expected behavior
        }
    }

    @Test
    public void testVirtualNodeCount() {
        // Test with different numbers of virtual nodes

        // Create hashes with 1, 10, and 1000 virtual nodes
        ConsistentHash<String> hash1 = new ConsistentHash<>(1, nodes);
        ConsistentHash<String> hash10 = new ConsistentHash<>(10, nodes);
        ConsistentHash<String> hash1000 = new ConsistentHash<>(1000, nodes);

        // Map 1000 keys with each hash
        Map<String, Integer> nodeCounts1 = new HashMap<>();
        Map<String, Integer> nodeCounts10 = new HashMap<>();
        Map<String, Integer> nodeCounts1000 = new HashMap<>();

        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;

            String node1 = hash1.getNode(key);
            String node10 = hash10.getNode(key);
            String node1000 = hash1000.getNode(key);

            nodeCounts1.put(node1, nodeCounts1.getOrDefault(node1, 0) + 1);
            nodeCounts10.put(node10, nodeCounts10.getOrDefault(node10, 0) + 1);
            nodeCounts1000.put(node1000, nodeCounts1000.getOrDefault(node1000, 0) + 1);
        }

        // Calculate standard deviation for each distribution
        double stdDev1 = calculateStdDev(nodeCounts1, 1000 / nodes.size());
        double stdDev10 = calculateStdDev(nodeCounts10, 1000 / nodes.size());
        double stdDev1000 = calculateStdDev(nodeCounts1000, 1000 / nodes.size());

        // Higher virtual node count should lead to more even distribution (lower std dev)
        assertTrue("More virtual nodes should give more even distribution",
                 stdDev1 > stdDev10 && stdDev10 > stdDev1000);
    }

    private double calculateStdDev(Map<String, Integer> counts, double mean) {
        double sum = 0;
        for (int count : counts.values()) {
            sum += Math.pow(count - mean, 2);
        }
        return Math.sqrt(sum / counts.size());
    }
}
