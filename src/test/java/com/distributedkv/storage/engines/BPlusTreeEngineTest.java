package com.distributedkv.storage.engines;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.StorageConfig;
import com.distributedkv.storage.engines.btree.BPlusTree;
import com.distributedkv.storage.engines.btree.BPlusTreeEngine;
import com.distributedkv.storage.engines.btree.BPlusTreeNode;

public class BPlusTreeEngineTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private BPlusTreeEngine engine;
    private Path dbPath;
    private StorageConfig config;

    @Before
    public void setUp() throws Exception {
        // Create temporary directory for the test database
        dbPath = tempFolder.newFolder("bplus-tree-test").toPath();

        // Setup storage configuration
        config = new StorageConfig();
        config.setDbPath(dbPath.toString());
        config.setBTreeOrder(4); // Small order for testing
        config.setBTreeCacheSize(100);

        // Initialize the B+ Tree engine
        engine = new BPlusTreeEngine(config);
        engine.open();
    }

    @After
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }
    }

    @Test
    public void testBasicPutGet() throws Exception {
        // Define test data
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        // Put the key-value pair
        engine.put(key, value);

        // Get the value back
        byte[] retrievedValue = engine.get(key);

        // Verify the value matches
        assertNotNull("Retrieved value should not be null", retrievedValue);
        assertArrayEquals("Retrieved value should match", value, retrievedValue);
    }

    @Test
    public void testGetNonExistentKey() throws Exception {
        // Try to get a key that doesn't exist
        byte[] key = "non-existent-key".getBytes();
        byte[] retrievedValue = engine.get(key);

        // Verify the value is null
        assertNull("Retrieved value should be null for non-existent key", retrievedValue);
    }

    @Test
    public void testDelete() throws Exception {
        // Define test data
        byte[] key = "delete-test-key".getBytes();
        byte[] value = "delete-test-value".getBytes();

        // Put the key-value pair
        engine.put(key, value);

        // Verify it exists
        byte[] retrievedValue = engine.get(key);
        assertNotNull("Value should exist before deletion", retrievedValue);

        // Delete the key
        engine.delete(key);

        // Verify it's gone
        retrievedValue = engine.get(key);
        assertNull("Value should be null after deletion", retrievedValue);
    }

    @Test
    public void testMultipleInsertions() throws Exception {
        // Define multiple key-value pairs
        int count = 100;
        byte[][] keys = new byte[count][];
        byte[][] values = new byte[count][];

        for (int i = 0; i < count; i++) {
            keys[i] = ("multi-key-" + i).getBytes();
            values[i] = ("multi-value-" + i).getBytes();
        }

        // Insert all pairs
        for (int i = 0; i < count; i++) {
            engine.put(keys[i], values[i]);
        }

        // Verify all pairs exist
        for (int i = 0; i < count; i++) {
            byte[] retrievedValue = engine.get(keys[i]);
            assertNotNull("Retrieved value should not be null", retrievedValue);
            assertArrayEquals("Retrieved value should match for index " + i, values[i], retrievedValue);
        }
    }

    @Test
    public void testTreeSplitting() throws Exception {
        // This test will cause the B+ tree to split nodes
        // by inserting keys in sequence beyond the node capacity

        // Get the underlying B+ tree for verification
        BPlusTree tree = engine.getTree();

        // Insert enough keys to cause splits
        int count = 20; // More than enough to cause splits with order 4

        for (int i = 0; i < count; i++) {
            byte[] key = ("split-key-" + String.format("%02d", i)).getBytes();
            byte[] value = ("split-value-" + i).getBytes();
            engine.put(key, value);
        }

        // Verify the tree height has increased beyond 1 (root only)
        assertTrue("Tree should have split and increased height", tree.getHeight() > 1);

        // Verify all values are still accessible
        for (int i = 0; i < count; i++) {
            byte[] key = ("split-key-" + String.format("%02d", i)).getBytes();
            byte[] expectedValue = ("split-value-" + i).getBytes();

            byte[] retrievedValue = engine.get(key);
            assertNotNull("Retrieved value should not be null after splits", retrievedValue);
            assertArrayEquals("Retrieved value should match after splits", expectedValue, retrievedValue);
        }
    }

    @Test
    public void testRangeQuery() throws Exception {
        // Define test data for range query
        int count = 10;
        byte[][] keys = new byte[count][];
        byte[][] values = new byte[count][];

        for (int i = 0; i < count; i++) {
            // Use formatted string to ensure lexicographic ordering
            keys[i] = ("range-key-" + String.format("%02d", i)).getBytes();
            values[i] = ("range-value-" + i).getBytes();
            engine.put(keys[i], values[i]);
        }

        // Define range: from key-02 to key-07 (inclusive)
        byte[] startKey = "range-key-02".getBytes();
        byte[] endKey = "range-key-07".getBytes();

        // Execute range query
        List<Map.Entry<byte[], byte[]>> results = engine.getRange(startKey, endKey);

        // Verify results
        assertEquals("Range query should return correct number of entries", 6, results.size());

        // Verify entries are in correct order with correct values
        for (int i = 0; i < results.size(); i++) {
            Map.Entry<byte[], byte[]> entry = results.get(i);

            // Expected key and value for this position
            byte[] expectedKey = keys[i + 2]; // Start from index 2
            byte[] expectedValue = values[i + 2];

            assertArrayEquals("Key should match at position " + i, expectedKey, entry.getKey());
            assertArrayEquals("Value should match at position " + i, expectedValue, entry.getValue());
        }
    }

    @Test
    public void testPrefixScan() throws Exception {
        // Define test data with a common prefix
        String prefix = "prefix-";
        byte[] prefixBytes = prefix.getBytes();

        // Insert keys with and without the prefix
        engine.put((prefix + "key1").getBytes(), "value1".getBytes());
        engine.put((prefix + "key2").getBytes(), "value2".getBytes());
        engine.put((prefix + "key3").getBytes(), "value3".getBytes());
        engine.put("other-key".getBytes(), "other-value".getBytes());

        // Scan with the prefix
        List<Map.Entry<byte[], byte[]>> results = engine.getPrefixRange(prefixBytes);

        // Verify results
        assertEquals("Prefix scan should return correct number of entries", 3, results.size());

        // Verify all entries have the prefix
        for (Map.Entry<byte[], byte[]> entry : results) {
            String keyString = new String(entry.getKey());
            assertTrue("Key should start with prefix", keyString.startsWith(prefix));
        }
    }

    @Test
    public void testIteratorTraversal() throws Exception {
        // Insert ordered data
        int count = 10;

        for (int i = 0; i < count; i++) {
            String keyStr = "iter-key-" + String.format("%02d", i);
            String valueStr = "iter-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Use iterator to traverse all entries
        List<Map.Entry<byte[], byte[]>> iterResults = new ArrayList<>();

        engine.iterate((key, value) -> {
            // Only collect keys with our test prefix
            if (new String(key).startsWith("iter-key-")) {
                iterResults.add(Map.entry(key.clone(), value.clone()));
            }
            return true; // Continue iteration
        });

        // Verify results
        assertEquals("Iterator should return all matching entries", count, iterResults.size());

        // Verify entries are in correct order
        for (int i = 0; i < count; i++) {
            Map.Entry<byte[], byte[]> entry = iterResults.get(i);

            String expectedKeyStr = "iter-key-" + String.format("%02d", i);
            String expectedValueStr = "iter-value-" + i;

            assertArrayEquals("Key should match at position " + i,
                             expectedKeyStr.getBytes(), entry.getKey());
            assertArrayEquals("Value should match at position " + i,
                             expectedValueStr.getBytes(), entry.getValue());
        }
    }

    @Test
    public void testUpdateExistingKey() throws Exception {
        // Define test data
        byte[] key = "update-key".getBytes();
        byte[] value1 = "original-value".getBytes();
        byte[] value2 = "updated-value".getBytes();

        // Put initial value
        engine.put(key, value1);

        // Verify initial value
        byte[] retrievedValue = engine.get(key);
        assertArrayEquals("Retrieved value should match initial value", value1, retrievedValue);

        // Update with new value
        engine.put(key, value2);

        // Verify updated value
        retrievedValue = engine.get(key);
        assertArrayEquals("Retrieved value should match updated value", value2, retrievedValue);
    }

    @Test
    public void testBatchOperations() throws Exception {
        // Define test data
        List<Map.Entry<byte[], byte[]>> putEntries = new ArrayList<>();
        List<byte[]> deleteKeys = new ArrayList<>();

        // Add some entries to put
        putEntries.add(Map.entry("batch-key1".getBytes(), "batch-value1".getBytes()));
        putEntries.add(Map.entry("batch-key2".getBytes(), "batch-value2".getBytes()));
        putEntries.add(Map.entry("batch-key3".getBytes(), "batch-value3".getBytes()));

        // Add some pre-existing keys to delete
        byte[] deleteKey1 = "delete-key1".getBytes();
        byte[] deleteKey2 = "delete-key2".getBytes();

        engine.put(deleteKey1, "to-be-deleted1".getBytes());
        engine.put(deleteKey2, "to-be-deleted2".getBytes());

        deleteKeys.add(deleteKey1);
        deleteKeys.add(deleteKey2);

        // Execute batch operations
        engine.executeBatch(putEntries, deleteKeys);

        // Verify puts succeeded
        for (Map.Entry<byte[], byte[]> entry : putEntries) {
            byte[] retrievedValue = engine.get(entry.getKey());
            assertNotNull("Value should exist after batch put", retrievedValue);
            assertArrayEquals("Retrieved value should match", entry.getValue(), retrievedValue);
        }

        // Verify deletes succeeded
        for (byte[] key : deleteKeys) {
            byte[] retrievedValue = engine.get(key);
            assertNull("Value should be null after batch delete", retrievedValue);
        }
    }

    @Test
    public void testPersistenceAndRecovery() throws Exception {
        // Insert some data
        for (int i = 0; i < 20; i++) {
            String keyStr = "persist-key-" + i;
            String valueStr = "persist-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Flush to disk
        engine.flush();

        // Close the engine
        engine.close();

        // Reopen the engine (should recover from disk)
        engine = new BPlusTreeEngine(config);
        engine.open();

        // Verify data still exists
        for (int i =ra = 0; i < 20; i++) {
            String keyStr = "persist-key-" + i;
            String expectedValueStr = "persist-value-" + i;

            byte[] retrievedValue = engine.get(keyStr.getBytes());
            assertNotNull("Value should exist after recovery", retrievedValue);
            assertArrayEquals("Retrieved value should match after recovery",
                             expectedValueStr.getBytes(), retrievedValue);
        }
    }

    @Test
    public void testConcurrentOperations() throws Exception {
        // Test concurrent reads and writes
        int numThreads = 10;
        int opsPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CompletableFuture<Void>[] futures = new CompletableFuture[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            futures[i] = CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        String keyStr = "thread-" + threadId + "-key-" + j;
                        String valueStr = "thread-" + threadId + "-value-" + j;

                        byte[] key = keyStr.getBytes();
                        byte[] value = valueStr.getBytes();

                        // Write
                        engine.put(key, value);

                        // Read and verify
                        byte[] retrievedValue = engine.get(key);
                        assertArrayEquals("Retrieved value should match in thread " + threadId,
                                         value, retrievedValue);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
        }

        // Wait for all threads to complete
        CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify all keys exist
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < opsPerThread; j++) {
                String keyStr = "thread-" + i + "-key-" + j;
                String valueStr = "thread-" + i + "-value-" + j;

                byte[] key = keyStr.getBytes();
                byte[] expectedValue = valueStr.getBytes();

                byte[] retrievedValue = engine.get(key);
                assertArrayEquals("All values should be retrievable after concurrent operations",
                                 expectedValue, retrievedValue);
            }
        }
    }

    @Test
    public void testTreeBalance() throws Exception {
        // Insert and delete keys to test tree rebalancing

        // First, insert keys in ascending order
        for (int i = 0; i < 20; i++) {
            String keyStr = "balance-key-" + String.format("%02d", i);
            String valueStr = "balance-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Get tree height after inserts
        int heightAfterInserts = engine.getTree().getHeight();

        // Delete some keys to trigger rebalancing
        for (int i = 5; i < 15; i++) {
            String keyStr = "balance-key-" + String.format("%02d", i);
            engine.delete(keyStr.getBytes());
        }

        // Verify tree is still functional
        for (int i = 0; i < 5; i++) {
            String keyStr = "balance-key-" + String.format("%02d", i);
            String expectedValueStr = "balance-value-" + i;

            byte[] retrievedValue = engine.get(keyStr.getBytes());
            assertNotNull("Value should exist after rebalancing", retrievedValue);
            assertArrayEquals("Retrieved value should match after rebalancing",
                             expectedValueStr.getBytes(), retrievedValue);
        }

        for (int i = 15; i < 20; i++) {
            String keyStr = "balance-key-" + String.format("%02d", i);
            String expectedValueStr = "balance-value-" + i;

            byte[] retrievedValue = engine.get(keyStr.getBytes());
            assertNotNull("Value should exist after rebalancing", retrievedValue);
            assertArrayEquals("Retrieved value should match after rebalancing",
                             expectedValueStr.getBytes(), retrievedValue);
        }

        // Verify deleted keys are gone
        for (int i = 5; i < 15; i++) {
            String keyStr = "balance-key-" + String.format("%02d", i);
            byte[] retrievedValue = engine.get(keyStr.getBytes());
            assertNull("Deleted value should be null after rebalancing", retrievedValue);
        }
    }
}
