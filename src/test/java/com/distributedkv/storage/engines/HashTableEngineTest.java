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
import com.distributedkv.storage.engines.hashtable.HashTableEngine;
import com.distributedkv.storage.engines.hashtable.ConcurrentHashMap;

public class HashTableEngineTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private HashTableEngine engine;
    private Path dbPath;
    private StorageConfig config;

    @Before
    public void setUp() throws Exception {
        // Create temporary directory for the test database
        dbPath = tempFolder.newFolder("hashtable-test").toPath();

        // Setup storage configuration
        config = new StorageConfig();
        config.setDbPath(dbPath.toString());
        config.setInitialCapacity(1024);
        config.setLoadFactor(0.75f);
        config.setConcurrencyLevel(16);

        // Initialize the HashTable engine
        engine = new HashTableEngine(config);
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
        int count = 1000;
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
    public void testHashCollisions() throws Exception {
        // Create keys with the same hash code but different content
        // This is a simplified way to simulate collisions

        // First, create a subclass of byte[] that overrides hashCode
        class CollidingKey extends byte[] {
            private final int forcedHash;

            public CollidingKey(byte[] original, int forcedHash) {
                super();
                System.arraycopy(original, 0, this, 0, original.length);
                this.forcedHash = forcedHash;
            }

            @Override
            public int hashCode() {
                return forcedHash;
            }
        }

        // Create keys with forced hash collision
        byte[] key1 = new CollidingKey("collision-key1".getBytes(), 12345);
        byte[] key2 = new CollidingKey("collision-key2".getBytes(), 12345);
        byte[] key3 = new CollidingKey("collision-key3".getBytes(), 12345);

        byte[] value1 = "collision-value1".getBytes();
        byte[] value2 = "collision-value2".getBytes();
        byte[] value3 = "collision-value3".getBytes();

        // Insert all pairs
        engine.put(key1, value1);
        engine.put(key2, value2);
        engine.put(key3, value3);

        // Verify all pairs are retrievable despite hash collisions
        byte[] retrievedValue1 = engine.get(key1);
        byte[] retrievedValue2 = engine.get(key2);
        byte[] retrievedValue3 = engine.get(key3);

        assertArrayEquals("Value for key1 should match despite collisions", value1, retrievedValue1);
        assertArrayEquals("Value for key2 should match despite collisions", value2, retrievedValue2);
        assertArrayEquals("Value for key3 should match despite collisions", value3, retrievedValue3);
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
    public void testIterateAllEntries() throws Exception {
        // Insert some entries
        int count = 100;
        for (int i = 0; i < count; i++) {
            String keyStr = "iter-key-" + i;
            String valueStr = "iter-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Use iterator to collect all entries
        List<Map.Entry<byte[], byte[]>> collectedEntries = new ArrayList<>();

        engine.iterate((key, value) -> {
            // Only collect keys with our test prefix
            if (new String(key).startsWith("iter-key-")) {
                collectedEntries.add(Map.entry(key.clone(), value.clone()));
            }
            return true; // Continue iteration
        });

        // Verify we collected the expected number of entries
        assertEquals("Iterator should return all inserted entries", count, collectedEntries.size());

        // Verify each entry is present with the correct value
        for (Map.Entry<byte[], byte[]> entry : collectedEntries) {
            String keyStr = new String(entry.getKey());
            String valueStr = new String(entry.getValue());

            // Extract the index from the key
            int index = Integer.parseInt(keyStr.substring("iter-key-".length()));

            // Verify the value matches
            assertEquals("Value should match the key index",
                        "iter-value-" + index, valueStr);
        }
    }

    @Test
    public void testPartialIteration() throws Exception {
        // Insert some entries
        int count = 100;
        for (int i = 0; i < count; i++) {
            String keyStr = "partial-key-" + i;
            String valueStr = "partial-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Use iterator but stop after 50 entries
        final int[] counter = new int[1];
        counter[0] = 0;

        engine.iterate((key, value) -> {
            // Only count keys with our test prefix
            if (new String(key).startsWith("partial-key-")) {
                counter[0]++;

                // Stop iteration after 50 entries
                return counter[0] < 50;
            }
            return true;
        });

        // Verify we processed the expected number of entries
        assertEquals("Iterator should process exactly 50 entries", 50, counter[0]);
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
        engine = new HashTableEngine(config);
        engine.open();

        // Verify data still exists
        for (int i = 0; i < 20; i++) {
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
    public void testResizing() throws Exception {
        // Get the underlying hash map for verification
        ConcurrentHashMap<byte[], byte[]> map = engine.getMap();

        // Initial size
        int initialCapacity = map.size();

        // Insert many entries to trigger resizing
        int largeCount = 10000; // Should trigger resizing with default settings

        for (int i = 0; i < largeCount; i++) {
            String keyStr = "resize-key-" + i;
            String valueStr = "resize-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Current size should be larger after resize
        assertTrue("Map should have resized", map.size() > initialCapacity);

        // Verify all entries are still accessible
        for (int i = 0; i < largeCount; i++) {
            String keyStr = "resize-key-" + i;
            String expectedValueStr = "resize-value-" + i;

            byte[] retrievedValue = engine.get(keyStr.getBytes());
            assertNotNull("Value should be accessible after resize", retrievedValue);
            assertArrayEquals("Retrieved value should match after resize",
                             expectedValueStr.getBytes(), retrievedValue);
        }
    }

    @Test
    public void testKeyWithZeroBytes() throws Exception {
        // Test keys with zero bytes in them
        byte[] key = new byte[] { 0, 1, 0, 2, 0 };
        byte[] value = "zero-byte-value".getBytes();

        engine.put(key, value);

        byte[] retrievedValue = engine.get(key);
        assertNotNull("Value should be retrievable with zero-byte key", retrievedValue);
        assertArrayEquals("Retrieved value should match", value, retrievedValue);
    }

    @Test
    public void testLargeValues() throws Exception {
        // Test storing and retrieving large values
        byte[] key = "large-value-key".getBytes();

        // Create a large value (1MB)
        int size = 1024 * 1024;
        byte[] largeValue = new byte[size];

        // Fill with a pattern
        for (int i = 0; i < size; i++) {
            largeValue[i] = (byte)(i % 256);
        }

        // Store large value
        engine.put(key, largeValue);

        // Retrieve and verify
        byte[] retrievedValue = engine.get(key);
        assertNotNull("Large value should be retrievable", retrievedValue);
        assertEquals("Retrieved large value should have correct size", size, retrievedValue.length);

        // Verify the pattern
        for (int i = 0; i < size; i++) {
            assertEquals("Byte at position " + i + " should match",
                        (byte)(i % 256), retrievedValue[i]);
        }
    }

    @Test
    public void testNullValue() throws Exception {
        // Test storing null value (should be treated as delete)
        byte[] key = "null-value-key".getBytes();
        byte[] initialValue = "initial-value".getBytes();

        // Store initial value
        engine.put(key, initialValue);

        // Verify it exists
        byte[] retrievedValue = engine.get(key);
        assertNotNull("Initial value should exist", retrievedValue);

        // Store null value (equivalent to delete)
        engine.put(key, null);

        // Verify it's gone
        retrievedValue = engine.get(key);
        assertNull("Key should be deleted after putting null value", retrievedValue);
    }

    @Test
    public void testPerformanceComparedToRehashing() throws Exception {
        // Compare performance of the engine with and without rehashing

        // First test: with rehashing (default behavior)
        long startTimeWithRehash = System.currentTimeMillis();

        for (int i = 0; i < 10000; i++) {
            String keyStr = "rehash-test-key-" + i;
            String valueStr = "rehash-test-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        long endTimeWithRehash = System.currentTimeMillis();
        long durationWithRehash = endTimeWithRehash - startTimeWithRehash;

        // Close the engine
        engine.close();

        // Create a new engine with rehashing disabled
        config.setDisableRehashing(true);
        engine = new HashTableEngine(config);
        engine.open();

        // Second test: without rehashing
        long startTimeWithoutRehash = System.currentTimeMillis();

        for (int i = 0; i < 10000; i++) {
            String keyStr = "no-rehash-test-key-" + i;
            String valueStr = "no-rehash-test-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        long endTimeWithoutRehash = System.currentTimeMillis();
        long durationWithoutRehash = endTimeWithoutRehash - startTimeWithoutRehash;

        // Log the times (in a real test, we'd use a proper benchmark framework)
        System.out.println("Time with rehashing: " + durationWithRehash + "ms");
        System.out.println("Time without rehashing: " + durationWithoutRehash + "ms");

        // Not asserting anything here since performance varies by system,
        // but the test demonstrates the capability to measure performance
    }
}
