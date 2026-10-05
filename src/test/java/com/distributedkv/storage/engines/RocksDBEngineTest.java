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
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.StorageConfig;
import com.distributedkv.storage.engines.rocksdb.RocksDBEngine;
import com.distributedkv.storage.engines.rocksdb.RocksDBIterator;
import com.distributedkv.storage.mvcc.Timestamp;
import com.distributedkv.storage.mvcc.Version;

public class RocksDBEngineTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private RocksDBEngine engine;
    private Path dbPath;
    private StorageConfig config;

    @Before
    public void setUp() throws Exception {
        // Initialize RocksDB native libraries
        RocksDB.loadLibrary();

        // Create temporary directory for the test database
        dbPath = tempFolder.newFolder("rocksdb-test").toPath();

        // Setup storage configuration
        config = new StorageConfig();
        config.setDbPath(dbPath.toString());
        config.setMaxBackgroundCompactions(2);
        config.setMaxBackgroundFlushes(2);
        config.setWriteBufferSizeMB(64);
        config.setMaxWriteBufferNumber(3);
        config.setTargetFileSizeMB(64);
        config.setMaxBytesForLevelBase(512);

        // Initialize the RocksDB engine
        engine = new RocksDBEngine(config);
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
    public void testScanPrefix() throws Exception {
        // Define test data with a common prefix
        String prefix = "prefix-";
        byte[] prefixBytes = prefix.getBytes();

        Map.Entry<byte[], byte[]>[] entries = new Map.Entry[] {
            Map.entry((prefix + "key1").getBytes(), "value1".getBytes()),
            Map.entry((prefix + "key2").getBytes(), "value2".getBytes()),
            Map.entry((prefix + "key3").getBytes(), "value3".getBytes()),
            Map.entry("other-key".getBytes(), "other-value".getBytes()) // Different prefix
        };

        // Insert all entries
        for (Map.Entry<byte[], byte[]> entry : entries) {
            engine.put(entry.getKey(), entry.getValue());
        }

        // Scan with the prefix
        List<Map.Entry<byte[], byte[]>> results = new ArrayList<>();
        try (RocksDBIterator iterator = engine.newPrefixIterator(prefixBytes)) {
            while (iterator.isValid()) {
                results.add(Map.entry(
                    iterator.key(),
                    iterator.value()
                ));
                iterator.next();
            }
        }

        // Verify results contain only prefix-matching entries
        assertEquals("Scan should return correct number of entries", 3, results.size());

        // Verify each entry has the correct prefix and value
        for (Map.Entry<byte[], byte[]> result : results) {
            String keyString = new String(result.getKey());
            assertTrue("Key should start with prefix", keyString.startsWith(prefix));

            // Find the matching original entry
            boolean found = false;
            for (Map.Entry<byte[], byte[]> entry : entries) {
                if (Arrays.equals(entry.getKey(), result.getKey())) {
                    assertArrayEquals("Value should match", entry.getValue(), result.getValue());
                    found = true;
                    break;
                }
            }
            assertTrue("Should find matching original entry", found);
        }
    }

    @Test
    public void testRangeQuery() throws Exception {
        // Define test data for range query
        byte[][] keys = {
            "range-key1".getBytes(),
            "range-key2".getBytes(),
            "range-key3".getBytes(),
            "range-key4".getBytes(),
            "range-key5".getBytes()
        };

        byte[][] values = {
            "value1".getBytes(),
            "value2".getBytes(),
            "value3".getBytes(),
            "value4".getBytes(),
            "value5".getBytes()
        };

        // Insert all entries
        for (int i = 0; i < keys.length; i++) {
            engine.put(keys[i], values[i]);
        }

        // Define range: from key2 to key4 (inclusive)
        byte[] startKey = keys[1]; // range-key2
        byte[] endKey = keys[3];   // range-key4

        // Execute range query
        List<Map.Entry<byte[], byte[]>> results = engine.getRange(startKey, endKey);

        // Verify results
        assertEquals("Range query should return correct number of entries", 3, results.size());

        // Verify each entry in the range is present with correct value
        for (int i = 1; i <= 3; i++) {
            boolean found = false;
            for (Map.Entry<byte[], byte[]> result : results) {
                if (Arrays.equals(keys[i], result.getKey())) {
                    assertArrayEquals("Value should match", values[i], result.getValue());
                    found = true;
                    break;
                }
            }
            assertTrue("Should find key in range results: " + new String(keys[i]), found);
        }
    }

    @Test
    public void testMVCCVersionStorage() throws Exception {
        // Test storing and retrieving MVCC versions
        byte[] key = "mvcc-key".getBytes();

        // Create multiple versions with different timestamps
        Timestamp ts1 = new Timestamp(1);
        Timestamp ts2 = new Timestamp(2);
        Timestamp ts3 = new Timestamp(3);

        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();

        Version v1 = new Version(ts1, value1);
        Version v2 = new Version(ts2, value2);
        Version v3 = new Version(ts3, value3);

        // Store versions
        engine.putVersion(key, ts1, v1.toBytes());
        engine.putVersion(key, ts2, v2.toBytes());
        engine.putVersion(key, ts3, v3.toBytes());

        // Retrieve versions
        List<Version> versions = engine.getVersions(key);

        // Verify versions
        assertEquals("Should return correct number of versions", 3, versions.size());

        // Versions should be in descending order by timestamp
        assertEquals("First version should have highest timestamp", ts3.getValue(), versions.get(0).getTimestamp().getValue());
        assertEquals("Second version should have middle timestamp", ts2.getValue(), versions.get(1).getTimestamp().getValue());
        assertEquals("Third version should have lowest timestamp", ts1.getValue(), versions.get(2).getTimestamp().getValue());

        // Verify values
        assertArrayEquals("First version should have correct value", value3, versions.get(0).getValue());
        assertArrayEquals("Second version should have correct value", value2, versions.get(1).getValue());
        assertArrayEquals("Third version should have correct value", value1, versions.get(2).getValue());
    }

    @Test
    public void testDeleteVersion() throws Exception {
        // Test deleting specific MVCC versions
        byte[] key = "delete-version-key".getBytes();

        // Create multiple versions
        Timestamp ts1 = new Timestamp(1);
        Timestamp ts2 = new Timestamp(2);
        Timestamp ts3 = new Timestamp(3);

        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();

        Version v1 = new Version(ts1, value1);
        Version v2 = new Version(ts2, value2);
        Version v3 = new Version(ts3, value3);

        // Store versions
        engine.putVersion(key, ts1, v1.toBytes());
        engine.putVersion(key, ts2, v2.toBytes());
        engine.putVersion(key, ts3, v3.toBytes());

        // Delete the middle version
        engine.deleteVersion(key, ts2);

        // Retrieve remaining versions
        List<Version> versions = engine.getVersions(key);

        // Verify versions
        assertEquals("Should have two versions left", 2, versions.size());
        assertEquals("First version should have highest timestamp", ts3.getValue(), versions.get(0).getTimestamp().getValue());
        assertEquals("Second version should have lowest timestamp", ts1.getValue(), versions.get(1).getTimestamp().getValue());
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
    public void testSnapshotAndRestore() throws Exception {
        // Test snapshot creation and restoration

        // Add some initial data
        for (int i = 0; i < 100; i++) {
            String keyStr = "snapshot-key-" + i;
            String valueStr = "snapshot-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Create a snapshot
        String snapshotPath = tempFolder.newFolder("snapshot").getPath();
        engine.createSnapshot(snapshotPath);

        // Modify some data after snapshot
        for (int i = 0; i < 50; i++) {
            String keyStr = "snapshot-key-" + i;
            String newValueStr = "modified-value-" + i;
            engine.put(keyStr.getBytes(), newValueStr.getBytes());
        }

        // Add some new data
        for (int i = 100; i < 150; i++) {
            String keyStr = "snapshot-key-" + i;
            String valueStr = "snapshot-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Close current engine
        engine.close();

        // Create new engine and restore from snapshot
        RocksDBEngine restoredEngine = new RocksDBEngine(config);
        restoredEngine.restoreFromSnapshot(snapshotPath);

        // Verify original data is restored
        for (int i = 0; i < 100; i++) {
            String keyStr = "snapshot-key-" + i;
            String expectedValueStr = "snapshot-value-" + i;

            byte[] retrievedValue = restoredEngine.get(keyStr.getBytes());
            assertArrayEquals("Restored data should match original",
                             expectedValueStr.getBytes(), retrievedValue);
        }

        // Verify post-snapshot modifications are not present
        for (int i = 100; i < 150; i++) {
            byte[] retrievedValue = restoredEngine.get(("snapshot-key-" + i).getBytes());
            assertNull("Post-snapshot data should not be present", retrievedValue);
        }

        restoredEngine.close();
    }

    @Test
    public void testCompaction() throws Exception {
        // Generate lots of data to trigger compaction
        int numKeys = 10000;

        // Write initial data
        for (int i = 0; i < numKeys; i++) {
            String keyStr = "compaction-key-" + i;
            String valueStr = "compaction-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Overwrite with new values to create file fragmentation
        for (int i = 0; i < numKeys; i++) {
            String keyStr = "compaction-key-" + i;
            String valueStr = "compaction-new-value-" + i;
            engine.put(keyStr.getBytes(), valueStr.getBytes());
        }

        // Trigger manual compaction
        engine.compactRange();

        // Verify data is still accessible
        for (int i = 0; i < numKeys; i++) {
            String keyStr = "compaction-key-" + i;
            String expectedValueStr = "compaction-new-value-" + i;

            byte[] retrievedValue = engine.get(keyStr.getBytes());
            assertArrayEquals("Data should be accessible after compaction",
                             expectedValueStr.getBytes(), retrievedValue);
        }
    }
}
