package com.distributedkv.storage;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.storage.mvcc.MVCCStorage;
import com.distributedkv.storage.mvcc.SnapshotIsolation;
import com.distributedkv.storage.mvcc.Timestamp;
import com.distributedkv.storage.mvcc.Transaction;
import com.distributedkv.storage.mvcc.TransactionManager;
import com.distributedkv.storage.mvcc.Version;

public class MVCCTest {

    private MVCCStorage mvccStorage;
    private TransactionManager txManager;
    private StorageEngine mockEngine;

    @Before
    public void setUp() throws Exception {
        // Create mock storage engine
        mockEngine = mock(StorageEngine.class);

        // Create timestamp generator
        AtomicLong timeCounter = new AtomicLong(1);

        // Setup MVCC components
        txManager = new TransactionManager(() -> new Timestamp(timeCounter.getAndIncrement()));
        mvccStorage = new MVCCStorage(mockEngine, txManager);
    }

    @After
    public void tearDown() throws Exception {
        if (mvccStorage != null) {
            mvccStorage.close();
        }
    }

    @Test
    public void testBasicGetPut() throws Exception {
        // Start a transaction
        Transaction tx = txManager.begin();

        // Mock storage engine behavior
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        // Initially no versions exist
        when(mockEngine.get(any(byte[].class))).thenReturn(null);

        // Put should create a new version
        mvccStorage.put(tx, key, value);

        // Verify that the version was written to storage
        // The version key should include the transaction timestamp
        byte[] expectedVersionKey = new byte[key.length + 8]; // key + timestamp
        System.arraycopy(key, 0, expectedVersionKey, 0, key.length);
        System.arraycopy(tx.getTimestamp().toBytes(), 0, expectedVersionKey, key.length, 8);

        verify(mockEngine, times(1)).put(eq(expectedVersionKey), eq(value));

        // Commit the transaction
        tx.commit();

        // Start another transaction to read the value
        Transaction tx2 = txManager.begin();

        // Now setup the mockEngine to return the version
        Version version = new Version(tx.getTimestamp(), value);
        when(mockEngine.get(any(byte[].class))).thenReturn(version.toBytes());

        // Read the value
        byte[] retrievedValue = mvccStorage.get(tx2, key);

        // Verify the value matches
        assertArrayEquals("Retrieved value should match", value, retrievedValue);

        // Commit the read transaction
        tx2.commit();
    }

    @Test
    public void testSnapshotIsolation() throws Exception {
        // Start transaction 1
        Transaction tx1 = txManager.begin();

        byte[] key = "test-key".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        // Put initial value
        when(mockEngine.get(any(byte[].class))).thenReturn(null);
        mvccStorage.put(tx1, key, value1);

        // Commit transaction 1
        tx1.commit();

        // Start transaction 2 (will read value1)
        Transaction tx2 = txManager.begin();

        // Setup mockEngine to return version from tx1
        Version version1 = new Version(tx1.getTimestamp(), value1);
        when(mockEngine.get(any(byte[].class))).thenReturn(version1.toBytes());

        // Read the value in tx2
        byte[] retrievedValue = mvccStorage.get(tx2, key);
        assertArrayEquals("Transaction 2 should read value1", value1, retrievedValue);

        // Start transaction 3 (will update to value2)
        Transaction tx3 = txManager.begin();

        // Update to value2 in tx3
        mvccStorage.put(tx3, key, value2);

        // Commit transaction 3
        tx3.commit();

        // Now transaction 2 should still read value1 (snapshot isolation)
        // Reset mock to return both versions with tx3's version first
        Version version2 = new Version(tx3.getTimestamp(), value2);
        when(mockEngine.getVersions(eq(key)))
            .thenReturn(Arrays.asList(version2, version1));

        // Re-read the value in tx2
        retrievedValue = mvccStorage.get(tx2, key);
        assertArrayEquals("Transaction 2 should still read value1 under snapshot isolation",
                         value1, retrievedValue);

        // Commit transaction 2
        tx2.commit();

        // Start transaction 4 (will read value2)
        Transaction tx4 = txManager.begin();

        // Reset mock to return both versions
        when(mockEngine.getVersions(eq(key)))
            .thenReturn(Arrays.asList(version2, version1));

        // Read the value in tx4
        retrievedValue = mvccStorage.get(tx4, key);
        assertArrayEquals("Transaction 4 should read value2", value2, retrievedValue);

        // Commit transaction 4
        tx4.commit();
    }

    @Test
    public void testWriteSkew() throws Exception {
        // Write skew is a concurrency anomaly that can happen in snapshot isolation
        // but not serializable isolation

        byte[] keyX = "X".getBytes();
        byte[] keyY = "Y".getBytes();
        byte[] value50 = "50".getBytes();
        byte[] value70 = "70".getBytes();
        byte[] value0 = "0".getBytes();

        // Start and commit an initial transaction to set both X and Y to 50
        Transaction initTx = txManager.begin();

        // Setup mock for initial puts
        when(mockEngine.get(any(byte[].class))).thenReturn(null);

        mvccStorage.put(initTx, keyX, value50);
        mvccStorage.put(initTx, keyY, value50);
        initTx.commit();

        // Now start two concurrent transactions
        Transaction tx1 = txManager.begin();
        Transaction tx2 = txManager.begin();

        // Setup mockEngine to return version from initTx
        Version versionX = new Version(initTx.getTimestamp(), value50);
        Version versionY = new Version(initTx.getTimestamp(), value50);

        // T1 reads X=50, Y=50
        when(mockEngine.getVersions(eq(keyX))).thenReturn(Arrays.asList(versionX));
        when(mockEngine.getVersions(eq(keyY))).thenReturn(Arrays.asList(versionY));

        byte[] tx1X = mvccStorage.get(tx1, keyX);
        byte[] tx1Y = mvccStorage.get(tx1, keyY);
        assertArrayEquals("TX1 should read X=50", value50, tx1X);
        assertArrayEquals("TX1 should read Y=50", value50, tx1Y);

        // T2 reads X=50, Y=50
        byte[] tx2X = mvccStorage.get(tx2, keyX);
        byte[] tx2Y = mvccStorage.get(tx2, keyY);
        assertArrayEquals("TX2 should read X=50", value50, tx2X);
        assertArrayEquals("TX2 should read Y=50", value50, tx2Y);

        // Both transactions see that X+Y=100

        // T1 updates X=0 (because it saw Y=50)
        mvccStorage.put(tx1, keyX, value0);

        // T2 updates Y=0 (because it saw X=50)
        mvccStorage.put(tx2, keyY, value0);

        // Both transactions can commit under snapshot isolation
        tx1.commit();
        tx2.commit();

        // Start a new transaction to see the final state
        Transaction tx3 = txManager.begin();

        // Setup mockEngine to return latest versions
        Version versionX0 = new Version(tx1.getTimestamp(), value0);
        Version versionY0 = new Version(tx2.getTimestamp(), value0);
        when(mockEngine.getVersions(eq(keyX))).thenReturn(Arrays.asList(versionX0, versionX));
        when(mockEngine.getVersions(eq(keyY))).thenReturn(Arrays.asList(versionY0, versionY));

        byte[] finalX = mvccStorage.get(tx3, keyX);
        byte[] finalY = mvccStorage.get(tx3, keyY);

        assertArrayEquals("Final X should be 0", value0, finalX);
        assertArrayEquals("Final Y should be 0", value0, finalY);

        // This is the write skew anomaly in action - we end up with X=0 and Y=0,
        // where neither transaction would have committed this if they had seen each other's changes.

        tx3.commit();
    }

    @Test
    public void testWriteConflict() throws Exception {
        byte[] key = "test-key".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        // Start transaction 1
        Transaction tx1 = txManager.begin();

        // Setup mock for initial put
        when(mockEngine.get(any(byte[].class))).thenReturn(null);

        // T1 writes value1
        mvccStorage.put(tx1, key, value1);

        // Start transaction 2
        Transaction tx2 = txManager.begin();

        // T2 tries to write value2 to the same key
        mvccStorage.put(tx2, key, value2);

        // T1 commits successfully
        tx1.commit();

        // T2 should fail to commit due to write conflict
        try {
            tx2.commit();
            fail("Transaction 2 should have failed to commit due to write conflict");
        } catch (ExecutionException e) {
            assertTrue("Should throw write conflict error",
                    e.getCause().getMessage().contains("Write conflict"));
        }
    }

    @Test
    public void testReadModifyWrite() throws Exception {
        byte[] key = "counter".getBytes();
        byte[] initialValue = "10".getBytes();
        byte[] incrementedValue = "11".getBytes();

        // Setup initial value
        Transaction initTx = txManager.begin();
        when(mockEngine.get(any(byte[].class))).thenReturn(null);
        mvccStorage.put(initTx, key, initialValue);
        initTx.commit();

        // Start a new transaction for the read-modify-write operation
        Transaction tx = txManager.begin();

        // Setup mock to return initial version
        Version initialVersion = new Version(initTx.getTimestamp(), initialValue);
        when(mockEngine.getVersions(eq(key))).thenReturn(Arrays.asList(initialVersion));

        // Read current value
        byte[] currentValue = mvccStorage.get(tx, key);
        assertArrayEquals("Should read initial value", initialValue, currentValue);

        // Modify: increment the counter
        int counterValue = Integer.parseInt(new String(currentValue));
        counterValue++;

        // Write back
        mvccStorage.put(tx, key, String.valueOf(counterValue).getBytes());

        // Commit the transaction
        tx.commit();

        // Start another transaction to verify the final value
        Transaction verifyTx = txManager.begin();

        // Setup mock to return the updated version
        Version updatedVersion = new Version(tx.getTimestamp(), incrementedValue);
        when(mockEngine.getVersions(eq(key))).thenReturn(Arrays.asList(updatedVersion, initialVersion));

        // Read and verify
        byte[] finalValue = mvccStorage.get(verifyTx, key);
        assertArrayEquals("Counter should be incremented", incrementedValue, finalValue);

        verifyTx.commit();
    }

    @Test
    public void testConcurrentReadModifyWrite() throws Exception {
        byte[] key = "counter".getBytes();
        byte[] initialValue = "10".getBytes();
        byte[] tx1Value = "11".getBytes();
        byte[] tx2Value = "11".getBytes(); // Both increment by 1

        // Setup initial value
        Transaction initTx = txManager.begin();
        when(mockEngine.get(any(byte[].class))).thenReturn(null);
        mvccStorage.put(initTx, key, initialValue);
        initTx.commit();

        // Create two concurrent transactions
        Transaction tx1 = txManager.begin();
        Transaction tx2 = txManager.begin();

        // Setup mock to return initial version for both transactions
        Version initialVersion = new Version(initTx.getTimestamp(), initialValue);
        when(mockEngine.getVersions(eq(key))).thenReturn(Arrays.asList(initialVersion));

        // Both transactions read the initial value
        byte[] tx1Read = mvccStorage.get(tx1, key);
        byte[] tx2Read = mvccStorage.get(tx2, key);

        assertArrayEquals("TX1 should read initial value", initialValue, tx1Read);
        assertArrayEquals("TX2 should read initial value", initialValue, tx2Read);

        // Both transactions increment the value
        int tx1Counter = Integer.parseInt(new String(tx1Read));
        tx1Counter++;
        mvccStorage.put(tx1, key, String.valueOf(tx1Counter).getBytes());

        int tx2Counter = Integer.parseInt(new String(tx2Read));
        tx2Counter++;
        mvccStorage.put(tx2, key, String.valueOf(tx2Counter).getBytes());

        // TX1 commits successfully
        tx1.commit();

        // Update mock to include tx1's commit
        Version tx1Version = new Version(tx1.getTimestamp(), tx1Value);
        when(mockEngine.getVersions(eq(key))).thenReturn(Arrays.asList(tx1Version, initialVersion));

        // TX2 should fail to commit due to write conflict (under serializable isolation)
        try {
            tx2.commit();
            fail("Transaction 2 should have failed to commit due to write conflict");
        } catch (ExecutionException e) {
            assertTrue("Should throw write conflict error",
                    e.getCause().getMessage().contains("Write conflict"));
        }

        // Start a new transaction to verify the final value
        Transaction verifyTx = txManager.begin();
        when(mockEngine.getVersions(eq(key))).thenReturn(Arrays.asList(tx1Version, initialVersion));

        byte[] finalValue = mvccStorage.get(verifyTx, key);
        assertArrayEquals("Final value should be from TX1", tx1Value, finalValue);

        verifyTx.commit();
    }

    @Test
    public void testTransactionTimeout() throws Exception {
        // Configure transaction with short timeout
        long timeoutMs = 100;
        Transaction tx = txManager.beginWithTimeout(timeoutMs);

        // Do some operations
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        when(mockEngine.get(any(byte[].class))).thenReturn(null);
        mvccStorage.put(tx, key, value);

        // Wait for the transaction to time out
        Thread.sleep(timeoutMs * 2);

        // Transaction should be aborted automatically
        assertTrue("Transaction should be aborted after timeout", tx.isAborted());

        // Attempting to commit should fail
        try {
            tx.commit();
            fail("Commit should fail for timed-out transaction");
        } catch (Exception e) {
            assertTrue("Should throw transaction aborted error",
                    e.getMessage().contains("aborted"));
        }
    }

    @Test
    public void testGarbageCollection() throws Exception {
        // Setup test with multiple versions
        byte[] key = "gc-test".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();

        // Create several versions through different transactions
        Transaction tx1 = txManager.begin();
        when(mockEngine.get(any(byte[].class))).thenReturn(null);
        mvccStorage.put(tx1, key, value1);
        tx1.commit();

        Transaction tx2 = txManager.begin();
        Version v1 = new Version(tx1.getTimestamp(), value1);
        when(mockEngine.getVersions(eq(key))).thenReturn(Arrays.asList(v1));
        mvccStorage.put(tx2, key, value2);
        tx2.commit();

        Transaction tx3 = txManager.begin();
        Version v2 = new Version(tx2.getTimestamp(), value2);
        when(mockEngine.getVersions(eq(key))).thenReturn(Arrays.asList(v2, v1));
        mvccStorage.put(tx3, key, value3);
        tx3.commit();

        // Now we have 3 versions: v1, v2, v3
        Version v3 = new Version(tx3.getTimestamp(), value3);
        when(mockEngine.getVersions(eq(key))).thenReturn(Arrays.asList(v3, v2, v1));

        // Simulate that no transactions need v1 anymore (oldest timestamp is after v1)
        Timestamp oldestActiveTs = new Timestamp(tx2.getTimestamp().getValue() + 1);

        // Run garbage collection
        int removed = mvccStorage.garbageCollect(oldestActiveTs);

        // Verify that one version was removed (v1)
        assertEquals("One version should be garbage collected", 1, removed);

        // Verify the storage engine delete was called for v1
        verify(mockEngine, times(1)).deleteVersion(eq(key), eq(tx1.getTimestamp()));
    }
}
