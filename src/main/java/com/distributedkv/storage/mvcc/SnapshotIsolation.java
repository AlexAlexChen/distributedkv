package com.distributedkv.storage.mvcc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Implementation of snapshot isolation for MVCC.
 *
 * Snapshot isolation guarantees that all reads made in a transaction see a
 * consistent snapshot of the database from the point in time when the transaction
 * started. Additionally, it guarantees that any write conflict will be detected.
 */
public class SnapshotIsolation extends TransactionManager.AbstractTransaction {
    private static final Logger LOGGER = Logger.getLogger(SnapshotIsolation.class.getName());

    /** The MVCC storage */
    private final MVCCStorage storage;

    /** Read cache for this transaction */
    private final Map<TransactionManager.ByteArrayWrapper, byte[]> readCache = new ConcurrentHashMap<>();

    /**
     * Creates a new snapshot isolation transaction.
     *
     * @param id The transaction ID
     * @param startTimestamp The start timestamp
     * @param readOnly Whether this transaction is read-only
     * @param txManager The transaction manager
     */
    public SnapshotIsolation(String id, Timestamp startTimestamp, boolean readOnly, TransactionManager txManager) {
        super(id, startTimestamp, readOnly, txManager, IsolationLevel.SNAPSHOT_ISOLATION);
        this.storage = null; // Will be set by MVCCStorage when the transaction is used
    }

    /**
     * Creates a new snapshot isolation transaction with the specified storage.
     *
     * @param id The transaction ID
     * @param startTimestamp The start timestamp
     * @param readOnly Whether this transaction is read-only
     * @param txManager The transaction manager
     * @param storage The MVCC storage
     */
    public SnapshotIsolation(String id, Timestamp startTimestamp, boolean readOnly,
                            TransactionManager txManager, MVCCStorage storage) {
        super(id, startTimestamp, readOnly, txManager, IsolationLevel.SNAPSHOT_ISOLATION);
        this.storage = storage;
    }

    /**
     * Sets the MVCC storage.
     *
     * @param storage The MVCC storage
     */
    void setStorage(MVCCStorage storage) {
        if (this.storage != null) {
            throw new IllegalStateException("Storage already set");
        }
    }

    @Override
    public byte[] get(byte[] key) {
        if (!isActive()) {
            throw new IllegalStateException("Transaction is not active");
        }

        // First check the write set
        TransactionManager.ByteArrayWrapper keyWrapper = new TransactionManager.ByteArrayWrapper(key);
        if (writeSet.containsKey(keyWrapper)) {
            return writeSet.get(keyWrapper);
        }

        // Then check if it has been deleted
        if (deleteSet.containsKey(keyWrapper)) {
            return null;
        }

        // Then check the read cache
        if (readCache.containsKey(keyWrapper)) {
            return readCache.get(keyWrapper);
        }

        // Finally, read from storage
        if (storage == null) {
            throw new IllegalStateException("Storage not set");
        }

        byte[] value = storage.get(key, startTimestamp);

        // Cache the result
        if (value != null) {
            readCache.put(keyWrapper, value);
        }

        return value;
    }

    @Override
    public Map<byte[], byte[]> getAll() {
        if (!isActive()) {
            throw new IllegalStateException("Transaction is not active");
        }

        if (storage == null) {
            throw new IllegalStateException("Storage not set");
        }

        // Get all entries from storage
        Map<byte[], byte[]> result = storage.getAll(startTimestamp);

        // Apply deletes and writes
        for (Map.Entry<TransactionManager.ByteArrayWrapper, Boolean> entry : deleteSet.entrySet()) {
            result.remove(entry.getKey().getData());
        }

        for (Map.Entry<TransactionManager.ByteArrayWrapper, byte[]> entry : writeSet.entrySet()) {
            result.put(entry.getKey().getData(), entry.getValue());
        }

        return result;
    }

    @Override
    public boolean commit() throws TransactionConflictException {
        if (!isActive()) {
            return false;
        }

        if (readOnly) {
            // For read-only transactions, just mark as committed
            setCommitTimestamp(Timestamp.allocate());
            return true;
        }

        if (storage == null) {
            throw new IllegalStateException("Storage not set");
        }

        boolean committed = txManager.commitTransaction(this);

        if (committed) {
            // Apply changes to storage
            for (Map.Entry<TransactionManager.ByteArrayWrapper, byte[]> entry : writeSet.entrySet()) {
                storage.put(entry.getKey().getData(), entry.getValue(), startTimestamp, commitTimestamp);
            }

            for (Map.Entry<TransactionManager.ByteArrayWrapper, Boolean> entry : deleteSet.entrySet()) {
                storage.delete(entry.getKey().getData(), startTimestamp, commitTimestamp);
            }
        }

        return committed;
    }

    /**
     * Gets the versions of a key that are visible to this transaction.
     *
     * @param key The key
     * @return List of visible versions, sorted by timestamp (newest first)
     */
    public List<Version> getVisibleVersions(byte[] key) {
        if (storage == null) {
            throw new IllegalStateException("Storage not set");
        }

        List<Version> versions = storage.getVersions(key);
        List<Version> visibleVersions = new ArrayList<>();

        for (Version version : versions) {
            if (version.isVisibleAt(startTimestamp)) {
                visibleVersions.add(version);
            }
        }

        // Sort by timestamp, newest first
        Collections.sort(visibleVersions, (v1, v2) ->
                v2.getCommitTimestamp().compareTo(v1.getCommitTimestamp()));

        return visibleVersions;
    }

    /**
     * Gets a specific version of a key.
     *
     * @param key The key
     * @param timestamp The timestamp
     * @return The version, or null if not found
     */
    public Version getVersion(byte[] key, Timestamp timestamp) {
        if (storage == null) {
            throw new IllegalStateException("Storage not set");
        }

        List<Version> versions = storage.getVersions(key);

        for (Version version : versions) {
            if (version.isVisibleAt(timestamp)) {
                return version;
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "SnapshotIsolation{" +
                "id='" + id + '\'' +
                ", startTimestamp=" + startTimestamp +
                ", commitTimestamp=" + commitTimestamp +
                ", readOnly=" + readOnly +
                ", active=" + isActive() +
                ", writeSetSize=" + writeSet.size() +
                ", deleteSetSize=" + deleteSet.size() +
                '}';
    }
}
