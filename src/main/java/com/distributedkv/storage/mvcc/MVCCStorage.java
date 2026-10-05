package com.distributedkv.storage.mvcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import com.distributedkv.storage.StorageEngine;

/**
 * MVCC (Multi-Version Concurrency Control) implementation for storage engines.
 *
 * MVCCStorage wraps a storage engine and provides multi-version concurrency
 * control, allowing multiple transactions to concurrently access and modify
 * the data without blocking each other.
 */
public class MVCCStorage {
    private static final Logger LOGGER = Logger.getLogger(MVCCStorage.class.getName());

    /** The underlying storage engine */
    private final StorageEngine storage;

    /** The transaction manager */
    private final TransactionManager txManager;

    /** Map of keys to their versions */
    private final Map<ByteArrayWrapper, List<Version>> versions = new ConcurrentHashMap<>();

    /** Lock for version operations */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** MVCC options */
    private final MVCCOptions options;

    /**
     * Creates a new MVCC storage.
     *
     * @param storage The underlying storage engine
     * @param txManager The transaction manager
     */
    public MVCCStorage(StorageEngine storage, TransactionManager txManager) {
        this(storage, txManager, new MVCCOptions());
    }

    /**
     * Creates a new MVCC storage with the specified options.
     *
     * @param storage The underlying storage engine
     * @param txManager The transaction manager
     * @param options The MVCC options
     */
    public MVCCStorage(StorageEngine storage, TransactionManager txManager, MVCCOptions options) {
        this.storage = storage;
        this.txManager = txManager;
        this.options = options;

        // Initialize versions from storage
        initialize();
    }

    /**
     * Initializes versions from the underlying storage.
     */
    private void initialize() {
        LOGGER.info("Initializing MVCC storage from " + storage.getName());

        // Clear versions
        versions.clear();

        // Create initial versions for all keys in storage
        Map<byte[], byte[]> allEntries = storage.getAll();
        Timestamp initialTimestamp = Timestamp.of(1);

        for (Map.Entry<byte[], byte[]> entry : allEntries.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();

            // Create initial version
            Version version = new Version(value, initialTimestamp, initialTimestamp);
            version.setCommitTimestamp(initialTimestamp);

            // Store version
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            List<Version> versionList = new ArrayList<>();
            versionList.add(version);
            versions.put(keyWrapper, versionList);
        }

        LOGGER.info("Initialized MVCC storage with " + versions.size() + " keys");
    }

    /**
     * Begins a new transaction.
     *
     * @param readOnly Whether the transaction is read-only
     * @return The new transaction
     */
    public Transaction beginTransaction(boolean readOnly) {
        Transaction tx = txManager.beginTransaction(readOnly);

        // Set storage for snapshot isolation transactions
        if (tx instanceof SnapshotIsolation) {
            ((SnapshotIsolation) tx).setStorage(this);
        }

        return tx;
    }

    /**
     * Gets the value for a key at the specified timestamp.
     *
     * @param key The key
     * @param timestamp The timestamp
     * @return The value, or null if not found
     */
    public byte[] get(byte[] key, Timestamp timestamp) {
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

        lock.readLock().lock();
        try {
            List<Version> versionList = versions.get(keyWrapper);
            if (versionList == null || versionList.isEmpty()) {
                return null;
            }

            // Find the latest version visible at the timestamp
            for (Version version : versionList) {
                if (version.isVisibleAt(timestamp)) {
                    return version.getValue();
                }
            }

            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets all versions of a key.
     *
     * @param key The key
     * @return List of all versions of the key
     */
    public List<Version> getVersions(byte[] key) {
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

        lock.readLock().lock();
        try {
            List<Version> versionList = versions.get(keyWrapper);
            if (versionList == null) {
                return Collections.emptyList();
            }

            // Return a copy to avoid concurrent modification
            return new ArrayList<>(versionList);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets all key-value pairs visible at the specified timestamp.
     *
     * @param timestamp The timestamp
     * @return Map of all visible key-value pairs
     */
    public Map<byte[], byte[]> getAll(Timestamp timestamp) {
        Map<byte[], byte[]> result = new HashMap<>();

        lock.readLock().lock();
        try {
            for (Map.Entry<ByteArrayWrapper, List<Version>> entry : versions.entrySet()) {
                ByteArrayWrapper keyWrapper = entry.getKey();
                List<Version> versionList = entry.getValue();

                if (versionList.isEmpty()) {
                    continue;
                }

                // Find the latest version visible at the timestamp
                for (Version version : versionList) {
                    if (version.isVisibleAt(timestamp)) {
                        result.put(keyWrapper.getData(), version.getValue());
                        break;
                    }
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Puts a key-value pair with the specified timestamps.
     *
     * @param key The key
     * @param value The value
     * @param startTimestamp The start timestamp
     * @param commitTimestamp The commit timestamp
     */
    public void put(byte[] key, byte[] value, Timestamp startTimestamp, Timestamp commitTimestamp) {
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

        lock.writeLock().lock();
        try {
            // Create new version
            Version version = new Version(value, startTimestamp, startTimestamp);
            version.setCommitTimestamp(commitTimestamp);

            // Get or create version list
            List<Version> versionList = versions.computeIfAbsent(keyWrapper, k -> new ArrayList<>());

            // Add new version
            versionList.add(version);

            // Sort versions by commit timestamp (newest first)
            versionList.sort(Comparator.comparing(Version::getCommitTimestamp).reversed());

            // Limit number of versions if configured
            if (options.getMaxVersionsPerKey() > 0 && versionList.size() > options.getMaxVersionsPerKey()) {
                // Remove oldest versions
                versionList.subList(options.getMaxVersionsPerKey(), versionList.size()).clear();
            }

            // Persist to storage
            storage.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Deletes a key with the specified timestamps.
     *
     * @param key The key
     * @param startTimestamp The start timestamp
     * @param commitTimestamp The commit timestamp
     */
    public void delete(byte[] key, Timestamp startTimestamp, Timestamp commitTimestamp) {
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

        lock.writeLock().lock();
        try {
            // Get version list
            List<Version> versionList = versions.get(keyWrapper);
            if (versionList == null || versionList.isEmpty()) {
                return;
            }

            // Get the latest version
            Version latestVersion = versionList.get(0);

            // Create a deleted version
            Version deletedVersion = new Version(null, startTimestamp, startTimestamp);
            deletedVersion.setCommitTimestamp(commitTimestamp);
            deletedVersion.setDeletedAt(commitTimestamp);

            // Add deleted version
            versionList.add(0, deletedVersion);

            // Limit number of versions if configured
            if (options.getMaxVersionsPerKey() > 0 && versionList.size() > options.getMaxVersionsPerKey()) {
                // Remove oldest versions
                versionList.subList(options.getMaxVersionsPerKey(), versionList.size()).clear();
            }

            // If all versions are deleted, remove the key from storage
            if (versionList.stream().allMatch(Version::isDeleted)) {
                storage.delete(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Cleans up old versions.
     *
     * @param olderThan Versions older than this timestamp will be removed
     */
    public void cleanupOldVersions(Timestamp olderThan) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<ByteArrayWrapper, List<Version>> entry : versions.entrySet()) {
                ByteArrayWrapper keyWrapper = entry.getKey();
                List<Version> versionList = entry.getValue();

                // Remove versions that are older than the specified timestamp
                versionList.removeIf(version ->
                        version.getCommitTimestamp() != null &&
                        version.getCommitTimestamp().isBefore(olderThan));

                // If no versions remain, remove the key
                if (versionList.isEmpty()) {
                    versions.remove(keyWrapper);
                    storage.delete(keyWrapper.getData());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the underlying storage engine.
     *
     * @return The storage engine
     */
    public StorageEngine getStorage() {
        return storage;
    }

    /**
     * Gets the transaction manager.
     *
     * @return The transaction manager
     */
    public TransactionManager getTransactionManager() {
        return txManager;
    }

    /**
     * Gets the MVCC options.
     *
     * @return The MVCC options
     */
    public MVCCOptions getOptions() {
        return options;
    }

    /**
     * Wrapper for byte arrays to use them as keys in maps.
     */
    private static class ByteArrayWrapper {
        private final byte[] data;
        private final int hashCode;

        /**
         * Creates a new byte array wrapper.
         *
         * @param data The byte array
         */
        public ByteArrayWrapper(byte[] data) {
            this.data = data;
            this.hashCode = java.util.Arrays.hashCode(data);
        }

        /**
         * Gets the wrapped byte array.
         *
         * @return The byte array
         */
        public byte[] getData() {
            return data;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ByteArrayWrapper other = (ByteArrayWrapper) obj;
            return java.util.Arrays.equals(data, other.data);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    /**
     * Options for MVCC storage.
     */
    public static class MVCCOptions {
        /** Maximum number of versions to keep per key (0 = unlimited) */
        private int maxVersionsPerKey = 0;

        /** Cleanup interval in milliseconds (0 = no cleanup) */
        private long cleanupIntervalMs = 0;

        /** Time-to-live for versions in milliseconds (0 = no TTL) */
        private long versionTtlMs = 0;

        /**
         * Creates new MVCC options with default values.
         */
        public MVCCOptions() {
            // Default constructor
        }

        /**
         * Gets the maximum number of versions to keep per key.
         *
         * @return The maximum number of versions
         */
        public int getMaxVersionsPerKey() {
            return maxVersionsPerKey;
        }

        /**
         * Sets the maximum number of versions to keep per key.
         *
         * @param maxVersionsPerKey The maximum number of versions
         * @return This options object for chaining
         */
        public MVCCOptions setMaxVersionsPerKey(int maxVersionsPerKey) {
            this.maxVersionsPerKey = maxVersionsPerKey;
            return this;
        }

        /**
         * Gets the cleanup interval in milliseconds.
         *
         * @return The cleanup interval
         */
        public long getCleanupIntervalMs() {
            return cleanupIntervalMs;
        }

        /**
         * Sets the cleanup interval in milliseconds.
         *
         * @param cleanupIntervalMs The cleanup interval
         * @return This options object for chaining
         */
        public MVCCOptions setCleanupIntervalMs(long cleanupIntervalMs) {
            this.cleanupIntervalMs = cleanupIntervalMs;
            return this;
        }

        /**
         * Gets the time-to-live for versions in milliseconds.
         *
         * @return The version TTL
         */
        public long getVersionTtlMs() {
            return versionTtlMs;
        }

        /**
         * Sets the time-to-live for versions in milliseconds.
         *
         * @param versionTtlMs The version TTL
         * @return This options object for chaining
         */
        public MVCCOptions setVersionTtlMs(long versionTtlMs) {
            this.versionTtlMs = versionTtlMs;
            return this;
        }
    }
}
