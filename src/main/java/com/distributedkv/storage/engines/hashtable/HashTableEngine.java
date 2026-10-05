package com.distributedkv.storage.engines.hashtable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.storage.StorageEngine;

/**
 * Hash table implementation of the StorageEngine interface.
 *
 * This implementation uses a concurrent hash map to store key-value pairs,
 * providing high-performance random access and concurrent operations.
 */
public class HashTableEngine implements StorageEngine {
    private static final Logger LOGGER = Logger.getLogger(HashTableEngine.class.getName());

    /** The name of this storage engine */
    private final String name;

    /** The path where this storage engine persists its data */
    private final String dataPath;

    /** The hash map storing the data */
    private ConcurrentHashMap<ByteArrayWrapper, byte[]> map;

    /** Options for the hash table */
    private final HashTableOptions options;

    /** Executor for background tasks */
    private ScheduledExecutorService scheduler;

    /** Flag indicating whether this engine is closed */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Flag indicating whether the map has been modified since the last save */
    private final AtomicBoolean dirty = new AtomicBoolean(false);

    /**
     * Creates a new hash table engine.
     *
     * @param name The engine name
     * @param dataPath The data path
     */
    public HashTableEngine(String name, String dataPath) {
        this(name, dataPath, new HashTableOptions());
    }

    /**
     * Creates a new hash table engine with the specified options.
     *
     * @param name The engine name
     * @param dataPath The data path
     * @param options The hash table options
     */
    public HashTableEngine(String name, String dataPath, HashTableOptions options) {
        this.name = name;
        this.dataPath = dataPath;
        this.options = options;
    }

    @Override
    public void initialize() throws Exception {
        LOGGER.info("Initializing hash table engine: " + name + " at " + dataPath);

        // Create data directory if it doesn't exist
        Path dirPath = Paths.get(dataPath);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        // Try to load existing map
        File mapFile = getMapFile();
        if (mapFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(mapFile))) {
                map = (ConcurrentHashMap<ByteArrayWrapper, byte[]>) ois.readObject();
                LOGGER.info("Loaded hash map from " + mapFile.getAbsolutePath() +
                          " with " + map.size() + " entries");
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to load hash map, creating a new one", e);
                map = createMap();
            }
        } else {
            map = createMap();
            LOGGER.info("Created new hash map");
        }

        // Start background tasks
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "hashtable-engine-" + name);
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic flush
        if (options.getFlushIntervalMs() > 0) {
            scheduler.scheduleAtFixedRate(this::backgroundFlush,
                                        options.getFlushIntervalMs(),
                                        options.getFlushIntervalMs(),
                                        TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Creates a new hash map based on the options.
     *
     * @return The new hash map
     */
    private ConcurrentHashMap<ByteArrayWrapper, byte[]> createMap() {
        return new ConcurrentHashMap<>(options.getInitialCapacity(), options.getLoadFactor(), options.getConcurrencyLevel());
    }

    /**
     * Gets the file where the hash map is stored.
     *
     * @return The map file
     */
    private File getMapFile() {
        return new File(dataPath, name + ".hmap");
    }

    /**
     * Performs a background flush if the map is dirty.
     */
    private void backgroundFlush() {
        if (dirty.compareAndSet(true, false)) {
            try {
                saveMap();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to flush hash map", e);
                dirty.set(true);
            }
        }
    }

    /**
     * Saves the hash map to disk.
     *
     * @throws IOException If an I/O error occurs
     */
    private void saveMap() throws IOException {
        if (closed.get()) {
            return;
        }

        File mapFile = getMapFile();
        File tempFile = new File(mapFile.getParentFile(), mapFile.getName() + ".tmp");

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            oos.writeObject(map);
            oos.flush();
        }

        // Atomic rename
        if (!tempFile.renameTo(mapFile)) {
            throw new IOException("Failed to rename temp file to " + mapFile.getAbsolutePath());
        }

        LOGGER.fine("Saved hash map to " + mapFile.getAbsolutePath());
    }

    @Override
    public byte[] get(byte[] key) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        return map.get(new ByteArrayWrapper(key));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        map.put(new ByteArrayWrapper(key), value);
        dirty.set(true);
    }

    @Override
    public void putAll(Map<byte[], byte[]> entries) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            map.put(new ByteArrayWrapper(entry.getKey()), entry.getValue());
        }

        dirty.set(true);
    }

    @Override
    public boolean delete(byte[] key) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        byte[] oldValue = map.remove(new ByteArrayWrapper(key));
        if (oldValue != null) {
            dirty.set(true);
            return true;
        }

        return false;
    }

    @Override
    public void deleteAll(List<byte[]> keys) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        boolean anyDeleted = false;

        for (byte[] key : keys) {
            byte[] oldValue = map.remove(new ByteArrayWrapper(key));
            if (oldValue != null) {
                anyDeleted = true;
            }
        }

        if (anyDeleted) {
            dirty.set(true);
        }
    }

    @Override
    public boolean exists(byte[] key) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        return map.containsKey(new ByteArrayWrapper(key));
    }

    @Override
    public Map<byte[], byte[]> getAll() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        // Create a map to hold all entries
        Map<byte[], byte[]> result = new java.util.HashMap<>();

        // Copy all entries from the internal map
        for (Map.Entry<ByteArrayWrapper, byte[]> entry : map.entrySet()) {
            result.put(entry.getKey().getData(), entry.getValue());
        }

        return result;
    }

    @Override
    public Iterator<byte[]> keyIterator() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        final Iterator<ByteArrayWrapper> keyIter = map.keySet().iterator();

        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return keyIter.hasNext();
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                return keyIter.next().getData();
            }
        };
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> entryIterator() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        final Iterator<Map.Entry<ByteArrayWrapper, byte[]>> entryIter = map.entrySet().iterator();

        return new Iterator<Map.Entry<byte[], byte[]>>() {
            @Override
            public boolean hasNext() {
                return entryIter.hasNext();
            }

            @Override
            public Map.Entry<byte[], byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                Map.Entry<ByteArrayWrapper, byte[]> entry = entryIter.next();
                return new AbstractMap.SimpleEntry<>(entry.getKey().getData(), entry.getValue());
            }
        };
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> rangeIterator(byte[] startKey, byte[] endKey) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        // Hash tables don't support efficient range queries, so we need to filter all entries
        final List<Map.Entry<byte[], byte[]>> filteredEntries = new ArrayList<>();
        ByteArrayWrapper startKeyWrapper = startKey != null ? new ByteArrayWrapper(startKey) : null;
        ByteArrayWrapper endKeyWrapper = endKey != null ? new ByteArrayWrapper(endKey) : null;

        for (Map.Entry<ByteArrayWrapper, byte[]> entry : map.entrySet()) {
            ByteArrayWrapper keyWrapper = entry.getKey();

            // Check if the key is in the range
            if (startKeyWrapper != null && keyWrapper.compareTo(startKeyWrapper) < 0) {
                continue;
            }

            if (endKeyWrapper != null && keyWrapper.compareTo(endKeyWrapper) >= 0) {
                continue;
            }

            filteredEntries.add(new AbstractMap.SimpleEntry<>(keyWrapper.getData(), entry.getValue()));
        }

        // Sort the entries by key
        filteredEntries.sort((e1, e2) -> {
            return ByteArrayWrapper.compare(e1.getKey(), e2.getKey());
        });

        return filteredEntries.iterator();
    }

    @Override
    public void clear() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        map.clear();
        dirty.set(true);
    }

    @Override
    public long approximateSize() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        return map.size();
    }

    @Override
    public Object createSnapshot() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        // For hash table, a snapshot is a shallow copy of the map
        ConcurrentHashMap<ByteArrayWrapper, byte[]> snapshot = new ConcurrentHashMap<>(map);
        return snapshot;
    }

    @Override
    public void releaseSnapshot(Object snapshot) {
        // Nothing to do for hash table snapshots
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDataPath() {
        return dataPath;
    }

    @Override
    public void flush() throws Exception {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        if (dirty.compareAndSet(true, false)) {
            saveMap();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            LOGGER.info("Closing hash table engine: " + name);

            // Shut down scheduler
            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }

            // Flush if dirty
            if (dirty.get()) {
                try {
                    saveMap();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Failed to save hash map during shutdown", e);
                }
            }
        }
    }

    /**
     * Gets the internal hash map.
     *
     * @return The hash map
     */
    public ConcurrentHashMap<ByteArrayWrapper, byte[]> getMap() {
        return map;
    }

    /**
     * Options for the hash table.
     */
    public static class HashTableOptions {
        /** The initial capacity of the hash map */
        private int initialCapacity = 16;

        /** The load factor of the hash map */
        private float loadFactor = 0.75f;

        /** The concurrency level of the hash map */
        private int concurrencyLevel = 16;

        /** The flush interval in milliseconds (0 = no automatic flush) */
        private long flushIntervalMs = 5000;

        /**
         * Creates new hash table options with default values.
         */
        public HashTableOptions() {
            // Default constructor
        }

        /**
         * Gets the initial capacity of the hash map.
         *
         * @return The initial capacity
         */
        public int getInitialCapacity() {
            return initialCapacity;
        }

        /**
         * Sets the initial capacity of the hash map.
         *
         * @param initialCapacity The initial capacity
         * @return This options object for chaining
         */
        public HashTableOptions setInitialCapacity(int initialCapacity) {
            this.initialCapacity = initialCapacity;
            return this;
        }

        /**
         * Gets the load factor of the hash map.
         *
         * @return The load factor
         */
        public float getLoadFactor() {
            return loadFactor;
        }

        /**
         * Sets the load factor of the hash map.
         *
         * @param loadFactor The load factor
         * @return This options object for chaining
         */
        public HashTableOptions setLoadFactor(float loadFactor) {
            this.loadFactor = loadFactor;
            return this;
        }

        /**
         * Gets the concurrency level of the hash map.
         *
         * @return The concurrency level
         */
        public int getConcurrencyLevel() {
            return concurrencyLevel;
        }

        /**
         * Sets the concurrency level of the hash map.
         *
         * @param concurrencyLevel The concurrency level
         * @return This options object for chaining
         */
        public HashTableOptions setConcurrencyLevel(int concurrencyLevel) {
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        /**
         * Gets the flush interval in milliseconds.
         *
         * @return The flush interval
         */
        public long getFlushIntervalMs() {
            return flushIntervalMs;
        }

        /**
         * Sets the flush interval in milliseconds.
         *
         * @param flushIntervalMs The flush interval
         * @return This options object for chaining
         */
        public HashTableOptions setFlushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }
    }
}
