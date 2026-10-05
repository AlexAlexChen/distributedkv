package com.distributedkv.storage.engines.btree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.storage.StorageEngine;

/**
 * B+ tree implementation of the StorageEngine interface.
 *
 * This implementation uses a B+ tree to store key-value pairs, providing
 * efficient range queries and good performance for ordered data.
 */
public class BPlusTreeEngine implements StorageEngine {
    private static final Logger LOGGER = Logger.getLogger(BPlusTreeEngine.class.getName());

    /** The name of this storage engine */
    private final String name;

    /** The path where this storage engine persists its data */
    private final String dataPath;

    /** The B+ tree */
    private BPlusTree tree;

    /** Options for the B+ tree */
    private final BPlusTreeOptions options;

    /** Executor for background tasks */
    private ScheduledExecutorService scheduler;

    /** Flag indicating whether this engine is closed */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Flag indicating whether the tree has been modified since the last save */
    private final AtomicBoolean dirty = new AtomicBoolean(false);

    /**
     * Creates a new B+ tree engine.
     *
     * @param name The engine name
     * @param dataPath The data path
     */
    public BPlusTreeEngine(String name, String dataPath) {
        this(name, dataPath, new BPlusTreeOptions());
    }

    /**
     * Creates a new B+ tree engine with the specified options.
     *
     * @param name The engine name
     * @param dataPath The data path
     * @param options The B+ tree options
     */
    public BPlusTreeEngine(String name, String dataPath, BPlusTreeOptions options) {
        this.name = name;
        this.dataPath = dataPath;
        this.options = options;
    }

    @Override
    public void initialize() throws Exception {
        LOGGER.info("Initializing B+ tree engine: " + name + " at " + dataPath);

        // Create data directory if it doesn't exist
        Path dirPath = Paths.get(dataPath);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        // Try to load existing tree
        File treeFile = getTreeFile();
        if (treeFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(treeFile))) {
                tree = (BPlusTree) ois.readObject();
                LOGGER.info("Loaded B+ tree from " + treeFile.getAbsolutePath() +
                          " with " + tree.size() + " entries");
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to load B+ tree, creating a new one", e);
                tree = createTree();
            }
        } else {
            tree = createTree();
            LOGGER.info("Created new B+ tree");
        }

        // Start background tasks
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bplustree-engine-" + name);
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
     * Creates a new B+ tree based on the options.
     *
     * @return The new B+ tree
     */
    private BPlusTree createTree() {
        return new BPlusTree(options.getOrder());
    }

    /**
     * Gets the file where the B+ tree is stored.
     *
     * @return The tree file
     */
    private File getTreeFile() {
        return new File(dataPath, name + ".btree");
    }

    /**
     * Performs a background flush if the tree is dirty.
     */
    private void backgroundFlush() {
        if (dirty.compareAndSet(true, false)) {
            try {
                saveTree();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to flush B+ tree", e);
                dirty.set(true);
            }
        }
    }

    /**
     * Saves the B+ tree to disk.
     *
     * @throws IOException If an I/O error occurs
     */
    private void saveTree() throws IOException {
        if (closed.get()) {
            return;
        }

        File treeFile = getTreeFile();
        File tempFile = new File(treeFile.getParentFile(), treeFile.getName() + ".tmp");

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            oos.writeObject(tree);
            oos.flush();
        }

        // Atomic rename
        if (!tempFile.renameTo(treeFile)) {
            throw new IOException("Failed to rename temp file to " + treeFile.getAbsolutePath());
        }

        LOGGER.fine("Saved B+ tree to " + treeFile.getAbsolutePath());
    }

    @Override
    public byte[] get(byte[] key) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        return tree.get(key);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        tree.put(key, value);
        dirty.set(true);
    }

    @Override
    public void putAll(Map<byte[], byte[]> entries) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            tree.put(entry.getKey(), entry.getValue());
        }

        dirty.set(true);
    }

    @Override
    public boolean delete(byte[] key) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        byte[] oldValue = tree.remove(key);
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
            byte[] oldValue = tree.remove(key);
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

        return tree.get(key) != null;
    }

    @Override
    public Map<byte[], byte[]> getAll() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        // Create a map to hold all entries
        Map<byte[], byte[]> result = new java.util.HashMap<>();

        // Iterate over all entries in the tree
        Iterator<Map.Entry<byte[], byte[]>> iter = tree.entryIterator();
        while (iter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = iter.next();
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    @Override
    public Iterator<byte[]> keyIterator() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        Iterator<Map.Entry<byte[], byte[]>> entryIter = tree.entryIterator();

        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return entryIter.hasNext();
            }

            @Override
            public byte[] next() {
                return entryIter.next().getKey();
            }
        };
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> entryIterator() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        return tree.entryIterator();
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> rangeIterator(byte[] startKey, byte[] endKey) {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        return tree.rangeIterator(startKey, endKey);
    }

    @Override
    public void clear() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        tree.clear();
        dirty.set(true);
    }

    @Override
    public long approximateSize() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        return tree.size();
    }

    @Override
    public Object createSnapshot() {
        if (closed.get()) {
            throw new IllegalStateException("Storage engine is closed");
        }

        // For B+ tree, a snapshot is just a point-in-time iterator
        return tree.entryIterator();
    }

    @Override
    public void releaseSnapshot(Object snapshot) {
        // Nothing to do for B+ tree snapshots
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
            saveTree();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            LOGGER.info("Closing B+ tree engine: " + name);

            // Shut down scheduler
            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }

            // Flush if dirty
            if (dirty.get()) {
                try {
                    saveTree();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Failed to save B+ tree during shutdown", e);
                }
            }
        }
    }

    /**
     * Gets the B+ tree.
     *
     * @return The B+ tree
     */
    public BPlusTree getTree() {
        return tree;
    }

    /**
     * Options for the B+ tree.
     */
    public static class BPlusTreeOptions {
        /** The order of the B+ tree */
        private int order = 128;

        /** The flush interval in milliseconds (0 = no automatic flush) */
        private long flushIntervalMs = 5000;

        /**
         * Creates new B+ tree options with default values.
         */
        public BPlusTreeOptions() {
            // Default constructor
        }

        /**
         * Gets the order of the B+ tree.
         *
         * @return The order
         */
        public int getOrder() {
            return order;
        }

        /**
         * Sets the order of the B+ tree.
         *
         * @param order The order
         * @return This options object for chaining
         */
        public BPlusTreeOptions setOrder(int order) {
            this.order = order;
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
        public BPlusTreeOptions setFlushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }
    }
}
