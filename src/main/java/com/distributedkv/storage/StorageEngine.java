package com.distributedkv.storage;

import java.io.Closeable;
import java.util.Map;
import java.util.Iterator;
import java.util.List;

/**
 * Interface for storage engines used by the distributed key-value store.
 *
 * The StorageEngine interface defines the basic operations that all storage
 * engines must implement, such as get, put, delete, and iteration over the
 * stored key-value pairs.
 */
public interface StorageEngine extends Closeable {

    /**
     * Initializes the storage engine.
     *
     * @throws Exception If an error occurs during initialization
     */
    void initialize() throws Exception;

    /**
     * Gets the value associated with the specified key.
     *
     * @param key The key
     * @return The value, or null if the key is not found
     */
    byte[] get(byte[] key);

    /**
     * Puts a key-value pair into the store.
     *
     * @param key The key
     * @param value The value
     */
    void put(byte[] key, byte[] value);

    /**
     * Puts multiple key-value pairs into the store in a batch operation.
     *
     * @param entries The key-value pairs to put
     */
    void putAll(Map<byte[], byte[]> entries);

    /**
     * Deletes the specified key and its associated value.
     *
     * @param key The key
     * @return True if the key was deleted, false if it didn't exist
     */
    boolean delete(byte[] key);

    /**
     * Deletes multiple keys in a batch operation.
     *
     * @param keys The keys to delete
     */
    void deleteAll(List<byte[]> keys);

    /**
     * Checks if the specified key exists in the store.
     *
     * @param key The key
     * @return True if the key exists, false otherwise
     */
    boolean exists(byte[] key);

    /**
     * Gets all key-value pairs in the store.
     *
     * @return Map of all key-value pairs
     */
    Map<byte[], byte[]> getAll();

    /**
     * Returns an iterator over all keys in the store.
     *
     * @return Iterator over all keys
     */
    Iterator<byte[]> keyIterator();

    /**
     * Returns an iterator over all key-value pairs in the store.
     *
     * @return Iterator over all key-value pairs
     */
    Iterator<Map.Entry<byte[], byte[]>> entryIterator();

    /**
     * Returns an iterator over key-value pairs in the specified range.
     *
     * @param startKey The start key (inclusive)
     * @param endKey The end key (exclusive)
     * @return Iterator over key-value pairs in the range
     */
    Iterator<Map.Entry<byte[], byte[]>> rangeIterator(byte[] startKey, byte[] endKey);

    /**
     * Clears all data from the store.
     */
    void clear();

    /**
     * Gets the approximate number of entries in the store.
     *
     * @return The approximate number of entries
     */
    long approximateSize();

    /**
     * Creates a new snapshot of the storage engine.
     *
     * @return A handle to the snapshot
     */
    Object createSnapshot();

    /**
     * Releases a previously created snapshot.
     *
     * @param snapshot The snapshot to release
     */
    void releaseSnapshot(Object snapshot);

    /**
     * Gets the name of this storage engine.
     *
     * @return The engine name
     */
    String getName();

    /**
     * Gets the path where this storage engine persists its data.
     *
     * @return The data path
     */
    String getDataPath();

    /**
     * Flushes any in-memory data to disk.
     *
     * @throws Exception If an error occurs during the flush
     */
    void flush() throws Exception;

    /**
     * Closes the storage engine, releasing any resources.
     */
    @Override
    void close();
}
