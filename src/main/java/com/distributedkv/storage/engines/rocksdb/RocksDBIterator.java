package com.distributedkv.storage.engines.rocksdb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.AbstractMap;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.ReadOptions;

/**
 * Iterator implementation for RocksDB.
 *
 * This class provides a Java Iterator interface over RocksDB's native iterator,
 * allowing for seamless integration with Java collections and streaming APIs.
 */
public class RocksDBIterator implements Iterator<Map.Entry<byte[], byte[]>>, AutoCloseable {

    /** The RocksDB native iterator */
    private final RocksIterator iterator;

    /** Flag indicating whether this iterator is closed */
    private boolean closed = false;

    /** Flag indicating whether the next element has been pre-fetched */
    private boolean hasNextCalled = false;

    /** Flag indicating whether there is a next element available */
    private boolean hasNext = false;

    /** The current key */
    private byte[] currentKey;

    /** The current value */
    private byte[] currentValue;

    /** The end key for range iteration (may be null) */
    private final byte[] endKey;

    /**
     * Creates a new RocksDB iterator.
     *
     * @param db The RocksDB instance
     * @param readOptions The read options
     */
    public RocksDBIterator(RocksDB db, ReadOptions readOptions) {
        this(db, readOptions, null, null);
    }

    /**
     * Creates a new RocksDB iterator with a range.
     *
     * @param db The RocksDB instance
     * @param readOptions The read options
     * @param startKey The start key (inclusive), or null to start from the beginning
     * @param endKey The end key (exclusive), or null to iterate to the end
     */
    public RocksDBIterator(RocksDB db, ReadOptions readOptions, byte[] startKey, byte[] endKey) {
        this.iterator = db.newIterator(readOptions);
        this.endKey = endKey;

        // Position the iterator at the start position
        if (startKey == null) {
            iterator.seekToFirst();
        } else {
            iterator.seek(startKey);
        }
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }

        if (hasNextCalled) {
            return hasNext;
        }

        // Check if the iterator is valid
        hasNext = iterator.isValid();

        // Check if we've reached the end key
        if (hasNext && endKey != null) {
            byte[] key = iterator.key();
            hasNext = compareKeys(key, endKey) < 0;
        }

        // Pre-fetch the key and value
        if (hasNext) {
            currentKey = iterator.key();
            currentValue = iterator.value();
        }

        hasNextCalled = true;
        return hasNext;
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        if (!hasNextCalled) {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
        }

        if (!hasNext) {
            throw new NoSuchElementException();
        }

        // Reset the hasNext flag
        hasNextCalled = false;

        // Create an entry with the current key and value
        Map.Entry<byte[], byte[]> entry =
                new AbstractMap.SimpleImmutableEntry<>(currentKey, currentValue);

        // Move the iterator forward
        iterator.next();

        return entry;
    }

    /**
     * Gets the current key.
     *
     * @return The current key
     * @throws IllegalStateException If the iterator is closed or not positioned at an entry
     */
    public byte[] getKey() {
        if (closed || !iterator.isValid()) {
            throw new IllegalStateException("Iterator is not positioned at a valid entry");
        }

        return iterator.key();
    }

    /**
     * Gets the current value.
     *
     * @return The current value
     * @throws IllegalStateException If the iterator is closed or not positioned at an entry
     */
    public byte[] getValue() {
        if (closed || !iterator.isValid()) {
            throw new IllegalStateException("Iterator is not positioned at a valid entry");
        }

        return iterator.value();
    }

    /**
     * Positions the iterator at the specified key.
     *
     * @param key The key to seek to
     */
    public void seek(byte[] key) {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        iterator.seek(key);
        hasNextCalled = false;
    }

    /**
     * Positions the iterator at the first key.
     */
    public void seekToFirst() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        iterator.seekToFirst();
        hasNextCalled = false;
    }

    /**
     * Positions the iterator at the last key.
     */
    public void seekToLast() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        iterator.seekToLast();
        hasNextCalled = false;
    }

    /**
     * Closes this iterator, releasing any resources.
     */
    @Override
    public void close() {
        if (!closed) {
            iterator.close();
            closed = true;
        }
    }

    /**
     * Compares two byte arrays lexicographically.
     *
     * @param key1 The first key
     * @param key2 The second key
     * @return A negative, zero, or positive integer as the first key is less than, equal to,
     *         or greater than the second key
     */
    private int compareKeys(byte[] key1, byte[] key2) {
        int len1 = key1.length;
        int len2 = key2.length;
        int lim = Math.min(len1, len2);

        for (int i = 0; i < lim; i++) {
            int a = key1[i] & 0xff;
            int b = key2[i] & 0xff;
            if (a != b) {
                return a - b;
            }
        }

        return len1 - len2;
    }
}
