package com.distributedkv.storage.mvcc;

import java.util.Map;
import java.util.Set;

/**
 * Interface for MVCC transactions.
 *
 * Transactions provide isolation between concurrent operations in the key-value
 * store. The Transaction interface defines the operations that can be performed
 * in a transaction, such as get, put, and delete, as well as commit and rollback.
 */
public interface Transaction {

    /**
     * Enum for transaction isolation levels.
     */
    enum IsolationLevel {
        /** Read committed isolation level */
        READ_COMMITTED,

        /** Snapshot isolation level */
        SNAPSHOT_ISOLATION,

        /** Serializable isolation level */
        SERIALIZABLE
    }

    /**
     * Gets the transaction ID.
     *
     * @return The transaction ID
     */
    String getId();

    /**
     * Gets the start timestamp of this transaction.
     *
     * @return The start timestamp
     */
    Timestamp getStartTimestamp();

    /**
     * Gets the commit timestamp of this transaction.
     *
     * @return The commit timestamp, or null if not committed
     */
    Timestamp getCommitTimestamp();

    /**
     * Gets the isolation level of this transaction.
     *
     * @return The isolation level
     */
    IsolationLevel getIsolationLevel();

    /**
     * Checks if this transaction is read-only.
     *
     * @return True if this transaction is read-only
     */
    boolean isReadOnly();

    /**
     * Checks if this transaction is active.
     *
     * @return True if this transaction is active
     */
    boolean isActive();

    /**
     * Checks if this transaction is committed.
     *
     * @return True if this transaction is committed
     */
    boolean isCommitted();

    /**
     * Checks if this transaction is rolled back.
     *
     * @return True if this transaction is rolled back
     */
    boolean isRolledBack();

    /**
     * Gets the value associated with the specified key.
     *
     * @param key The key
     * @return The value, or null if the key is not found
     */
    byte[] get(byte[] key);

    /**
     * Puts a key-value pair.
     *
     * @param key The key
     * @param value The value
     */
    void put(byte[] key, byte[] value);

    /**
     * Deletes the specified key and its associated value.
     *
     * @param key The key
     * @return True if the key was deleted, false if it didn't exist
     */
    boolean delete(byte[] key);

    /**
     * Gets all key-value pairs modified in this transaction.
     *
     * @return Map of all modified key-value pairs
     */
    Map<byte[], byte[]> getModifiedEntries();

    /**
     * Gets the set of keys deleted in this transaction.
     *
     * @return Set of deleted keys
     */
    Set<byte[]> getDeletedKeys();

    /**
     * Gets all key-value pairs in the store that are visible to this transaction.
     *
     * @return Map of all visible key-value pairs
     */
    Map<byte[], byte[]> getAll();

    /**
     * Commits this transaction.
     *
     * @return True if the transaction was committed successfully
     * @throws TransactionConflictException If a conflict occurs
     */
    boolean commit() throws TransactionConflictException;

    /**
     * Rolls back this transaction.
     */
    void rollback();

    /**
     * Exception thrown when a transaction conflict occurs.
     */
    class TransactionConflictException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new transaction conflict exception.
         *
         * @param message The error message
         */
        public TransactionConflictException(String message) {
            super(message);
        }

        /**
         * Creates a new transaction conflict exception.
         *
         * @param message The error message
         * @param cause The cause
         */
        public TransactionConflictException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
