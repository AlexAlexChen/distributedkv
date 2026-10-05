package com.distributedkv.storage.mvcc;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages transactions in the MVCC system.
 *
 * The TransactionManager is responsible for creating, committing, and aborting
 * transactions, as well as managing transaction metadata like timestamps and
 * isolation levels.
 */
public class TransactionManager {
    private static final Logger LOGGER = Logger.getLogger(TransactionManager.class.getName());

    /** Map of active transactions by ID */
    private final Map<String, AbstractTransaction> activeTransactions = new ConcurrentHashMap<>();

    /** Map of committed transactions by ID */
    private final Map<String, AbstractTransaction> committedTransactions = new ConcurrentHashMap<>();

    /** Lock for transaction operations */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Default isolation level for new transactions */
    private Transaction.IsolationLevel defaultIsolationLevel = Transaction.IsolationLevel.SNAPSHOT_ISOLATION;

    /**
     * Creates a new transaction manager.
     */
    public TransactionManager() {
        // No initialization needed
    }

    /**
     * Sets the default isolation level for new transactions.
     *
     * @param level The default isolation level
     */
    public void setDefaultIsolationLevel(Transaction.IsolationLevel level) {
        this.defaultIsolationLevel = level;
    }

    /**
     * Gets the default isolation level for new transactions.
     *
     * @return The default isolation level
     */
    public Transaction.IsolationLevel getDefaultIsolationLevel() {
        return defaultIsolationLevel;
    }

    /**
     * Begins a new transaction.
     *
     * @param readOnly Whether the transaction is read-only
     * @return The new transaction
     */
    public Transaction beginTransaction(boolean readOnly) {
        return beginTransaction(readOnly, defaultIsolationLevel);
    }

    /**
     * Begins a new transaction with the specified isolation level.
     *
     * @param readOnly Whether the transaction is read-only
     * @param isolationLevel The isolation level
     * @return The new transaction
     */
    public Transaction beginTransaction(boolean readOnly, Transaction.IsolationLevel isolationLevel) {
        String txId = UUID.randomUUID().toString();
        Timestamp startTimestamp = Timestamp.allocate();

        AbstractTransaction tx;
        switch (isolationLevel) {
            case SNAPSHOT_ISOLATION:
                tx = new SnapshotIsolation(txId, startTimestamp, readOnly, this);
                break;
            case SERIALIZABLE:
                // TODO: Implement serializable isolation
                throw new UnsupportedOperationException("Serializable isolation not yet implemented");
            case READ_COMMITTED:
                // TODO: Implement read committed isolation
                throw new UnsupportedOperationException("Read committed isolation not yet implemented");
            default:
                throw new IllegalArgumentException("Unknown isolation level: " + isolationLevel);
        }

        activeTransactions.put(txId, tx);

        LOGGER.fine("Started transaction " + txId + " with isolation level " + isolationLevel);

        return tx;
    }

    /**
     * Commits a transaction.
     *
     * @param tx The transaction to commit
     * @return True if the transaction was committed successfully
     * @throws Transaction.TransactionConflictException If a conflict occurs
     */
    public boolean commitTransaction(AbstractTransaction tx) throws Transaction.TransactionConflictException {
        lock.writeLock().lock();
        try {
            if (!tx.isActive()) {
                return false;
            }

            // Check for conflicts
            if (!tx.isReadOnly()) {
                checkConflicts(tx);
            }

            // Allocate commit timestamp
            Timestamp commitTimestamp = Timestamp.allocate();
            tx.setCommitTimestamp(commitTimestamp);

            // Move transaction from active to committed
            activeTransactions.remove(tx.getId());
            committedTransactions.put(tx.getId(), tx);

            LOGGER.fine("Committed transaction " + tx.getId());

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks for conflicts with active and committed transactions.
     *
     * @param tx The transaction to check
     * @throws Transaction.TransactionConflictException If a conflict is detected
     */
    private void checkConflicts(AbstractTransaction tx) throws Transaction.TransactionConflictException {
        // Implement conflict detection based on the transaction's isolation level
        switch (tx.getIsolationLevel()) {
            case SNAPSHOT_ISOLATION:
                checkSnapshotIsolationConflicts(tx);
                break;
            case SERIALIZABLE:
                // TODO: Implement serializable conflict checking
                break;
            case READ_COMMITTED:
                // No conflict checking needed for read committed
                break;
        }
    }

    /**
     * Checks for conflicts under snapshot isolation.
     *
     * @param tx The transaction to check
     * @throws Transaction.TransactionConflictException If a conflict is detected
     */
    private void checkSnapshotIsolationConflicts(AbstractTransaction tx) throws Transaction.TransactionConflictException {
        Timestamp startTs = tx.getStartTimestamp();

        // Check for write-write conflicts with committed transactions
        for (AbstractTransaction other : committedTransactions.values()) {
            if (other.getCommitTimestamp().isAfter(startTs)) {
                // Other transaction committed after this one started

                // Check for overlapping write sets
                for (byte[] key : tx.getModifiedKeys()) {
                    if (other.hasModified(key)) {
                        throw new Transaction.TransactionConflictException(
                                "Write-write conflict detected with transaction " + other.getId() +
                                " on key: " + new String(key));
                    }
                }
            }
        }

        // Check for write-write conflicts with active transactions
        for (AbstractTransaction other : activeTransactions.values()) {
            if (other == tx) {
                continue;
            }

            // Check for overlapping write sets
            for (byte[] key : tx.getModifiedKeys()) {
                if (other.hasModified(key)) {
                    throw new Transaction.TransactionConflictException(
                            "Write-write conflict detected with active transaction " + other.getId() +
                            " on key: " + new String(key));
                }
            }
        }
    }

    /**
     * Rolls back a transaction.
     *
     * @param tx The transaction to roll back
     */
    public void rollbackTransaction(AbstractTransaction tx) {
        if (!tx.isActive()) {
            return;
        }

        activeTransactions.remove(tx.getId());
        tx.setRolledBack(true);

        LOGGER.fine("Rolled back transaction " + tx.getId());
    }

    /**
     * Gets a transaction by ID.
     *
     * @param txId The transaction ID
     * @return The transaction, or null if not found
     */
    public AbstractTransaction getTransaction(String txId) {
        AbstractTransaction tx = activeTransactions.get(txId);
        if (tx == null) {
            tx = committedTransactions.get(txId);
        }
        return tx;
    }

    /**
     * Gets all active transactions.
     *
     * @return Map of active transactions
     */
    public Map<String, AbstractTransaction> getActiveTransactions() {
        return new HashMap<>(activeTransactions);
    }

    /**
     * Gets all committed transactions.
     *
     * @return Map of committed transactions
     */
    public Map<String, AbstractTransaction> getCommittedTransactions() {
        return new HashMap<>(committedTransactions);
    }

    /**
     * Cleans up old committed transactions.
     *
     * @param olderThan Transactions older than this timestamp will be removed
     */
    public void cleanupCommittedTransactions(Timestamp olderThan) {
        lock.writeLock().lock();
        try {
            committedTransactions.entrySet().removeIf(entry -> {
                AbstractTransaction tx = entry.getValue();
                return tx.getCommitTimestamp() != null && tx.getCommitTimestamp().isBefore(olderThan);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Aborts all active transactions.
     */
    public void abortAllActiveTransactions() {
        lock.writeLock().lock();
        try {
            for (AbstractTransaction tx : activeTransactions.values()) {
                tx.setRolledBack(true);
            }
            activeTransactions.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Abstract base class for transaction implementations.
     */
    public abstract static class AbstractTransaction implements Transaction {
        /** The transaction ID */
        protected final String id;

        /** The start timestamp */
        protected final Timestamp startTimestamp;

        /** Flag indicating whether this transaction is read-only */
        protected final boolean readOnly;

        /** The transaction manager */
        protected final TransactionManager txManager;

        /** The isolation level */
        protected final IsolationLevel isolationLevel;

        /** The commit timestamp */
        protected Timestamp commitTimestamp;

        /** Flag indicating whether this transaction has been rolled back */
        protected boolean rolledBack;

        /** Map of modified keys to values */
        protected final Map<ByteArrayWrapper, byte[]> writeSet = new HashMap<>();

        /** Set of deleted keys */
        protected final Map<ByteArrayWrapper, Boolean> deleteSet = new HashMap<>();

        /**
         * Creates a new abstract transaction.
         *
         * @param id The transaction ID
         * @param startTimestamp The start timestamp
         * @param readOnly Whether this transaction is read-only
         * @param txManager The transaction manager
         * @param isolationLevel The isolation level
         */
        protected AbstractTransaction(String id, Timestamp startTimestamp, boolean readOnly,
                                     TransactionManager txManager, IsolationLevel isolationLevel) {
            this.id = id;
            this.startTimestamp = startTimestamp;
            this.readOnly = readOnly;
            this.txManager = txManager;
            this.isolationLevel = isolationLevel;
            this.commitTimestamp = null;
            this.rolledBack = false;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public Timestamp getStartTimestamp() {
            return startTimestamp;
        }

        @Override
        public Timestamp getCommitTimestamp() {
            return commitTimestamp;
        }

        /**
         * Sets the commit timestamp.
         *
         * @param commitTimestamp The commit timestamp
         */
        public void setCommitTimestamp(Timestamp commitTimestamp) {
            this.commitTimestamp = commitTimestamp;
        }

        /**
         * Sets whether this transaction has been rolled back.
         *
         * @param rolledBack Whether this transaction has been rolled back
         */
        public void setRolledBack(boolean rolledBack) {
            this.rolledBack = rolledBack;
        }

        @Override
        public IsolationLevel getIsolationLevel() {
            return isolationLevel;
        }

        @Override
        public boolean isReadOnly() {
            return readOnly;
        }

        @Override
        public boolean isActive() {
            return commitTimestamp == null && !rolledBack;
        }

        @Override
        public boolean isCommitted() {
            return commitTimestamp != null;
        }

        @Override
        public boolean isRolledBack() {
            return rolledBack;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            if (!isActive()) {
                throw new IllegalStateException("Transaction is not active");
            }

            if (readOnly) {
                throw new IllegalStateException("Cannot write in a read-only transaction");
            }

            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            writeSet.put(keyWrapper, value);
            deleteSet.remove(keyWrapper);
        }

        @Override
        public boolean delete(byte[] key) {
            if (!isActive()) {
                throw new IllegalStateException("Transaction is not active");
            }

            if (readOnly) {
                throw new IllegalStateException("Cannot delete in a read-only transaction");
            }

            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            writeSet.remove(keyWrapper);
            deleteSet.put(keyWrapper, true);

            return true;  // Actual verification of key existence happens at commit time
        }

        @Override
        public Map<byte[], byte[]> getModifiedEntries() {
            Map<byte[], byte[]> result = new HashMap<>();
            for (Map.Entry<ByteArrayWrapper, byte[]> entry : writeSet.entrySet()) {
                result.put(entry.getKey().getData(), entry.getValue());
            }
            return result;
        }

        @Override
        public Set<byte[]> getDeletedKeys() {
            Map<byte[], Boolean> result = new HashMap<>();
            for (Map.Entry<ByteArrayWrapper, Boolean> entry : deleteSet.entrySet()) {
                result.put(entry.getKey().getData(), true);
            }
            return result.keySet();
        }

        /**
         * Gets the set of keys that have been modified or deleted in this transaction.
         *
         * @return Set of modified or deleted keys
         */
        public Set<byte[]> getModifiedKeys() {
            Map<byte[], Boolean> result = new HashMap<>();
            for (ByteArrayWrapper key : writeSet.keySet()) {
                result.put(key.getData(), true);
            }
            for (ByteArrayWrapper key : deleteSet.keySet()) {
                result.put(key.getData(), true);
            }
            return result.keySet();
        }

        /**
         * Checks if the specified key has been modified or deleted in this transaction.
         *
         * @param key The key
         * @return True if the key has been modified or deleted
         */
        public boolean hasModified(byte[] key) {
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            return writeSet.containsKey(keyWrapper) || deleteSet.containsKey(keyWrapper);
        }

        @Override
        public boolean commit() throws TransactionConflictException {
            if (!isActive()) {
                return false;
            }

            return txManager.commitTransaction(this);
        }

        @Override
        public void rollback() {
            txManager.rollbackTransaction(this);
        }
    }

    /**
     * Wrapper for byte arrays to use them as keys in maps.
     */
    public static class ByteArrayWrapper {
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
}
