package com.distributedkv.client;

import com.distributedkv.common.Result;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * KVClient is the interface for client operations on the distributed key-value store.
 * It provides both synchronous and asynchronous methods for get, put, delete, and other operations.
 */
public interface KVClient {

    /**
     * Opens a new session with the key-value store.
     *
     * @return A result containing the session or an error
     */
    Result<Session> openSession();

    /**
     * Closes the current session with the key-value store.
     *
     * @param session The session to close
     * @return A result indicating success or failure
     */
    Result<Void> closeSession(Session session);

    /**
     * Gets the value associated with the given key.
     *
     * @param key The key
     * @return A result containing the value or an error
     */
    Result<byte[]> get(String key);

    /**
     * Gets the value associated with the given key asynchronously.
     *
     * @param key The key
     * @return A future that will contain the result
     */
    CompletableFuture<Result<byte[]>> getAsync(String key);

    /**
     * Puts the given key-value pair into the store.
     *
     * @param key The key
     * @param value The value
     * @return A result indicating success or failure
     */
    Result<Void> put(String key, byte[] value);

    /**
     * Puts the given key-value pair into the store asynchronously.
     *
     * @param key The key
     * @param value The value
     * @return A future that will contain the result
     */
    CompletableFuture<Result<Void>> putAsync(String key, byte[] value);

    /**
     * Deletes the value associated with the given key.
     *
     * @param key The key
     * @return A result indicating success or failure
     */
    Result<Void> delete(String key);

    /**
     * Deletes the value associated with the given key asynchronously.
     *
     * @param key The key
     * @return A future that will contain the result
     */
    CompletableFuture<Result<Void>> deleteAsync(String key);

    /**
     * Gets multiple values in a batch operation.
     *
     * @param keys The list of keys
     * @return A result containing a map of keys to values or an error
     */
    Result<Map<String, byte[]>> multiGet(List<String> keys);

    /**
     * Gets multiple values in a batch operation asynchronously.
     *
     * @param keys The list of keys
     * @return A future that will contain the result
     */
    CompletableFuture<Result<Map<String, byte[]>>> multiGetAsync(List<String> keys);

    /**
     * Puts multiple key-value pairs in a batch operation.
     *
     * @param keyValues The map of keys to values
     * @return A result indicating success or failure
     */
    Result<Void> multiPut(Map<String, byte[]> keyValues);

    /**
     * Puts multiple key-value pairs in a batch operation asynchronously.
     *
     * @param keyValues The map of keys to values
     * @return A future that will contain the result
     */
    CompletableFuture<Result<Void>> multiPutAsync(Map<String, byte[]> keyValues);

    /**
     * Performs a compare-and-set operation on the given key.
     *
     * @param key The key
     * @param expectedValue The expected current value
     * @param newValue The new value to set if the current value matches the expected value
     * @return A result indicating success or failure
     */
    Result<Boolean> compareAndSet(String key, byte[] expectedValue, byte[] newValue);

    /**
     * Performs a compare-and-set operation on the given key asynchronously.
     *
     * @param key The key
     * @param expectedValue The expected current value
     * @param newValue The new value to set if the current value matches the expected value
     * @return A future that will contain the result
     */
    CompletableFuture<Result<Boolean>> compareAndSetAsync(String key, byte[] expectedValue, byte[] newValue);

    /**
     * Begins a new transaction.
     *
     * @return A result containing the transaction ID or an error
     */
    Result<String> beginTransaction();

    /**
     * Commits the transaction with the given ID.
     *
     * @param transactionId The transaction ID
     * @return A result indicating success or failure
     */
    Result<Void> commitTransaction(String transactionId);

    /**
     * Aborts the transaction with the given ID.
     *
     * @param transactionId The transaction ID
     * @return A result indicating success or failure
     */
    Result<Void> abortTransaction(String transactionId);

    /**
     * Gets the value associated with the given key within the given transaction.
     *
     * @param transactionId The transaction ID
     * @param key The key
     * @return A result containing the value or an error
     */
    Result<byte[]> getWithinTransaction(String transactionId, String key);

    /**
     * Puts the given key-value pair into the store within the given transaction.
     *
     * @param transactionId The transaction ID
     * @param key The key
     * @param value The value
     * @return A result indicating success or failure
     */
    Result<Void> putWithinTransaction(String transactionId, String key, byte[] value);

    /**
     * Deletes the value associated with the given key within the given transaction.
     *
     * @param transactionId The transaction ID
     * @param key The key
     * @return A result indicating success or failure
     */
    Result<Void> deleteWithinTransaction(String transactionId, String key);

    /**
     * Gets the range of keys between the given start and end keys.
     *
     * @param startKey The start key (inclusive)
     * @param endKey The end key (exclusive)
     * @return A result containing a map of keys to values or an error
     */
    Result<Map<String, byte[]>> getRange(String startKey, String endKey);

    /**
     * Gets the range of keys between the given start and end keys asynchronously.
     *
     * @param startKey The start key (inclusive)
     * @param endKey The end key (exclusive)
     * @return A future that will contain the result
     */
    CompletableFuture<Result<Map<String, byte[]>>> getRangeAsync(String startKey, String endKey);

    /**
     * Gets the keys with the given prefix.
     *
     * @param prefix The prefix
     * @return A result containing a map of keys to values or an error
     */
    Result<Map<String, byte[]>> getByPrefix(String prefix);

    /**
     * Gets the keys with the given prefix asynchronously.
     *
     * @param prefix The prefix
     * @return A future that will contain the result
     */
    CompletableFuture<Result<Map<String, byte[]>>> getByPrefixAsync(String prefix);

    /**
     * Subscribes to changes for the given key.
     *
     * @param key The key
     * @param callback The callback to invoke when the key changes
     * @return A result containing the subscription ID or an error
     */
    Result<String> subscribe(String key, KeyChangeCallback callback);

    /**
     * Unsubscribes from changes for the given subscription ID.
     *
     * @param subscriptionId The subscription ID
     * @return A result indicating success or failure
     */
    Result<Void> unsubscribe(String subscriptionId);

    /**
     * Callback interface for key change notifications.
     */
    interface KeyChangeCallback {
        /**
         * Called when a key changes.
         *
         * @param key The key that changed
         * @param value The new value
         */
        void onKeyChange(String key, byte[] value);
    }

    /**
     * Closes the client and releases all resources.
     *
     * @return A result indicating success or failure
     */
    Result<Void> close();
}
