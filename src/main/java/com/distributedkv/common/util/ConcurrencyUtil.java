package com.distributedkv.common.util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * Utility class for concurrency operations
 */
public class ConcurrencyUtil {
    /**
     * Thread factory for creating named threads
     */
    private static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(0);
        private final boolean daemon;
        private final int priority;

        public NamedThreadFactory(String prefix, boolean daemon, int priority) {
            this.prefix = prefix;
            this.daemon = daemon;
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, prefix + "-" + counter.incrementAndGet());
            thread.setDaemon(daemon);
            thread.setPriority(priority);
            return thread;
        }
    }

    /**
     * Create a thread factory with customized settings
     *
     * @param namePrefix the prefix for thread names
     * @param daemon whether threads should be daemon threads
     * @param priority the thread priority
     * @return a thread factory
     */
    public static ThreadFactory createThreadFactory(String namePrefix, boolean daemon, int priority) {
        return new NamedThreadFactory(namePrefix, daemon, priority);
    }

    /**
     * Create a thread factory with default priority
     *
     * @param namePrefix the prefix for thread names
     * @param daemon whether threads should be daemon threads
     * @return a thread factory
     */
    public static ThreadFactory createThreadFactory(String namePrefix, boolean daemon) {
        return new NamedThreadFactory(namePrefix, daemon, Thread.NORM_PRIORITY);
    }

    /**
     * Create a thread factory with default settings
     *
     * @param namePrefix the prefix for thread names
     * @return a thread factory
     */
    public static ThreadFactory createThreadFactory(String namePrefix) {
        return new NamedThreadFactory(namePrefix, false, Thread.NORM_PRIORITY);
    }

    /**
     * Execute a task with a timeout
     *
     * @param <T> the return type
     * @param task the task to execute
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @return the task result
     * @throws TimeoutException if the task times out
     * @throws InterruptedException if the task is interrupted
     * @throws ExecutionException if the task throws an exception
     */
    public static <T> T executeWithTimeout(Callable<T> task, long timeout, TimeUnit unit)
            throws TimeoutException, InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<T> future = executor.submit(task);
            return future.get(timeout, unit);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Execute a task with a timeout, returning a default value on timeout
     *
     * @param <T> the return type
     * @param task the task to execute
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @param defaultValue the default value to return on timeout
     * @return the task result or the default value on timeout
     * @throws InterruptedException if the task is interrupted
     * @throws ExecutionException if the task throws an exception
     */
    public static <T> T executeWithTimeout(Callable<T> task, long timeout, TimeUnit unit, T defaultValue)
            throws InterruptedException, ExecutionException {
        try {
            return executeWithTimeout(task, timeout, unit);
        } catch (TimeoutException e) {
            return defaultValue;
        }
    }

    /**
     * Execute a task with retry logic
     *
     * @param <T> the return type
     * @param task the task to execute
     * @param maxRetries the maximum number of retries
     * @param retryDelayMs the delay between retries in milliseconds
     * @return the task result
     * @throws ExecutionException if all retries fail
     */
    public static <T> T executeWithRetry(Callable<T> task, int maxRetries, long retryDelayMs)
            throws ExecutionException {
        int attempts = 0;
        Exception lastException = null;

        while (attempts <= maxRetries) {
            attempts++;

            try {
                return task.call();
            } catch (Exception e) {
                lastException = e;

                if (attempts <= maxRetries) {
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new ExecutionException("Retry interrupted", ie);
                    }
                }
            }
        }

        throw new ExecutionException("Failed after " + attempts + " attempts", lastException);
    }

    /**
     * Execute a task with retry logic and exponential backoff
     *
     * @param <T> the return type
     * @param task the task to execute
     * @param maxRetries the maximum number of retries
     * @param initialDelayMs the initial delay between retries in milliseconds
     * @param maxDelayMs the maximum delay between retries in milliseconds
     * @return the task result
     * @throws ExecutionException if all retries fail
     */
    public static <T> T executeWithExponentialBackoff(Callable<T> task, int maxRetries,
            long initialDelayMs, long maxDelayMs) throws ExecutionException {
        int attempts = 0;
        Exception lastException = null;
        long delay = initialDelayMs;

        while (attempts <= maxRetries) {
            attempts++;

            try {
                return task.call();
            } catch (Exception e) {
                lastException = e;

                if (attempts <= maxRetries) {
                    try {
                        Thread.sleep(delay);
                        delay = Math.min(delay * 2, maxDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new ExecutionException("Retry interrupted", ie);
                    }
                }
            }
        }

        throw new ExecutionException("Failed after " + attempts + " attempts", lastException);
    }

    /**
     * Run a task in a separate thread
     *
     * @param task the task to run
     * @return a future representing the task
     */
    public static Future<?> runAsync(Runnable task) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(task);
        executor.shutdown();
        return future;
    }

    /**
     * Run a callable in a separate thread
     *
     * @param <T> the return type
     * @param task the task to run
     * @return a future representing the task
     */
    public static <T> Future<T> callAsync(Callable<T> task) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit(task);
        executor.shutdown();
        return future;
    }

    /**
     * Execute a function with a lock
     *
     * @param <T> the return type
     * @param lock the lock to acquire
     * @param supplier the function to execute
     * @return the function result
     */
    public static <T> T withLock(Lock lock, Supplier<T> supplier) {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Execute a task with a lock
     *
     * @param lock the lock to acquire
     * @param runnable the task to execute
     */
    public static void withLock(Lock lock, Runnable runnable) {
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Try to execute a function with a lock
     *
     * @param <T> the return type
     * @param lock the lock to acquire
     * @param supplier the function to execute
     * @param defaultValue the default value to return if the lock can't be acquired
     * @return the function result or the default value
     */
    public static <T> T withTryLock(Lock lock, Supplier<T> supplier, T defaultValue) {
        if (lock.tryLock()) {
            try {
                return supplier.get();
            } finally {
                lock.unlock();
            }
        }
        return defaultValue;
    }

    /**
     * Try to execute a task with a lock
     *
     * @param lock the lock to acquire
     * @param runnable the task to execute
     * @return true if the lock was acquired and the task executed, false otherwise
     */
    public static boolean withTryLock(Lock lock, Runnable runnable) {
        if (lock.tryLock()) {
            try {
                runnable.run();
                return true;
            } finally {
                lock.unlock();
            }
        }
        return false;
    }

    /**
     * Try to execute a function with a lock and timeout
     *
     * @param <T> the return type
     * @param lock the lock to acquire
     * @param supplier the function to execute
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @param defaultValue the default value to return if the lock can't be acquired
     * @return the function result or the default value
     * @throws InterruptedException if interrupted while waiting for the lock
     */
    public static <T> T withTryLock(Lock lock, Supplier<T> supplier, long timeout, TimeUnit unit, T defaultValue)
            throws InterruptedException {
        if (lock.tryLock(timeout, unit)) {
            try {
                return supplier.get();
            } finally {
                lock.unlock();
            }
        }
        return defaultValue;
    }

    /**
     * Try to execute a task with a lock and timeout
     *
     * @param lock the lock to acquire
     * @param runnable the task to execute
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @return true if the lock was acquired and the task executed, false otherwise
     * @throws InterruptedException if interrupted while waiting for the lock
     */
    public static boolean withTryLock(Lock lock, Runnable runnable, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (lock.tryLock(timeout, unit)) {
            try {
                runnable.run();
                return true;
            } finally {
                lock.unlock();
            }
        }
        return false;
    }

    /**
     * Shutdown an executor service gracefully
     *
     * @param executor the executor service
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @return true if the executor service terminated, false if it timed out
     */
    public static boolean shutdownGracefully(ExecutorService executor, long timeout, TimeUnit unit) {
        executor.shutdown();
        try {
            return executor.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Shutdown an executor service forcibly
     *
     * @param executor the executor service
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @return true if the executor service terminated, false if it timed out
     */
    public static boolean shutdownForcibly(ExecutorService executor, long timeout, TimeUnit unit) {
        executor.shutdownNow();
        try {
            return executor.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
