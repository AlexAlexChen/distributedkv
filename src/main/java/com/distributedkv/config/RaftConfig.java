package com.distributedkv.config;

import java.util.Properties;

/**
 * Configuration for the Raft consensus algorithm
 */
public class RaftConfig {
    private final int electionTimeoutMs;
    private final int minElectionTimeoutMs;
    private final int maxElectionTimeoutMs;
    private final int heartbeatIntervalMs;
    private final int snapshotIntervalSeconds;
    private final int maxLogEntriesPerRequest;
    private final int applyBatchSize;
    private final int followerReadTimeoutMs;
    private final boolean enableFollowerRead;
    private final boolean enableAsyncApply;
    private final boolean preVote;
    private final int maxSnapshotConcurrent;
    private final boolean logSyncBatch;
    private final int logSyncIntervalMs;
    private final boolean readIndexEnabled;
    private final int readIndexTimeoutMs;
    private final boolean readIndexBatchEnabled;
    private final boolean followerReadLeaseEnabled;
    private final int followerReadLeaseDurationMs;

    /**
     * Constructs a RaftConfig from properties
     *
     * @param properties the configuration properties
     */
    public RaftConfig(Properties properties) {
        this.electionTimeoutMs = Integer.parseInt(properties.getProperty("raft.election.timeout.ms", "1000"));
        this.minElectionTimeoutMs = Integer.parseInt(properties.getProperty("raft.election.timeout.min.ms", "1000"));
        this.maxElectionTimeoutMs = Integer.parseInt(properties.getProperty("raft.election.timeout.max.ms", "2000"));
        this.heartbeatIntervalMs = Integer.parseInt(properties.getProperty("raft.heartbeat.interval.ms", "500"));
        this.snapshotIntervalSeconds = Integer.parseInt(properties.getProperty("raft.snapshot.interval.seconds", "3600"));
        this.maxLogEntriesPerRequest = Integer.parseInt(properties.getProperty("raft.max.log.entries.per.request", "1000"));
        this.applyBatchSize = Integer.parseInt(properties.getProperty("raft.apply.batch.size", "100"));
        this.followerReadTimeoutMs = Integer.parseInt(properties.getProperty("raft.follower.read.timeout.ms", "1000"));
        this.enableFollowerRead = Boolean.parseBoolean(properties.getProperty("raft.enable.follower.read", "true"));
        this.enableAsyncApply = Boolean.parseBoolean(properties.getProperty("raft.enable.async.apply", "true"));
        this.preVote = Boolean.parseBoolean(properties.getProperty("raft.pre.vote", "true"));
        this.maxSnapshotConcurrent = Integer.parseInt(properties.getProperty("raft.max.snapshot.concurrent", "3"));
        this.logSyncBatch = Boolean.parseBoolean(properties.getProperty("raft.log.sync.batch", "true"));
        this.logSyncIntervalMs = Integer.parseInt(properties.getProperty("raft.log.sync.interval.ms", "10"));
        this.readIndexEnabled = Boolean.parseBoolean(properties.getProperty("raft.enable.read.index", "true"));
        this.readIndexTimeoutMs = Integer.parseInt(properties.getProperty("raft.read.index.timeout.ms", "1000"));
        this.readIndexBatchEnabled = Boolean.parseBoolean(properties.getProperty("raft.read.index.batch.enabled", "true"));
        this.followerReadLeaseEnabled = Boolean.parseBoolean(properties.getProperty("raft.follower.read.lease.enabled", "true"));
        this.followerReadLeaseDurationMs = Integer.parseInt(properties.getProperty("raft.follower.read.lease.duration.ms", "9000"));
    }

    /**
     * Gets the election timeout in milliseconds
     *
     * @return election timeout
     */
    public int getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    /**
     * Gets the minimum election timeout in milliseconds.
     * The election timer randomizes within [min, max] to avoid split votes.
     *
     * @return minimum election timeout
     */
    public int getMinElectionTimeoutMs() {
        return minElectionTimeoutMs;
    }

    /**
     * Gets the maximum election timeout in milliseconds.
     * The election timer randomizes within [min, max] to avoid split votes.
     *
     * @return maximum election timeout
     */
    public int getMaxElectionTimeoutMs() {
        return maxElectionTimeoutMs;
    }

    /**
     * Gets the heartbeat interval in milliseconds
     *
     * @return heartbeat interval
     */
    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    /**
     * Gets the snapshot interval in seconds
     *
     * @return snapshot interval
     */
    public int getSnapshotIntervalSeconds() {
        return snapshotIntervalSeconds;
    }

    /**
     * Gets the maximum number of log entries per request
     *
     * @return max log entries per request
     */
    public int getMaxLogEntriesPerRequest() {
        return maxLogEntriesPerRequest;
    }

    /**
     * Gets the batch size for applying log entries
     *
     * @return apply batch size
     */
    public int getApplyBatchSize() {
        return applyBatchSize;
    }

    /**
     * Gets the follower read timeout in milliseconds
     *
     * @return follower read timeout
     */
    public int getFollowerReadTimeoutMs() {
        return followerReadTimeoutMs;
    }

    /**
     * Checks if follower read is enabled
     *
     * @return true if follower read is enabled
     */
    public boolean isEnableFollowerRead() {
        return enableFollowerRead;
    }

    /**
     * Checks if async apply is enabled
     *
     * @return true if async apply is enabled
     */
    public boolean isEnableAsyncApply() {
        return enableAsyncApply;
    }

    /**
     * Checks if pre-vote is enabled
     *
     * @return true if pre-vote is enabled
     */
    public boolean isPreVote() {
        return preVote;
    }

    /**
     * Gets the maximum number of concurrent snapshots
     *
     * @return max concurrent snapshots
     */
    public int getMaxSnapshotConcurrent() {
        return maxSnapshotConcurrent;
    }

    /**
     * Checks if log sync batching is enabled
     *
     * @return true if log sync batching is enabled
     */
    public boolean isLogSyncBatch() {
        return logSyncBatch;
    }

    /**
     * Gets the log sync interval in milliseconds
     *
     * @return log sync interval
     */
    public int getLogSyncIntervalMs() {
        return logSyncIntervalMs;
    }

    /**
     * Checks if ReadIndex is enabled
     *
     * @return true if ReadIndex is enabled
     */
    public boolean isReadIndexEnabled() {
        return readIndexEnabled;
    }

    /**
     * Gets the ReadIndex timeout in milliseconds
     *
     * @return ReadIndex timeout
     */
    public int getReadIndexTimeoutMs() {
        return readIndexTimeoutMs;
    }

    /**
     * Checks if ReadIndex batching is enabled
     *
     * @return true if ReadIndex batching is enabled
     */
    public boolean isReadIndexBatchEnabled() {
        return readIndexBatchEnabled;
    }

    /**
     * Checks if follower read lease is enabled
     *
     * @return true if follower read lease is enabled
     */
    public boolean isFollowerReadLeaseEnabled() {
        return followerReadLeaseEnabled;
    }

    /**
     * Gets the follower read lease duration in milliseconds
     *
     * @return follower read lease duration
     */
    public int getFollowerReadLeaseDurationMs() {
        return followerReadLeaseDurationMs;
    }

    @Override
    public String toString() {
        return "RaftConfig{" +
                "electionTimeoutMs=" + electionTimeoutMs +
                ", heartbeatIntervalMs=" + heartbeatIntervalMs +
                ", snapshotIntervalSeconds=" + snapshotIntervalSeconds +
                ", maxLogEntriesPerRequest=" + maxLogEntriesPerRequest +
                ", applyBatchSize=" + applyBatchSize +
                ", followerReadTimeoutMs=" + followerReadTimeoutMs +
                ", enableFollowerRead=" + enableFollowerRead +
                ", enableAsyncApply=" + enableAsyncApply +
                ", preVote=" + preVote +
                ", maxSnapshotConcurrent=" + maxSnapshotConcurrent +
                ", logSyncBatch=" + logSyncBatch +
                ", logSyncIntervalMs=" + logSyncIntervalMs +
                ", readIndexEnabled=" + readIndexEnabled +
                ", readIndexTimeoutMs=" + readIndexTimeoutMs +
                ", readIndexBatchEnabled=" + readIndexBatchEnabled +
                ", followerReadLeaseEnabled=" + followerReadLeaseEnabled +
                ", followerReadLeaseDurationMs=" + followerReadLeaseDurationMs +
                '}';
    }
}
