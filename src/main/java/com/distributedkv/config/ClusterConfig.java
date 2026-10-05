package com.distributedkv.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Configuration for the cluster
 */
public class ClusterConfig {
    private final List<String> seedNodes;
    private final int shardCount;
    private final int replicationFactor;
    private final int virtualNodesPerNode;
    private final boolean autoRebalance;
    private final int minNodesForWrite;
    private final String membershipProtocol;
    private final long membershipTimeoutMs;
    private final String shardStrategy;
    private final String placementStrategy;
    private final double rebalanceThreshold;
    private final int maxRebalanceTasks;
    private final int rebalanceBatchSize;
    private final long rebalanceThrottleBytesPerSec;

    /**
     * Constructs a ClusterConfig from properties
     *
     * @param properties the configuration properties
     */
    public ClusterConfig(Properties properties) {
        String seedNodesStr = properties.getProperty("cluster.seed.nodes", "localhost:10000");
        this.seedNodes = Arrays.stream(seedNodesStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        this.shardCount = Integer.parseInt(properties.getProperty("cluster.shard.count", "16"));
        this.replicationFactor = Integer.parseInt(properties.getProperty("cluster.replication.factor", "3"));
        this.virtualNodesPerNode = Integer.parseInt(properties.getProperty("cluster.virtual.nodes.per.node", "100"));
        this.autoRebalance = Boolean.parseBoolean(properties.getProperty("cluster.auto.rebalance", "true"));
        this.minNodesForWrite = Integer.parseInt(properties.getProperty("cluster.min.nodes.for.write", "2"));
        this.membershipProtocol = properties.getProperty("cluster.membership.protocol", "RAFT");
        this.membershipTimeoutMs = Long.parseLong(properties.getProperty("cluster.membership.timeout.ms", "10000"));
        this.shardStrategy = properties.getProperty("cluster.shard.strategy", "CONSISTENT_HASH");
        this.placementStrategy = properties.getProperty("cluster.placement.strategy", "RACK_AWARE");
        this.rebalanceThreshold = Double.parseDouble(properties.getProperty("cluster.rebalance.threshold", "0.1"));
        this.maxRebalanceTasks = Integer.parseInt(properties.getProperty("cluster.max.rebalance.tasks", "2"));
        this.rebalanceBatchSize = Integer.parseInt(properties.getProperty("cluster.rebalance.batch.size", "1000"));
        this.rebalanceThrottleBytesPerSec = Long.parseLong(
                properties.getProperty("cluster.rebalance.throttle.bytes.per.sec", "10485760"));
    }

    /**
     * Gets the seed nodes for cluster bootstrap
     *
     * @return list of seed nodes
     */
    public List<String> getSeedNodes() {
        return new ArrayList<>(seedNodes);
    }

    /**
     * Gets the number of shards in the cluster
     *
     * @return shard count
     */
    public int getShardCount() {
        return shardCount;
    }

    /**
     * Gets the replication factor for data
     *
     * @return replication factor
     */
    public int getReplicationFactor() {
        return replicationFactor;
    }

    /**
     * Gets the number of virtual nodes per physical node
     *
     * @return virtual nodes per node
     */
    public int getVirtualNodesPerNode() {
        return virtualNodesPerNode;
    }

    /**
     * Checks if automatic rebalancing is enabled
     *
     * @return true if auto rebalance is enabled
     */
    public boolean isAutoRebalance() {
        return autoRebalance;
    }

    /**
     * Gets the minimum number of nodes required for write operations
     *
     * @return minimum nodes for write
     */
    public int getMinNodesForWrite() {
        return minNodesForWrite;
    }

    /**
     * Gets the membership protocol (e.g., RAFT, GOSSIP)
     *
     * @return membership protocol
     */
    public String getMembershipProtocol() {
        return membershipProtocol;
    }

    /**
     * Gets the membership timeout in milliseconds
     *
     * @return membership timeout
     */
    public long getMembershipTimeoutMs() {
        return membershipTimeoutMs;
    }

    /**
     * Gets the sharding strategy (e.g., CONSISTENT_HASH)
     *
     * @return shard strategy
     */
    public String getShardStrategy() {
        return shardStrategy;
    }

    /**
     * Gets the node placement strategy (e.g., RACK_AWARE)
     *
     * @return placement strategy
     */
    public String getPlacementStrategy() {
        return placementStrategy;
    }

    /**
     * Gets the rebalance threshold
     *
     * @return rebalance threshold
     */
    public double getRebalanceThreshold() {
        return rebalanceThreshold;
    }

    /**
     * Gets the maximum number of concurrent rebalance tasks
     *
     * @return max rebalance tasks
     */
    public int getMaxRebalanceTasks() {
        return maxRebalanceTasks;
    }

    /**
     * Gets the batch size for rebalance operations
     *
     * @return rebalance batch size
     */
    public int getRebalanceBatchSize() {
        return rebalanceBatchSize;
    }

    /**
     * Gets the throttle rate for rebalance operations in bytes per second
     *
     * @return rebalance throttle rate
     */
    public long getRebalanceThrottleBytesPerSec() {
        return rebalanceThrottleBytesPerSec;
    }

    @Override
    public String toString() {
        return "ClusterConfig{" +
                "seedNodes=" + seedNodes +
                ", shardCount=" + shardCount +
                ", replicationFactor=" + replicationFactor +
                ", virtualNodesPerNode=" + virtualNodesPerNode +
                ", autoRebalance=" + autoRebalance +
                ", minNodesForWrite=" + minNodesForWrite +
                ", membershipProtocol='" + membershipProtocol + '\'' +
                ", membershipTimeoutMs=" + membershipTimeoutMs +
                ", shardStrategy='" + shardStrategy + '\'' +
                ", placementStrategy='" + placementStrategy + '\'' +
                ", rebalanceThreshold=" + rebalanceThreshold +
                ", maxRebalanceTasks=" + maxRebalanceTasks +
                ", rebalanceBatchSize=" + rebalanceBatchSize +
                ", rebalanceThrottleBytesPerSec=" + rebalanceThrottleBytesPerSec +
                '}';
    }
}
