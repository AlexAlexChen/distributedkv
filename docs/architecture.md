# DistributedKV Architecture

## Overview

DistributedKV is a high-performance distributed key-value storage system designed for reliability, scalability, and linearizable consistency. The system is built on a Multi-Raft architecture, which provides strong consistency guarantees while maintaining high availability (3 Nines, or 99.9%) and supporting up to 50K queries per second (QPS).

## System Architecture

![Architecture Diagram](images/architecture.png)

The system is composed of the following major components:

### 1. Client Layer

- **KVClient**: Client interface for application integration
- **Session Management**: Maintains client connections and provides retry logic
- **Request Routing**: Routes requests to the appropriate node using consistent hashing

### 2. Server Layer

- **KVServer**: Main server component that handles client requests
- **RPC Framework**: Netty-based networking for high-performance communication
- **Cluster Management**: Manages node membership and cluster topology

### 3. Consensus Layer (Multi-Raft)

- **RaftGroupManager**: Manages multiple Raft groups for different key ranges
- **RaftGroup**: Individual Raft consensus group for a key range
- **RaftNode**: Single node in a Raft consensus group
- **Log Management**: Persistent storage for the Raft log
- **StateMachine**: Applies committed log entries to the storage engine

### 4. Storage Layer

- **StorageManager**: Manages different storage engines
- **MVCC**: Multi-Version Concurrency Control for concurrent operations
- **Storage Engines**:
  - **RocksDB**: LSM-tree based storage for high write throughput
  - **B+ Tree**: For range queries and ordered scans
  - **Hash Table**: For simple key-value lookups with low memory footprint

### 5. Sharding and Data Distribution

- **Consistent Hashing**: Distributes keys evenly across Raft groups
- **ShardManager**: Manages shard allocation and migration
- **DataMigrator**: Handles data movement between nodes during rebalancing

## Multi-Raft Architecture

The Multi-Raft architecture divides the key space into multiple shards, each managed by a separate Raft consensus group. This approach provides several benefits:

1. **Scalability**: By partitioning the key space, the system can scale horizontally
2. **Performance**: Multiple Raft groups can process requests in parallel
3. **Failure Isolation**: Failures are isolated to specific Raft groups

![Multi-Raft Diagram](images/multi-raft.png)

Each Raft group consists of multiple nodes (typically 3 or 5) that replicate data for fault tolerance. One node in each group serves as the leader, which handles all write operations and coordinated read operations for that group.

## Data Consistency and Availability

DistributedKV provides linearizable consistency for all operations. The system guarantees that:

1. All operations appear to execute atomically
2. All operations respect real-time ordering (if operation A completes before operation B begins, then B will see the effects of A)

To maintain high availability (99.9% uptime), the system employs several strategies:

1. **Replication**: Each piece of data is replicated across multiple nodes
2. **Automatic Leader Election**: If a leader fails, a new leader is automatically elected
3. **Failure Detection**: Periodic heartbeats detect node failures
4. **Automatic Recovery**: Failed nodes automatically recover and rejoin the cluster

## Read and Write Path

### Write Path

1. Client sends a write request to any node
2. The node uses consistent hashing to determine the Raft group responsible for the key
3. The request is forwarded to the leader of the Raft group
4. The leader appends the write operation to its log
5. The operation is replicated to followers in the Raft group
6. Once a majority of nodes have confirmed replication, the operation is committed
7. The leader applies the operation to its state machine (storage engine)
8. The leader returns success to the client

### Read Path

DistributedKV supports multiple read mechanisms:

1. **Leader Read**:
   - Provides linearizable consistency
   - All reads go through the leader of the Raft group

2. **ReadIndex**:
   - Provides linearizable consistency without the overhead of log replication
   - The leader confirms its leadership with a quorum of nodes
   - The leader provides a read index to guarantee the read is linearizable

3. **FollowerRead**:
   - Reduces read latency by allowing reads from follower nodes
   - Suitable for read-heavy workloads
   - May provide slightly stale data depending on configuration

## Performance Optimizations

DistributedKV implements several optimizations to achieve high performance:

1. **Async Apply**:
   - Log entries are applied asynchronously to the state machine
   - Reduces blocking and improves throughput

2. **Batching**:
   - Multiple operations are batched for efficiency
   - Reduces network overhead and improves throughput

3. **Pre-Vote**:
   - Reduces unnecessary leadership changes
   - Improves stability in case of network partitions

4. **Follower Read**:
   - Allows read operations from follower nodes
   - Reduces latency and distributes load
   - Typically provides 40-45% improvement in read latency

5. **MVCC (Multi-Version Concurrency Control)**:
   - Enables concurrent reads and writes
   - Provides snapshot isolation for read operations
   - Reduces contention and improves throughput

## Data Distribution and Load Balancing

DistributedKV uses consistent hashing to distribute data evenly across Raft groups:

1. **Consistent Hashing**:
   - Minimizes data movement when nodes are added or removed
   - Uses virtual nodes to improve distribution
   - Provides configurable replication factor

2. **Dynamic Rebalancing**:
   - Automatically rebalances data when nodes are added or removed
   - Uses background data migration for minimal impact on performance
   - Maintains consistency during migration

## System Monitoring and Management

DistributedKV provides comprehensive monitoring and management capabilities:

1. **Metrics Collection**:
   - Throughput, latency, error rates
   - Resource utilization (CPU, memory, disk, network)
   - Raft-specific metrics (leadership changes, log size, etc.)

2. **Administration Tools**:
   - Cluster management (add/remove nodes)
   - Data migration and rebalancing
   - Backup and restore
   - Performance tuning

## Failure Scenarios and Recovery

The system is designed to handle various failure scenarios:

1. **Node Failure**:
   - Automatic leader election in affected Raft groups
   - Automatic recovery when the node comes back online
   - Data replication to maintain redundancy

2. **Network Partition**:
   - Raft consensus ensures the system remains consistent
   - Only the partition with the majority can make progress
   - Automatic recovery when the partition is resolved

3. **Disk Failure**:
   - Data replication across nodes provides redundancy
   - Failed node can recover by copying data from other nodes
   - Hot standby nodes can be promoted to maintain availability

## References

1. Raft Consensus Algorithm: https://raft.github.io/
2. Consistent Hashing and Random Trees: Distributed Caching Protocols for Relieving Hot Spots on the World Wide Web
3. RocksDB: A Persistent Key-Value Store for Flash and RAM Storage
