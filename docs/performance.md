# DistributedKV Performance Tuning Guide

This document provides guidelines and best practices for optimizing the performance of your DistributedKV system to achieve maximum throughput and minimum latency.

## Table of Contents

1. [Performance Overview](#performance-overview)
2. [Hardware Considerations](#hardware-considerations)
3. [JVM Tuning](#jvm-tuning)
4. [Storage Engine Optimization](#storage-engine-optimization)
5. [Network Optimization](#network-optimization)
6. [Read Optimization](#read-optimization)
7. [Write Optimization](#write-optimization)
8. [Multi-Raft Tuning](#multi-raft-tuning)
9. [MVCC Configuration](#mvcc-configuration)
10. [Monitoring Performance](#monitoring-performance)
11. [Benchmarking](#benchmarking)
12. [Performance Troubleshooting](#performance-troubleshooting)

## Performance Overview

DistributedKV is designed to deliver high performance with linearizable consistency:

- **Throughput**: Up to 50K QPS
- **Latency**:
  - Read: 1-5ms (average)
  - Write: 5-10ms (average)
- **Availability**: 99.9% (3 Nines)

Performance characteristics can vary based on:
- Workload (read/write ratio)
- Key and value sizes
- Hardware configuration
- Network conditions
- Cluster size and topology
- Configuration settings

## Hardware Considerations

### CPU

- **Recommendation**: Modern CPUs with high single-thread performance
- **Core Count**: 8+ cores per node for production
- **Impact**:
  - More cores allow more parallel operations
  - Higher clock speeds reduce operation latency

Optimization tips:
- Ensure CPU frequency scaling is set to "performance" mode
- Disable CPU power-saving features for production servers
- Use CPU affinity to bind processes to specific cores
- Monitor CPU usage and ensure it stays below 70% under normal load

### Memory

- **Recommendation**: 16-64 GB RAM per node
- **Impact**:
  - More memory allows larger caches
  - Higher memory bandwidth improves throughput

Memory allocation guidelines:
- JVM heap: 50-70% of system memory
- OS page cache: 20-30% of system memory
- System overhead: 10-20% of system memory

### Storage

- **Recommendation**: NVMe SSDs for production
- **Impact**:
  - IOPS capability directly affects throughput
  - Lower latency drives reduce operation latency

Storage considerations:
- Random IO performance is more important than sequential
- Use RAID for additional redundancy if needed (RAID 10 recommended)
- Separate logs and data on different physical devices
- Consider using multiple storage devices for different Raft groups

### Network

- **Recommendation**: 10 Gbps+ for production
- **Impact**:
  - Network bandwidth can become a bottleneck in large clusters
  - Network latency affects operation latency

Network optimization:
- Use jumbo frames where possible (MTU 9000)
- Ensure low-latency connections between nodes
- Consider network topology when deploying (prefer same rack)
- Use dedicated network interfaces for client and inter-node traffic

## JVM Tuning

### Memory Settings

```bash
# For a 16GB system
JAVA_OPTS="-Xms10G -Xmx10G"

# For a 32GB system
JAVA_OPTS="-Xms20G -Xmx20G"

# For a 64GB system
JAVA_OPTS="-Xms40G -Xmx40G"
```

### Garbage Collection

G1GC (Garbage First) is recommended for most deployments:

```bash
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled"
```

For larger heaps and lower latency requirements:

```bash
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC -XX:MaxGCPauseMillis=50 -XX:G1HeapRegionSize=32m -XX:InitiatingHeapOccupancyPercent=45"
```

### Memory Allocation

```bash
# Direct memory settings for off-heap storage
JAVA_OPTS="$JAVA_OPTS -XX:MaxDirectMemorySize=8G"

# Stack size configuration
JAVA_OPTS="$JAVA_OPTS -Xss512k"
```

### GC Logging

```bash
JAVA_OPTS="$JAVA_OPTS -Xlog:gc*=info:file=logs/gc.log:time,uptime,level,tags:filecount=5,filesize=100m"
```

## Storage Engine Optimization

### RocksDB Configuration

RocksDB is the default and recommended storage engine for most workloads:

```properties
# Cache and memory settings
storage.rocksdb.block.cache.size.mb=4096
storage.rocksdb.write.buffer.size.mb=128
storage.rocksdb.max.write.buffer.number=4

# Compaction settings
storage.rocksdb.level0.file.num.compaction.trigger=4
storage.rocksdb.level0.slowdown.writes.trigger=20
storage.rocksdb.level0.stop.writes.trigger=36
storage.rocksdb.target.file.size.base.mb=64
storage.rocksdb.max.background.jobs=4

# Compression
storage.rocksdb.compression=SNAPPY
storage.rocksdb.bottommost.compression=ZSTD

# Performance tuning
storage.rocksdb.use.direct.io=true
storage.rocksdb.allow.concurrent.memtable.write=true
storage.rocksdb.enable.write.thread.adaptivity=true
```

### B+ Tree Configuration

For workloads with frequent range scans:

```properties
storage.btree.order=128
storage.btree.node.size=8192
storage.btree.cache.size.mb=1024
storage.btree.max.height=5
```

### Hash Table Configuration

For simple key-value lookups with fixed-size keys:

```properties
storage.hashtable.buckets=1000000
storage.hashtable.initial.capacity=1000000
storage.hashtable.load.factor=0.75
```

### Hybrid Storage Configuration

Configure different storage engines for different types of data:

```properties
# Use hash table for small, frequently accessed keys
storage.mapping.user_session=HASHTABLE

# Use B+ Tree for ordered data
storage.mapping.time_series=BTREE

# Use RocksDB for most data
storage.mapping.default=ROCKSDB
```

## Network Optimization

### Socket Configuration

```properties
# TCP settings
network.tcp.no.delay=true
network.reuse.address=true
network.keep.alive=true
network.send.buffer.size=262144
network.receive.buffer.size=262144

# Connection pooling
network.max.connections=1000
network.backlog=128
```

### RPC Tuning

```properties
# RPC framework settings
rpc.timeout.ms=5000
rpc.max.inbound.message.size=10485760
rpc.max.outbound.message.size=10485760
rpc.compression.enabled=true
```

### Thread Pools

```properties
# Network thread pools
performance.network.workers=8
performance.io.workers=16
performance.apply.workers=8
```

## Read Optimization

### ReadIndex Optimization

ReadIndex provides linearizable reads without log replication:

```properties
# Enable ReadIndex
raft.enable.read.index=true

# ReadIndex timeout
raft.read.index.timeout.ms=1000

# Batching for ReadIndex requests
raft.read.index.batch.enabled=true
```

### FollowerRead Configuration

FollowerRead allows reads from follower nodes, reducing latency by up to 42%:

```properties
# Enable FollowerRead
raft.enable.follower.read=true

# Maximum clock drift allowed
raft.follower.read.max.clock.drift.ms=500

# Lease-based optimization
raft.follower.read.lease.enabled=true
raft.follower.read.lease.duration.ms=9000
```

### Read Caching

```properties
# Client-side caching
client.cache.enabled=true
client.cache.size.mb=256
client.cache.expiry.seconds=60

# Server-side caching
server.read.cache.enabled=true
server.read.cache.size.mb=1024
server.bloom.filter.enabled=true
```

## Write Optimization

### Async Apply

Async Apply processes log entries without blocking, improving write throughput:

```properties
# Enable async apply
raft.enable.async.apply=true

# Batch size for applying entries
raft.apply.batch.size=100

# Maximum pending applies
raft.max.pending.applies=10000
```

### Write Batching

```properties
# Client-side batching
client.batch.enabled=true
client.batch.size=100
client.batch.timeout.ms=10

# Server-side batching
raft.log.append.batch.size=32
raft.propose.batch.enabled=true
```

### Write Ahead Log

```properties
# WAL settings for RocksDB
storage.rocksdb.wal.dir=/mnt/fast_disk/wal
storage.rocksdb.wal.ttl.seconds=0
storage.rocksdb.wal.size.limit.mb=0
storage.rocksdb.wal.bytes.per.sync=65536
```

## Multi-Raft Tuning

### Shard Configuration

```properties
# Number of shards (Raft groups)
cluster.shard.count=16

# Replication factor
cluster.replication.factor=3

# Virtual nodes per physical node
cluster.virtual.nodes.per.node=100
```

Guidelines for determining shard count:
- 2-4 shards per physical core
- Shard count should be a multiple of node count
- More shards allow better parallelism
- Too many shards increase overhead

### Consistent Hashing

```properties
# Consistent hashing configuration
cluster.consistent.hash.implementation=JUMP_HASH
cluster.consistent.hash.virtual.nodes=200
```

### Raft Group Balancing

```properties
# Load balancing parameters
cluster.leader.balance.enabled=true
cluster.max.leaders.per.node=5
cluster.auto.rebalance=true
cluster.rebalance.threshold=0.2
```

## MVCC Configuration

Multi-Version Concurrency Control provides snapshot isolation for concurrent operations:

```properties
# Enable MVCC
storage.enable.mvcc=true

# MVCC settings
storage.mvcc.max.versions=5
storage.mvcc.gc.interval.seconds=300
storage.mvcc.min.snapshot.retention.seconds=3600
```

MVCC optimization tips:
- Adjust max versions based on your workload and concurrency
- Set GC interval based on transaction volume
- Configure snapshot retention based on long-running transaction needs
- Monitor version chain length to detect potential issues

## Monitoring Performance

Key metrics to monitor for performance analysis:

### Operation Metrics
- Operations per second (reads/writes)
- Operation latency (min, avg, p95, p99, max)
- Error rate and error types
- Cache hit rate

### Raft Metrics
- Leader election frequency
- Log append latency
- Commit latency
- Apply latency
- Log compression ratio

### Storage Metrics
- Disk usage
- Disk IO operations
- Read/write amplification
- Compaction statistics

### Resource Metrics
- CPU usage
- Memory usage
- Network throughput
- GC frequency and duration

Configuration for metrics collection:

```properties
# Enable metrics collection
metrics.enabled=true

# Metrics sampling rate
metrics.sample.interval.ms=1000

# Output options
metrics.prometheus.enabled=true
metrics.prometheus.port=9090
metrics.jmx.enabled=true
metrics.log.enabled=true
metrics.log.interval.seconds=60
```

## Benchmarking

DistributedKV includes a benchmarking tool for performance testing:

```bash
./bin/benchmark.sh --threads 8 --operations 1000000 --value-size 1024 --distribution uniform --ratio 80:20
```

Key benchmarking parameters:
- `--threads`: Number of concurrent clients
- `--operations`: Total number of operations
- `--value-size`: Size of values in bytes
- `--distribution`: Key distribution pattern (uniform, zipfian, latest)
- `--ratio`: Read/write ratio (e.g., 80:20 means 80% reads, 20% writes)

Recommended benchmarking process:
1. Start with a small benchmark to validate setup
2. Run single-node benchmarks to establish baseline
3. Test with increasing cluster sizes
4. Test with different workloads (read-heavy, write-heavy, mixed)
5. Test with different key/value sizes
6. Test with different configuration parameters
7. Analyze results and optimize

## Performance Troubleshooting

### Common Performance Issues

1. **High Latency**
   - Check for GC pauses
   - Verify network latency between nodes
   - Check for disk IO bottlenecks
   - Ensure leader distribution is balanced

2. **Low Throughput**
   - Check for resource bottlenecks (CPU, memory, disk, network)
   - Verify thread pool sizing
   - Check for lock contention
   - Ensure shard count is appropriate

3. **Hotspots**
   - Review key distribution
   - Check consistent hashing configuration
   - Consider custom sharding for hotkeys
   - Monitor leader distribution

### Performance Analysis Commands

```bash
# Get system metrics
./bin/cluster-admin.sh metrics

# Profile specific operations
./bin/client-cli.sh --profile get user:1000

# Get hotspot analysis
./bin/cluster-admin.sh analyze-hotspots

# Generate performance report
./bin/cluster-admin.sh performance-report --duration 3600
```

### Performance Tuning Process

1. Establish performance baseline
2. Identify bottlenecks through monitoring
3. Make one configuration change at a time
4. Test the impact of each change
5. Document improvements and regressions
6. Repeat until performance goals are met

Recommended methodology:
- Use a production-like dataset for testing
- Test with realistic workloads
- Ensure tests run long enough to capture steady-state performance
- Consider daily patterns when evaluating performance
- Test both peak throughput and steady-state performance

---

By following these performance tuning guidelines, you can optimize your DistributedKV deployment to achieve maximum throughput and minimum latency while maintaining high availability and linearizable consistency.
