# DistributedKV API Reference

This document provides a comprehensive reference for the DistributedKV API, including client interfaces, server APIs, and administrative operations.

## Table of Contents

1. [Java Client API](#java-client-api)
2. [REST API](#rest-api)
3. [Command Line Interface](#command-line-interface)
4. [Administrative API](#administrative-api)
5. [Monitoring API](#monitoring-api)
6. [Error Handling](#error-handling)

## Java Client API

The Java client provides a high-level API for interacting with the DistributedKV system.

### Client Initialization

```java
// Create a client with default configuration
KVClient client = new KVClientImpl();

// Create a client with a specific configuration file
KVClient client = new KVClientImpl("config/client.conf");

// Create a client with specific server addresses
KVClient client = new KVClientImpl(Arrays.asList("localhost:10000", "localhost:10001"));

// Create a client with a custom configuration
KVClientConfig config = new KVClientConfig()
    .setServers(Arrays.asList("localhost:10000", "localhost:10001"))
    .setConnectionTimeout(5000)
    .setRequestTimeout(10000)
    .setRetryCount(3);
KVClient client = new KVClientImpl(config);
```

### Basic Operations

```java
// Get a value
CompletableFuture<Result<byte[]>> future = client.get(key);
Result<byte[]> result = future.get();
if (result.isSuccess()) {
    byte[] value = result.getData();
    // Process value
} else {
    // Handle error
    Status status = result.getStatus();
    String message = result.getMessage();
}

// Put a key-value pair
CompletableFuture<Result<Void>> future = client.put(key, value);
Result<Void> result = future.get();
if (result.isSuccess()) {
    // Success
} else {
    // Handle error
}

// Delete a key
CompletableFuture<Result<Void>> future = client.delete(key);
Result<Void> result = future.get();
if (result.isSuccess()) {
    // Success
} else {
    // Handle error
}
```

### Advanced Operations

```java
// Batch operations
List<KVOperation> operations = new ArrayList<>();
operations.add(KVOperation.put(key1, value1));
operations.add(KVOperation.put(key2, value2));
operations.add(KVOperation.delete(key3));

CompletableFuture<Result<Void>> future = client.batch(operations);
Result<Void> result = future.get();

// Scan a range of keys
byte[] startKey = "user:1000".getBytes();
byte[] endKey = "user:2000".getBytes();
int limit = 100;

CompletableFuture<Result<List<KVPair>>> future = client.scan(startKey, endKey, limit);
Result<List<KVPair>> result = future.get();
if (result.isSuccess()) {
    List<KVPair> pairs = result.getData();
    for (KVPair pair : pairs) {
        // Process key-value pair
    }
}

// Conditional put (only if key doesn't exist)
CompletableFuture<Result<Boolean>> future = client.putIfAbsent(key, value);

// Compare and swap
byte[] expectedValue = ...;
byte[] newValue = ...;
CompletableFuture<Result<Boolean>> future = client.compareAndSwap(key, expectedValue, newValue);

// Transactions
Transaction txn = client.beginTransaction();
try {
    txn.put(key1, value1);
    byte[] value = txn.get(key2).get();
    txn.delete(key3);
    txn.commit().get();
} catch (Exception e) {
    txn.rollback().get();
    // Handle error
}
```

### Read Options

```java
// Read with linearizable consistency (default)
ReadOptions readOptions = ReadOptions.newBuilder()
    .setConsistency(Consistency.LINEARIZABLE)
    .build();
CompletableFuture<Result<byte[]>> future = client.get(key, readOptions);

// Read from followers (may reduce latency)
ReadOptions readOptions = ReadOptions.newBuilder()
    .setConsistency(Consistency.LINEARIZABLE)
    .setAllowFollowerRead(true)
    .build();
CompletableFuture<Result<byte[]>> future = client.get(key, readOptions);

// Read with bounded staleness
ReadOptions readOptions = ReadOptions.newBuilder()
    .setConsistency(Consistency.BOUNDED_STALENESS)
    .setMaxStalenessMillis(1000)
    .build();
CompletableFuture<Result<byte[]>> future = client.get(key, readOptions);
```

### Write Options

```java
// Synchronous write (default)
WriteOptions writeOptions = WriteOptions.newBuilder()
    .setSync(true)
    .build();
CompletableFuture<Result<Void>> future = client.put(key, value, writeOptions);

// Asynchronous write (faster but may lose data on crash)
WriteOptions writeOptions = WriteOptions.newBuilder()
    .setSync(false)
    .build();
CompletableFuture<Result<Void>> future = client.put(key, value, writeOptions);

// Write with a timeout
WriteOptions writeOptions = WriteOptions.newBuilder()
    .setTimeout(5000)
    .build();
CompletableFuture<Result<Void>> future = client.put(key, value, writeOptions);
```

### Client Lifecycle

```java
// Close the client when done
client.close();

// Or use try-with-resources
try (KVClient client = new KVClientImpl()) {
    // Use client
}
```

## REST API

DistributedKV also provides a RESTful API for language-agnostic access.

### Base URL

```
http://<server-address>:<http-port>/api/v1
```

### Authentication

```
Authorization: Bearer <token>
```

### Key-Value Operations

#### Get a value

```
GET /kv/{key}
```

Response:
```json
{
  "status": "OK",
  "data": "base64-encoded-value"
}
```

#### Put a key-value pair

```
PUT /kv/{key}
Content-Type: application/octet-stream

[binary-data]
```

Response:
```json
{
  "status": "OK"
}
```

#### Delete a key

```
DELETE /kv/{key}
```

Response:
```json
{
  "status": "OK"
}
```

#### Scan a range of keys

```
GET /kv?startKey={startKey}&endKey={endKey}&limit={limit}
```

Response:
```json
{
  "status": "OK",
  "data": [
    {
      "key": "base64-encoded-key-1",
      "value": "base64-encoded-value-1"
    },
    {
      "key": "base64-encoded-key-2",
      "value": "base64-encoded-value-2"
    }
  ]
}
```

### Batch Operations

```
POST /batch
Content-Type: application/json

{
  "operations": [
    {
      "type": "PUT",
      "key": "base64-encoded-key-1",
      "value": "base64-encoded-value-1"
    },
    {
      "type": "DELETE",
      "key": "base64-encoded-key-2"
    },
    {
      "type": "PUT",
      "key": "base64-encoded-key-3",
      "value": "base64-encoded-value-3"
    }
  ]
}
```

Response:
```json
{
  "status": "OK"
}
```

### Transaction Operations

```
POST /transaction
Content-Type: application/json

{
  "operations": [
    {
      "type": "PUT",
      "key": "base64-encoded-key-1",
      "value": "base64-encoded-value-1"
    },
    {
      "type": "GET",
      "key": "base64-encoded-key-2"
    },
    {
      "type": "DELETE",
      "key": "base64-encoded-key-3"
    }
  ]
}
```

Response:
```json
{
  "status": "OK",
  "results": [
    {
      "status": "OK"
    },
    {
      "status": "OK",
      "data": "base64-encoded-value-2"
    },
    {
      "status": "OK"
    }
  ]
}
```

## Command Line Interface

DistributedKV provides a command-line interface for interacting with the system.

### Basic Usage

```
./bin/client-cli.sh [options] [command]
```

### Options

```
-c, --config FILE    Path to configuration file (default: config/client.conf)
-s, --server HOST:PORT  Server address (default: localhost:10000)
-h, --help           Show help message
```

### Commands

```
get KEY              Get value for key
put KEY VALUE        Put key-value pair
delete KEY           Delete key
scan START END       Scan keys in range
stats                Get server statistics
interactive          Start interactive mode
```

### Examples

```
# Get a value
./bin/client-cli.sh get my-key

# Put a value
./bin/client-cli.sh put my-key my-value

# Delete a key
./bin/client-cli.sh delete my-key

# Scan a range of keys
./bin/client-cli.sh scan user:1000 user:2000

# Start interactive mode
./bin/client-cli.sh interactive
```

## Administrative API

The administrative API provides operations for managing the DistributedKV cluster.

### Java Admin API

```java
// Create admin client
ClusterAdmin admin = new ClusterAdmin("config/cluster.conf");

// Get cluster status
ClusterStatus status = admin.getClusterStatus();

// Add a node to the cluster
admin.addNode("node4", "localhost:10003");

// Remove a node from the cluster
admin.removeNode("node3");

// Rebalance the cluster
admin.rebalance();

// Move a shard to another node
admin.moveShard("shard5", "node2");

// Backup the cluster
admin.backup("/path/to/backup");

// Restore the cluster
admin.restore("/path/to/backup");

// Get cluster metrics
ClusterMetrics metrics = admin.getMetrics();
```

### Admin CLI

```
./bin/cluster-admin.sh [options] [command]
```

Options:
```
-c, --config FILE    Path to configuration file (default: config/cluster.conf)
-h, --help           Show help message
```

Commands:
```
status               Show cluster status
add-node HOST:PORT   Add node to cluster
remove-node ID       Remove node from cluster
rebalance            Rebalance data across the cluster
move-shard ID NODE   Move shard to another node
backup PATH          Backup the cluster data
restore PATH         Restore the cluster from backup
metrics              Show cluster metrics
reset-node ID        Reset a node (dangerous)
```

## Monitoring API

DistributedKV provides metrics and monitoring endpoints for health checking and performance monitoring.

### Metrics Endpoint

```
GET /metrics
```

Response:
```json
{
  "system": {
    "cpu_usage": 0.25,
    "memory_usage": 0.4,
    "disk_usage": 0.3,
    "uptime_seconds": 86400
  },
  "operations": {
    "reads_per_second": 8500,
    "writes_per_second": 1500,
    "average_read_latency_ms": 2.5,
    "average_write_latency_ms": 5.2,
    "p99_read_latency_ms": 10.5,
    "p99_write_latency_ms": 15.8
  },
  "storage": {
    "total_keys": 10000000,
    "total_size_bytes": 5368709120,
    "compaction_pending": false
  },
  "raft": {
    "leader_changes": 2,
    "committed_log_index": 15000000,
    "applied_log_index": 14999500,
    "pending_proposals": 5
  }
}
```

### Health Check Endpoint

```
GET /health
```

Response:
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "node_id": "node1",
  "uptime_seconds": 86400,
  "raft_status": "leader",
  "errors": []
}
```

## Error Handling

DistributedKV uses a consistent error handling model across all APIs.

### Status Codes

- `OK` (0): Operation succeeded
- `UNKNOWN_ERROR` (1): Unknown error occurred
- `TIMEOUT` (2): Operation timed out
- `NOT_LEADER` (3): Node is not the leader
- `NOT_FOUND` (4): Key not found
- `IO_ERROR` (5): I/O error
- `NETWORK_ERROR` (6): Network error
- `INVALID_ARGUMENT` (7): Invalid argument
- `NOT_INITIALIZED` (8): System not initialized
- `ALREADY_EXISTS` (9): Key already exists
- `NOT_SUPPORTED` (10): Operation not supported
- `SERIALIZATION_ERROR` (11): Serialization error
- `BUSY` (12): System is busy
- `CANCELLED` (13): Operation cancelled

### Java Error Handling

```java
try {
    Result<byte[]> result = client.get(key).get();
    if (result.isSuccess()) {
        // Process value
    } else {
        Status status = result.getStatus();
        String message = result.getMessage();

        switch (status) {
            case NOT_FOUND:
                // Handle not found
                break;
            case TIMEOUT:
                // Handle timeout
                break;
            case NETWORK_ERROR:
                // Handle network error
                break;
            default:
                // Handle other errors
                break;
        }
    }
} catch (Exception e) {
    // Handle exception
}
```

### REST API Error Response

```json
{
  "status": "ERROR",
  "code": "NOT_FOUND",
  "message": "Key not found: my-key"
}
```

### Command Line Error Output

```
Error: Key not found: my-key
Status code: NOT_FOUND (4)
```
