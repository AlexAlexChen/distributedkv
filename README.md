# DistributedKV

A high-performance distributed Key-Value storage system with Multi-Raft architecture supporting 50K QPS and 3 Nines of availability.

## Features

- **Multi-Raft Architecture**: Linearizable key-value operations with consensus across distributed nodes
- **Consistent Hashing**: Even data distribution across shards with dynamic load balancing
- **High Performance Reads**:
  - **AsyncApply**: Process log entries without blocking
  - **ReadIndex**: Linearizable reads without leader election
  - **FollowerRead**: Allow followers to serve read requests (42% latency reduction)
- **Flexible Storage**: Multiple storage engines (RocksDB, B+ trees, Hash tables)
- **MVCC**: Multi-Version Concurrency Control for high concurrency

## Getting Started

### Prerequisites

- Java 11 or higher
- Gradle 7.0 or higher

### Building the Project

```bash
./gradlew build
```

### Running the Server

```bash
./bin/start-server.sh
```

### Using the Client

```bash
./bin/client-cli.sh
```

## Project Structure

The project is organized into several key components:

- **raft**: Implementation of the Multi-Raft consensus algorithm
- **storage**: Storage engine interfaces and implementations
- **network**: Network layer for RPC communication
- **common**: Common utilities including consistent hashing
- **server**: Server implementation
- **client**: Client implementation

## Configuration

Configuration files are located in the `config` directory:

- `server.conf`: Server configuration
- `client.conf`: Client configuration
- `cluster.conf`: Cluster configuration

## Performance

- Throughput: 50K QPS
- Availability: 99.9% (3 Nines)
- Read Latency: Reduced by 42% with FollowerRead

## Documentation

Additional documentation can be found in the `docs` directory:

- [Architecture Overview](docs/architecture.md)
- [API Documentation](docs/api.md)
- [Deployment Guide](docs/deployment.md)
- [Performance Tuning](docs/performance.md)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
