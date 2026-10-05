# DistributedKV Deployment Guide

This document provides comprehensive instructions for deploying and operating the DistributedKV storage system in production environments.

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Cluster Setup](#cluster-setup)
5. [Production Deployment](#production-deployment)
6. [Security Considerations](#security-considerations)
7. [Backup and Recovery](#backup-and-recovery)
8. [Scaling](#scaling)
9. [Monitoring and Alerting](#monitoring-and-alerting)
10. [Upgrading](#upgrading)
11. [Troubleshooting](#troubleshooting)

## System Requirements

### Hardware Recommendations

| Component | Minimum | Recommended | High Performance |
|-----------|---------|-------------|------------------|
| CPU | 4 cores | 8+ cores | 16+ cores |
| Memory | 8 GB | 16-32 GB | 64+ GB |
| Disk | 100 GB SSD | 500 GB SSD | 1+ TB NVMe SSD |
| Network | 1 Gbps | 10 Gbps | 25+ Gbps |

### Software Requirements

- Java 11 or higher
- Linux (recommended: Ubuntu 20.04 LTS or higher, CentOS 8+)
- Open file limit: at least 65536
- Disable swapping or set swappiness to a low value

## Installation

### Binary Installation

1. Download the latest release package:
   ```bash
   wget https://example.com/distributedkv-1.0.0.tar.gz
   ```

2. Extract the package:
   ```bash
   tar -xzf distributedkv-1.0.0.tar.gz
   cd distributedkv-1.0.0
   ```

3. Verify the installation:
   ```bash
   ./bin/client-cli.sh --version
   ```

### Building from Source

1. Clone the repository:
   ```bash
   git clone https://github.com/example/distributedkv.git
   cd distributedkv
   ```

2. Build the project:
   ```bash
   ./gradlew packageDistribution
   ```

3. The distribution package will be available in `build/distributions/`.

### Directory Structure

```
distributedkv/
├── bin/                     # Executable scripts
├── lib/                     # JAR files
├── config/                  # Configuration files
├── data/                    # Data directory
├── logs/                    # Log files
└── docs/                    # Documentation
```

## Configuration

DistributedKV uses configuration files in a simple key-value format. The main configuration files are:

- `server.conf`: Server-side configuration
- `client.conf`: Client-side configuration
- `cluster.conf`: Cluster configuration

### Essential Server Configuration Parameters

```properties
# Node identification
node.id=node1
node.host=192.168.1.101
node.port=10000
node.data.dir=/var/lib/distributedkv/data

# Cluster configuration
cluster.seed.nodes=192.168.1.101:10000,192.168.1.102:10000,192.168.1.103:10000
cluster.replication.factor=3

# Storage configuration
storage.type=ROCKSDB
storage.rocksdb.max.open.files=1000
storage.rocksdb.block.cache.size.mb=512
storage.enable.mvcc=true

# Performance tuning
raft.enable.follower.read=true
raft.enable.async.apply=true
performance.io.workers=8
```

### JVM Configuration

Recommended JVM options for production:

```bash
JAVA_OPTS="-Xms4G -Xmx4G -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled"
```

## Cluster Setup

### Initial Cluster Deployment

1. Prepare server nodes (minimum 3 nodes for high availability)

2. Configure each node with unique node ID and appropriate network settings:
   ```properties
   # Node 1
   node.id=node1
   node.host=192.168.1.101
   node.port=10000
   ```

3. Configure the same seed nodes for all nodes in the cluster:
   ```properties
   cluster.seed.nodes=192.168.1.101:10000,192.168.1.102:10000,192.168.1.103:10000
   ```

4. Start the first node:
   ```bash
   ./bin/start-server.sh -c config/server-node1.conf
   ```

5. Start the remaining nodes:
   ```bash
   # On node 2
   ./bin/start-server.sh -c config/server-node2.conf

   # On node 3
   ./bin/start-server.sh -c config/server-node3.conf
   ```

6. Verify the cluster status:
   ```bash
   ./bin/cluster-admin.sh status
   ```

### Adding Nodes

1. Prepare the new node with appropriate configuration:
   ```properties
   node.id=node4
   node.host=192.168.1.104
   node.port=10000
   cluster.seed.nodes=192.168.1.101:10000,192.168.1.102:10000,192.168.1.103:10000
   ```

2. Start the new node:
   ```bash
   ./bin/start-server.sh -c config/server-node4.conf
   ```

3. Add the node to the cluster:
   ```bash
   ./bin/cluster-admin.sh add-node 192.168.1.104:10000
   ```

4. Rebalance the cluster (optional):
   ```bash
   ./bin/cluster-admin.sh rebalance
   ```

### Removing Nodes

1. Identify the node to remove:
   ```bash
   ./bin/cluster-admin.sh status
   ```

2. Remove the node from the cluster:
   ```bash
   ./bin/cluster-admin.sh remove-node node3
   ```

3. Rebalance the cluster:
   ```bash
   ./bin/cluster-admin.sh rebalance
   ```

4. Stop the removed node:
   ```bash
   # On node3
   ./bin/stop-server.sh
   ```

## Production Deployment

### Systemd Service

Create a systemd service file for automatic startup and management:

```
# /etc/systemd/system/distributedkv.service
[Unit]
Description=DistributedKV Service
After=network.target

[Service]
User=distributedkv
Group=distributedkv
Type=forking
ExecStart=/opt/distributedkv/bin/start-server.sh -c /opt/distributedkv/config/server.conf
ExecStop=/opt/distributedkv/bin/stop-server.sh
WorkingDirectory=/opt/distributedkv
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
systemctl daemon-reload
systemctl enable distributedkv
systemctl start distributedkv
```

### Docker Deployment

Dockerfile:

```dockerfile
FROM openjdk:11-jre-slim

WORKDIR /app

COPY distributedkv-1.0.0/ /app/

RUN chmod +x /app/bin/*

EXPOSE 10000 10001 9090

ENTRYPOINT ["/app/bin/start-server.sh", "-c", "/app/config/server.conf"]
```

Docker Compose for a 3-node cluster:

```yaml
version: '3'
services:
  node1:
    build: .
    volumes:
      - ./config/server-node1.conf:/app/config/server.conf
      - node1-data:/app/data
    ports:
      - "10000:10000"
      - "9090:9090"
    environment:
      - JAVA_OPTS=-Xms2G -Xmx2G

  node2:
    build: .
    volumes:
      - ./config/server-node2.conf:/app/config/server.conf
      - node2-data:/app/data
    ports:
      - "10001:10000"
      - "9091:9090"
    environment:
      - JAVA_OPTS=-Xms2G -Xmx2G

  node3:
    build: .
    volumes:
      - ./config/server-node3.conf:/app/config/server.conf
      - node3-data:/app/data
    ports:
      - "10002:10000"
      - "9092:9090"
    environment:
      - JAVA_OPTS=-Xms2G -Xmx2G

volumes:
  node1-data:
  node2-data:
  node3-data:
```

### Kubernetes Deployment

StatefulSet example for Kubernetes:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: distributedkv
spec:
  serviceName: "distributedkv"
  replicas: 3
  selector:
    matchLabels:
      app: distributedkv
  template:
    metadata:
      labels:
        app: distributedkv
    spec:
      containers:
      - name: distributedkv
        image: distributedkv:1.0.0
        ports:
        - containerPort: 10000
          name: rpc
        - containerPort: 9090
          name: metrics
        env:
        - name: JAVA_OPTS
          value: "-Xms2G -Xmx2G -XX:+UseG1GC"
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        volumeMounts:
        - name: data
          mountPath: /app/data
        - name: config
          mountPath: /app/config/server.conf
          subPath: server.conf
        readinessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 30
          periodSeconds: 10
        livenessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 60
          periodSeconds: 20
      volumes:
      - name: config
        configMap:
          name: distributedkv-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 100Gi
```

Service configuration:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: distributedkv
  labels:
    app: distributedkv
spec:
  ports:
  - port: 10000
    name: rpc
  - port: 9090
    name: metrics
  clusterIP: None
  selector:
    app: distributedkv
```

ConfigMap for configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: distributedkv-config
data:
  server.conf: |
    # Node configuration
    node.port=10000
    node.data.dir=/app/data

    # Cluster configuration
    cluster.seed.nodes=distributedkv-0.distributedkv:10000,distributedkv-1.distributedkv:10000,distributedkv-2.distributedkv:10000
    cluster.replication.factor=3

    # Storage configuration
    storage.type=ROCKSDB
    storage.enable.mvcc=true

    # Performance tuning
    raft.enable.follower.read=true
    raft.enable.async.apply=true
```

## Security Considerations

### Network Security

1. **Firewall Configuration**

   Configure firewalls to restrict access to only necessary ports:

   - TCP 10000: RPC port (internal cluster communication)
   - TCP 10001: HTTP API port (if enabled)
   - TCP 9090: Metrics port (internal monitoring)

   Example iptables rules:
   ```bash
   # Allow internal cluster communication
   iptables -A INPUT -p tcp -s 192.168.1.0/24 --dport 10000 -j ACCEPT

   # Allow client connections from trusted networks
   iptables -A INPUT -p tcp -s 10.0.0.0/8 --dport 10001 -j ACCEPT

   # Allow monitoring access from admin network
   iptables -A INPUT -p tcp -s 192.168.2.0/24 --dport 9090 -j ACCEPT

   # Deny all other connections
   iptables -A INPUT -p tcp --dport 10000 -j DROP
   iptables -A INPUT -p tcp --dport 10001 -j DROP
   iptables -A INPUT -p tcp --dport 9090 -j DROP
   ```

2. **TLS Configuration**

   Enable TLS for secure communication:

   ```properties
   # In server.conf
   security.ssl.enabled=true
   security.ssl.keystore=/path/to/keystore.jks
   security.ssl.keystore.password=password
   security.ssl.truststore=/path/to/truststore.jks
   security.ssl.truststore.password=password
   ```

### Authentication and Authorization

1. **Enable Authentication**

   ```properties
   # In server.conf
   security.authentication.enabled=true
   security.authentication.type=SIMPLE
   ```

2. **Configure Users**

   Create a users.conf file:

   ```
   # username:password:roles
   admin:password123:admin
   reader:reader456:reader
   writer:writer789:reader,writer
   ```

3. **Enable Authorization**

   ```properties
   # In server.conf
   security.authorization.enabled=true
   security.acl.file=/path/to/acl.conf
   ```

4. **Configure ACLs**

   Create an acl.conf file:

   ```
   # Format: resource:operation:role
   *:read:reader
   *:write:writer
   *:admin:admin
   user/*:read,write:user_admin
   ```

### Data Security

1. **Encryption at Rest**

   Enable encryption for RocksDB:

   ```properties
   storage.rocksdb.encryption.enabled=true
   storage.rocksdb.encryption.key.file=/path/to/encryption.key
   ```

2. **Secure Configuration**

   - Restrict access to configuration files:
     ```bash
     chmod 600 config/*.conf
     ```

   - Use environment variables for sensitive values:
     ```bash
     export KV_SSL_KEYSTORE_PASSWORD=password123
     ```

     In configuration:
     ```properties
     security.ssl.keystore.password=${env:KV_SSL_KEYSTORE_PASSWORD}
     ```

## Backup and Recovery

### Backup Strategies

1. **Full Cluster Backup**

   Use the admin tool to create a consistent backup:

   ```bash
   ./bin/cluster-admin.sh backup /path/to/backup
   ```

2. **Scheduled Backups**

   Set up a cron job for regular backups:

   ```bash
   # Backup every day at 2 AM
   0 2 * * * /opt/distributedkv/bin/cluster-admin.sh backup /backups/distributedkv-$(date +\%Y\%m\%d) > /var/log/distributedkv-backup.log 2>&1
   ```

3. **Remote Backup Storage**

   Copy backups to remote storage:

   ```bash
   # After backup completes
   rsync -avz /backups/distributedkv-* backup-server:/archive/distributedkv/
   ```

### Recovery Procedures

1. **Full Cluster Recovery**

   ```bash
   # Stop all nodes
   systemctl stop distributedkv

   # Clear data directories
   rm -rf /var/lib/distributedkv/data/*

   # Restore from backup
   ./bin/cluster-admin.sh restore /path/to/backup

   # Start all nodes
   systemctl start distributedkv
   ```

2. **Single Node Recovery**

   ```bash
   # Stop the node
   systemctl stop distributedkv

   # Clear node data
   rm -rf /var/lib/distributedkv/data/*

   # Start the node (it will automatically recover from other nodes)
   systemctl start distributedkv
   ```

3. **Point-in-Time Recovery**

   If needed, specify a recovery timestamp:

   ```bash
   ./bin/cluster-admin.sh restore /path/to/backup --timestamp "2023-04-15T14:30:00"
   ```

## Scaling

### Horizontal Scaling

1. **Adding Capacity**

   Add new nodes as described in the [Adding Nodes](#adding-nodes) section.

2. **Rebalancing Data**

   After adding nodes, rebalance data:

   ```bash
   ./bin/cluster-admin.sh rebalance
   ```

3. **Monitoring Rebalance Progress**

   ```bash
   ./bin/cluster-admin.sh rebalance-status
   ```

### Vertical Scaling

1. **Increasing Resources**

   - Update memory allocation:
     ```properties
     # In start-server.sh or systemd service
     JAVA_OPTS="-Xms8G -Xmx8G -XX:+UseG1GC"
     ```

   - Update thread pool sizes:
     ```properties
     # In server.conf
     performance.network.workers=8
     performance.io.workers=16
     performance.apply.workers=8
     ```

2. **Storage Configuration**

   Adjust RocksDB for larger systems:

   ```properties
   storage.rocksdb.block.cache.size.mb=4096
   storage.rocksdb.write.buffer.size.mb=128
   storage.rocksdb.max.write.buffer.number=8
   ```

## Monitoring and Alerting

### Metrics Collection

1. **Prometheus Integration**

   Enable Prometheus metrics:

   ```properties
   metrics.prometheus.enabled=true
   metrics.prometheus.port=9090
   ```

2. **JMX Monitoring**

   Enable JMX:

   ```properties
   metrics.jmx.enabled=true
   ```

   JVM options for remote JMX:
   ```
   -Dcom.sun.management.jmxremote
   -Dcom.sun.management.jmxremote.port=9999
   -Dcom.sun.management.jmxremote.authenticate=true
   -Dcom.sun.management.jmxremote.ssl=true
   ```

3. **Log-based Monitoring**

   Configure detailed metrics logging:

   ```properties
   metrics.log.enabled=true
   metrics.log.interval.seconds=60
   ```

### Grafana Dashboard

A sample Grafana dashboard configuration is provided in `config/grafana-dashboard.json`.

Key metrics to monitor:

- Operations per second (reads/writes)
- Read/write latency (avg, p95, p99)
- Error rate
- Leader changes
- Disk usage
- Memory usage
- GC pause times

### Alerting Rules

Example Prometheus alerting rules:

```yaml
groups:
- name: distributedkv
  rules:
  - alert: HighErrorRate
    expr: rate(distributedkv_errors_total[5m]) / rate(distributedkv_operations_total[5m]) > 0.01
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High error rate"
      description: "Error rate is above 1% for 5 minutes"

  - alert: HighLatency
    expr: distributedkv_p99_latency_ms > 100
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High latency"
      description: "P99 latency is above 100ms for 5 minutes"

  - alert: LeaderChanges
    expr: rate(distributedkv_leader_changes_total[5m]) > 0.1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Frequent leader changes"
      description: "Raft groups experiencing frequent leader changes"
```

## Upgrading

### Rolling Upgrade Procedure

1. **Prepare for Upgrade**

   - Ensure all nodes are healthy:
     ```bash
     ./bin/cluster-admin.sh status
     ```

   - Create a backup:
     ```bash
     ./bin/cluster-admin.sh backup /path/to/backup
     ```

2. **Upgrade Process**

   Upgrade one node at a time:

   ```bash
   # Stop the node
   systemctl stop distributedkv

   # Backup configuration
   cp -r /opt/distributedkv/config /opt/distributedkv/config.backup

   # Replace binaries
   rm -rf /opt/distributedkv/lib
   cp -r /path/to/new/version/lib /opt/distributedkv/

   # Update scripts
   cp -r /path/to/new/version/bin /opt/distributedkv/

   # Start the node
   systemctl start distributedkv

   # Verify node health before proceeding to next node
   ./bin/cluster-admin.sh node-status <node-id>
   ```

3. **Rollback Procedure**

   If issues are encountered:

   ```bash
   # Stop the node
   systemctl stop distributedkv

   # Restore old version
   rm -rf /opt/distributedkv/lib /opt/distributedkv/bin
   cp -r /path/to/old/version/lib /opt/distributedkv/
   cp -r /path/to/old/version/bin /opt/distributedkv/

   # Restore configuration if needed
   cp -r /opt/distributedkv/config.backup/* /opt/distributedkv/config/

   # Start the node
   systemctl start distributedkv
   ```

## Troubleshooting

### Common Issues

1. **Node Won't Start**

   Check logs:
   ```bash
   tail -f logs/server.log
   ```

   Common causes:
   - Port conflicts
   - Permission issues
   - Invalid configuration
   - Insufficient memory

2. **Poor Performance**

   Check metrics:
   ```bash
   ./bin/cluster-admin.sh metrics
   ```

   Potential optimizations:
   - Increase memory allocation
   - Adjust RocksDB settings
   - Tune network parameters
   - Add more nodes for better distribution

3. **Data Inconsistency**

   Run consistency check:
   ```bash
   ./bin/cluster-admin.sh consistency-check
   ```

   Recovery options:
   - Force a leader election
   - Restore from backup
   - Reset problematic nodes

### Log Analysis

Key log patterns to look for:

- `ERROR` level messages
- Raft consensus issues
- GC pause times
- Disk I/O errors
- Network timeouts

### Support Resources

- Community Forum: [https://forum.distributedkv.org](https://forum.distributedkv.org)
- Issue Tracker: [https://github.com/example/distributedkv/issues](https://github.com/example/distributedkv/issues)
- Documentation: [https://docs.distributedkv.org](https://docs.distributedkv.org)
