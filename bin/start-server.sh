#!/bin/bash

# Script to start the DistributedKV server
# Usage: ./start-server.sh [options]

# Default values
CONFIG_FILE="config/server.conf"
NODE_ID=""
PORT=""
JAVA_OPTS="-Xmx2G -Xms1G -XX:+UseG1GC"
LOG_DIR="logs"
MAIN_CLASS="com.distributedkv.Main"
JAR_FILE="lib/distributedkv-1.0-SNAPSHOT-all.jar"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    -n|--node-id)
      NODE_ID="$2"
      shift 2
      ;;
    -p|--port)
      PORT="$2"
      shift 2
      ;;
    -m|--memory)
      JAVA_OPTS="-Xmx${2}G -Xms${2}G -XX:+UseG1GC"
      shift 2
      ;;
    -d|--debug)
      JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  -c, --config FILE    Path to configuration file (default: config/server.conf)"
      echo "  -n, --node-id ID     Node ID"
      echo "  -p, --port PORT      Server port"
      echo "  -m, --memory SIZE    Java heap size in GB (default: 2)"
      echo "  -d, --debug          Enable remote debugging on port 5005"
      echo "  -h, --help           Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use -h or --help to see available options"
      exit 1
      ;;
  esac
done

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Create the command
CMD="java $JAVA_OPTS -jar $JAR_FILE"

# Add options
CMD="$CMD -c $CONFIG_FILE"
if [ -n "$NODE_ID" ]; then
  CMD="$CMD -n $NODE_ID"
fi
if [ -n "$PORT" ]; then
  CMD="$CMD -p $PORT"
fi

# Get timestamp for log file name
TIMESTAMP=$(date "+%Y%m%d-%H%M%S")
LOG_FILE="$LOG_DIR/server-$TIMESTAMP.log"

echo "Starting DistributedKV server..."
echo "Command: $CMD"
echo "Logs: $LOG_FILE"

# Start the server
nohup $CMD > "$LOG_FILE" 2>&1 &

# Save PID to file
PID=$!
echo $PID > server.pid
echo "Server started with PID: $PID"
