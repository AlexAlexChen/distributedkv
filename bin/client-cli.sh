#!/bin/bash

# Command-line interface for the DistributedKV client
# Usage: ./client-cli.sh [options] [command]

# Default values
CONFIG_FILE="config/client.conf"
SERVER="localhost:10000"
JAVA_OPTS="-Xmx1G"
JAR_FILE="lib/distributedkv-1.0-SNAPSHOT-all.jar"
MAIN_CLASS="com.distributedkv.client.KVClientCLI"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    -s|--server)
      SERVER="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [options] [command]"
      echo "Options:"
      echo "  -c, --config FILE    Path to configuration file (default: config/client.conf)"
      echo "  -s, --server HOST:PORT  Server address (default: localhost:10000)"
      echo "  -h, --help           Show this help message"
      echo ""
      echo "Commands:"
      echo "  get KEY              Get value for key"
      echo "  put KEY VALUE        Put key-value pair"
      echo "  delete KEY           Delete key"
      echo "  scan START END       Scan keys in range"
      echo "  stats                Get server statistics"
      echo "  interactive          Start interactive mode"
      echo ""
      echo "If no command is provided, interactive mode is started."
      exit 0
      ;;
    *)
      # First non-option argument is the command
      break
      ;;
  esac
done

# Build the base command
CMD="java $JAVA_OPTS -cp $JAR_FILE $MAIN_CLASS"
CMD="$CMD --config $CONFIG_FILE --server $SERVER"

# Process command-specific arguments
if [ $# -eq 0 ]; then
  # No command provided, start interactive mode
  CMD="$CMD interactive"
else
  COMMAND="$1"
  shift

  case "$COMMAND" in
    get|put|delete|scan|stats|interactive)
      CMD="$CMD $COMMAND"
      # Add remaining arguments
