#!/bin/bash

# Script to stop the DistributedKV server
# Usage: ./stop-server.sh [options]

# Default values
PID_FILE="server.pid"
FORCE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -f|--force)
      FORCE=true
      shift
      ;;
    -p|--pid-file)
      PID_FILE="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  -f, --force          Force kill the server"
      echo "  -p, --pid-file FILE  Path to PID file (default: server.pid)"
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

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
  echo "PID file not found: $PID_FILE"
  echo "Server may not be running or was started with a different PID file"
  exit 1
fi

# Read PID from file
PID=$(cat "$PID_FILE")
if [ -z "$PID" ]; then
  echo "Empty PID file: $PID_FILE"
  exit 1
fi

# Check if process is running
if ! ps -p "$PID" > /dev/null; then
  echo "Process with PID $PID not found"
  echo "Removing stale PID file: $PID_FILE"
  rm -f "$PID_FILE"
  exit 1
fi

# Stop the server
echo "Stopping DistributedKV server with PID: $PID"

if [ "$FORCE" = true ]; then
  echo "Forcefully killing the server..."
  kill -9 "$PID"
else
  echo "Gracefully shutting down the server..."
  kill "$PID"

  # Wait for process to terminate
  MAX_WAIT=30
  for i in $(seq 1 $MAX_WAIT); do
    if ! ps -p "$PID" > /dev/null; then
      break
    fi
    echo "Waiting for server to shut down... ($i/$MAX_WAIT)"
    sleep 1
  done

  # Force kill if still running
  if ps -p "$PID" > /dev/null; then
    echo "Server did not shut down gracefully after $MAX_WAIT seconds"
    echo "Forcefully killing the server..."
    kill -9 "$PID"
  fi
fi

# Remove PID file
rm -f "$PID_FILE"
echo "Server stopped"
