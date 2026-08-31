#!/bin/sh
set -e

# Start Docker daemon in background
dockerd-entrypoint.sh dockerd &

# Wait for docker socket to be ready
echo "Waiting for Docker daemon to initialize..."
while ! docker info >/dev/null 2>&1; do
    sleep 0.5
done
echo "Docker daemon ready on node $NODE_NAME."

# Launch the worker agent
exec node dist/index.js
