#!/bin/sh
set -e

# A node container that was killed and started again keeps stale pid files,
# which stop dockerd from starting. Remove them first.
rm -f /var/run/docker.pid /var/run/docker/containerd/containerd.pid

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
