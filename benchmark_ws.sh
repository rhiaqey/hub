#!/bin/bash

# Start multiple instances of websocat in the background to simulate concurrent connections
# shellcheck disable=SC2034
for i in {1..250}; do
    websocat -n ws://localhost:3002/ws?channels=ticks &
done

# Wait for all background jobs to finish
sleep 1000

# Wait for a duration to allow connections to establish and messages to be exchanged
# sleep 1000

# Close all websocat instances
pkill -f "websocat"
