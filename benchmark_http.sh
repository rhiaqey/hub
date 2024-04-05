#!/bin/bash

# shellcheck disable=SC2034
for i in {1..250}; do
    curl -n http://localhost:3001/admin/api/hub
done

# Wait for all background jobs to finish
sleep 1000

