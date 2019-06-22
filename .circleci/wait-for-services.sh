#!/bin/bash
set -e

for service in mysql:3306; do
    "$(dirname "$0")/wait-for-it.sh" -t 60 "$service"
done
