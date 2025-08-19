#!/bin/bash
# shellcheck shell=bash

set -e

echo "Running tests with pprof..."
mkdir -p pprofs
rm -rf pprofs/*

for d in $(go list ./...); do
    package_name=$(basename "$d")
    echo "Running tests for $d with pprof..."
    if ! go test -v -cpuprofile="pprofs/${package_name}_cpu.pprof" -memprofile="pprofs/${package_name}_mem.pprof" "$d"; then
        if [ -f "pprofs/${package_name}_cpu.pprof" ]; then mv "pprofs/${package_name}_cpu.pprof" "pprofs/${package_name}_cpu.pprof"; fi
        if [ -f "pprofs/${package_name}_mem.pprof" ]; then mv "pprofs/${package_name}_mem.pprof" "pprofs/${package_name}_mem.pprof"; fi
        exit 1
    fi
    if [ -f "pprofs/${package_name}_cpu.pprof" ]; then mv "pprofs/${package_name}_cpu.pprof" "pprofs/${package_name}_cpu.pprof"; fi
    if [ -f "pprofs/${package_name}_mem.pprof" ]; then mv "pprofs/${package_name}_mem.pprof" "pprofs/${package_name}_mem.pprof"; fi
    if [ -f "${package_name}.test" ]; then rm "${package_name}.test"; fi
done

echo "Pprof files generated for each package in the 'pprofs' directory (e.g., pprofs/<package_name>_cpu.pprof, pprofs/<package_name>_mem.pprof)."
echo "To view a CPU profile: go tool pprof -http=:8080 pprofs/<package_name>_cpu.pprof"
echo "To view a Memory profile: go tool pprof -http=:8080 pprofs/<package_name>_mem.pprof"
echo "Remember to replace <package_name> with the actual package name."
echo "You can clean up these files manually after inspection."
