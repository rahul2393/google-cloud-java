#!/bin/bash
target=45000
run=1
while true
do
    echo "Running the loop $run"
    ./bin/ycsb run cloudspanner -P cloudspanner/conf/cloudspanner.properties -P workloads/workloadb -p recordcount=1000000000 -p operationcount=3000000 -threads 300 -target ${target} -s > ycsb.$run.log 2>&1
    ((run++))
    if [ $((run - 3)) -gt 0 ]; then
        echo "Cleaning up old log file ycsb.$((run - 3)).log"
        rm ycsb.$((run - 3)).log
    fi
done
