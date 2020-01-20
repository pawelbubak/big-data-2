#!/bin/sh

echo "Filling out the time table"

spark-submit --class TimeETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 time.jar
