#!/bin/sh

echo "Filling out the location table"

spark-submit --class LocationETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 location.jar
