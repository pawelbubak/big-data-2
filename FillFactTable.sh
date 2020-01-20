#!/bin/sh

echo "Filling out the fact table"

spark-submit --class FactETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 fact.jar
