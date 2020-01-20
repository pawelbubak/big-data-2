#!/bin/sh

echo "Filling out the price table"

spark-submit --class PriceETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 price.jar
