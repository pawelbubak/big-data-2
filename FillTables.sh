#!/bin/sh

echo "Filling the location table"
spark-submit --class LocationETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 location.jar

echo "Filling the location score table"
spark-submit --class LocationScoreETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 location-score.jar

echo "Filling the price table"
spark-submit --class PriceETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 price.jar

echo "Filling the time table"
spark-submit --class TimeETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 time.jar

echo "Filling the fact table"
spark-submit --class FactETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 fact.jar
