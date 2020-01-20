#!/bin/sh

echo "Filling out the location score table"

spark-submit --class LocationScoreETL --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 location-score.jar
