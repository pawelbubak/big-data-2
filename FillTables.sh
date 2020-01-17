#!/bin/sh

spark-submit --class Time --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 time.jar

spark-submit --class Location --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 location.jar

spark-submit --class LocationScore --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 location-score.jar

spark-submit --class Price --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 price.jar
