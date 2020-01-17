#!/bin/sh

echo "Downloading data"
rm -rf project-data
mkdir project-data && cd project-data || exit
wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/projekt2/airbnb.zip
echo "Downloaded data"

echo "Unpacking data"
unzip airbnb.zip
rm airbnb.zip
echo "Unpacked data"

cd ..

echo "Loading data into hadoop"
hadoop fs -mkdir -p project/spark/
hadoop fs -copyFromLocal project-data project/spark/
echo "Loaded data"
