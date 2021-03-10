#! /bin/bash

echo "Building dockers"
cd spark/docker
./build_docker.sh
cd ../..
echo "Building dockers complete"

echo "Building benchmark"
cd benchmark
./build.sh
cd ..
echo "Building benchmark complete"


echo "Building datasource"
cd datasource
./build.sh
cd ..
echo "Building datasource complete"

echo "Build of ndp complete"
