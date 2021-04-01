#! /bin/bash

pushd spark
./stop.sh
popd

pushd hadoop 
./stop.sh 
popd

printf "\nAll containers stopped successfully\n"
