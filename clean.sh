#!/bin/bash

cd datasource
./clean.sh

cd ../benchmark
./clean.sh

cd ../spark
./clean.sh

echo "Clean Done"