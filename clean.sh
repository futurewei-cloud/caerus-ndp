#!/bin/bash

pushd datasource
./clean.sh
popd

pushd benchmark
./clean.sh
popd

pushd spark
./clean.sh
popd

pushd hadoop
./clean.sh
popd

echo "Clean Done"
