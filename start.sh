#! /bin/bash

pushd spark
./start.sh || (echo "*** failed start of spark $?" ; exit 1)
popd

pushd hadoop
./start.sh bin/start_hadoop.sh || (echo "*** failed start of hadoop $?" ; exit 1)
popd

pushd benchmark
./run.sh --gen
popd

printf "\nSuccessfully started all servers.\n"
