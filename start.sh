#! /bin/bash

pushd spark
./start.sh || (echo "*** failed start of spark $?" ; exit 1)
popd

pushd hadoop
./start.sh || (echo "*** failed start of hadoop $?" ; exit 1)
# Wait for hadoop to start before disabling safe mode.
# at least 20 seconds is needed before hadoop accepts commands.
echo "Waiting for hadoop to start"
sleep 20
./disable-safe-mode.sh || (echo "*** failed hadoop disable of safe mode$?" ; exit 1)
popd

pushd benchmark
./test.sh --gen
popd

printf "\nSuccessfully started all servers.\n"
