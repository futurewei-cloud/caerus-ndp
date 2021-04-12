#! /bin/bash
set -e

printf "\nBuilding spark/docker\n"
pushd spark/docker
./build.sh || (echo "*** spark docker build failed with $?" ; exit 1)
popd
printf "\nBuilding spark docker complete\n"

printf "\nBuilding hadoop\n"
pushd hadoop
./build.sh || (echo "*** hadoop build failed with $?" ; exit 1)
popd
printf "\nBuilding hadoop complete\n"

printf "\nBuilding benchmark\n"
pushd benchmark
./build.sh || (echo "*** benchmark build failed with $?" ; exit 1)
popd
printf "\nBuilding benchmark complete\n"

printf "\nBuilding datasource\n"
pushd datasource
./build.sh || (echo "*** datasource build failed with $?" ; exit 1)
popd
printf "\nBuilding datasource complete\n"

printf "\nBuild of ndp complete\n"
