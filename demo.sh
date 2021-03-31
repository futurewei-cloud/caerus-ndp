#!/bin/bash

INTERACTIVE=1
if [ "$#" -ge 1 ] ; then
  if [ "$1" == "-quiet" ] ; then
    INTERACTIVE=0
  fi
fi
printf "\nNext Test: Spark TPC-H query with HDFS storage and with no pushdown\n"

if [ $INTERACTIVE -eq 1 ] ; then
  read -n 1 -s -r -p "Press any key to continue with test."
fi
pushd benchmark
./test.sh -t 6
printf "\nTest Complete: Spark TPC-H query with HDFS storage and with no pushdown\n"

printf "\nNext Test: Spark TPC-H query with HDFS storage and with pushdown enabled.\n"
if [ $INTERACTIVE -eq 1 ] ; then
  read -n 1 -s -r -p "Press any key to continue with test."
fi
./test.sh -t 6 --pushdown
printf "\nTest Complete: Spark TPC-H query with HDFS storage and with pushdown enabled.\n"

popd
printf "\nDemo Complete\n"