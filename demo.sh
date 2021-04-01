#!/bin/bash

INTERACTIVE=1
GENERATE_DATABASE=1
if [ "$#" -ge 1 ]; then
  if [ "$1" == "-help" ]; then
    printf "\ndemo.sh: [-quiet] [-skipgen]\n"
    exit 0
  fi
  if [ "$1" == "-quiet" ]; then
    INTERACTIVE=0
    shift
  fi
  if [ "$1" == "-skipgen" ]; then
    GENERATE_DATABASE=0
    shift
  fi
fi

pushd benchmark

if [ $GENERATE_DATABASE -eq 1 ] ; then
  printf "\nFirst we will generate the TPC-H database.  This may take a few minutes.\n"
  if [ $INTERACTIVE -eq 1 ] ; then
    read -n 1 -s -r -p "Press any key to continue."
  fi
  ./test.sh --gen
  printf "\nTPC-H database generation complete.\n"
fi
printf "\nNext Test: Spark TPC-H query with HDFS storage and with no pushdown\n"

if [ $INTERACTIVE -eq 1 ] ; then
  read -n 1 -s -r -p "Press any key to continue with test."
fi
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