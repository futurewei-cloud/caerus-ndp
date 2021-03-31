#!/bin/bash

printf "\nspark: cleaning artifacts\n"
rm -rf build
rm -rf volume

source docker/setup.sh

DOCKERS = "caerus-ndp-spark-base caerus-ndp-spark-base-${USER_NAME}"
printf "\nspark: cleaning dockers: ${DOCKERS}\n"
#docker rmi ${DOCKERS}

printf "\nspark: cleaning done\n"
