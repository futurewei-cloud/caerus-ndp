#!/bin/bash

printf "\nspark: cleaning artifacts\n"
rm -rf build
rm -rf volume

# shellcheck disable=SC1091
source docker/setup.sh

DOCKERS="caerus-ndp-spark-base caerus-ndp-spark-base-${USER_NAME}"
printf "\nspark: cleaning dockers: %s\n" "${DOCKERS}"
DOCKER_RM_CMD="docker rmi ${DOCKERS}"
eval "$DOCKER_RM_CMD"

printf "\nspark: cleaning done\n"
