#!/usr/bin/env bash

set -e               # exit on error

pushd "$(dirname "$0")" # connect to root
ROOT_DIR=$(pwd)
pushd "${ROOT_DIR}/projection-plugin"
mvn package
popd
popd