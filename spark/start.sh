#!/bin/bash

./docker/start-master.sh && sleep 5 && ./docker/start-worker.sh

sleep 5