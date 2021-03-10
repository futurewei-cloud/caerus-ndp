#!/bin/bash

./docker/run_master.sh & sleep 5 && ./docker/run_worker.sh &
