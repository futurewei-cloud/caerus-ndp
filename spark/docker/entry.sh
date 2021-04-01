#!/bin/bash

echo "$0 PID $BASHPID"

_term() { 
  echo "Caught SIGTERM signal!" 
  exit 0
}

trap _term SIGTERM

echo "PID $BASHPID SIGTERM trap enabled"
echo "$@"

if [ "${RUNNING_MODE:-RUNNING_MODE_DEFAULT}" = "daemon" ]; then
    eval "$@" &
    child=$! 
    wait "$child"
    exit
fi

eval "$@"
