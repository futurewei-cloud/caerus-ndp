#!/bin/bash

echo "$0 PID $BASHPID"

_term() { 
  echo "Caught SIGTERM signal!" 
  exit 0
}

trap _term SIGTERM

echo "PID $BASHPID SIGTERM trap enabled"
echo "$@"

if [ $RUNNING_MODE = "daemon" ]; then
    eval "$@" &
    child=$! 
    wait "$child"
fi

eval "$@"
