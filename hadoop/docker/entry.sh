#!/bin/bash

#sudo /etc/init.d/ssh start
#export PDSH_RCMD_TYPE=ssh
#ssh-keyscan -H localhost >> ~/.ssh/known_hosts

echo "$@"
eval "$@"