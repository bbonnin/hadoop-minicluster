#!/bin/bash

echo "Start the mini-cluster with the following arguments : $*"
mvn exec:exec -Dexec.arguments="$*" -Pcluster


