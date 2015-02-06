#!/bin/bash

echo "Start the hive shell with the following arguments : $*"
mvn exec:exec -Dexec.arguments="$*" -Phiveshell


