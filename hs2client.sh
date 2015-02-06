#!/bin/bash

echo "Start the hive server2 client tests with the following arguments : $*"
mvn exec:exec -Dexec.arguments="$*" -Phs2client
