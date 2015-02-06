#!/bin/bash

echo "Start the jdbc client tests with the following arguments : $*"
mvn exec:exec -Dexec.arguments="$*" -Pjdbcclient
