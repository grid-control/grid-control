#!/bin/bash

# (jobNum, sandbox, stdout, stderr) (local.sh) (...)
export SANDBOX="$2"
STDOUT="$3"
STDERR="$4"
shift 4
cd $SANDBOX
(
	nice $@
) > "$STDOUT" 2> "$STDERR" &
echo $!
