#!/bin/bash

# (jobNum, sandbox, stdout, stderr) (local.sh) (...)
export GC_SANDBOX="$2"
GC_STDOUT="$3"
GC_STDERR="$4"
shift 4
cd $GC_SANDBOX
(
	nice $@
) > "$GC_STDOUT" 2> "$GC_STDERR" &
echo $!
