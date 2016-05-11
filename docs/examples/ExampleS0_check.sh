#!/bin/bash

SRCFILE="${1:-output.vars1}"
if [ -z "$1" ]; then
	export
fi
(
for OUTPUTDIR in $GC_WORKDIR/output/*; do
	echo $(cat $OUTPUTDIR/$SRCFILE)
done
) | sort -g | tee $GC_TASK_CONF.list
