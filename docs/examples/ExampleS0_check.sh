#!/bin/bash

if [ -n "$GC_CHECK_OUTPUT" ]; then
	SRCFILE="${1:-output.vars1}"
	echo
	(
	for OUTPUTDIR in $GC_WORKDIR/output/*; do
		echo $(cat $OUTPUTDIR/$SRCFILE)
	done
	) | sort -g | tee $GC_TASK_CONF.list
fi
