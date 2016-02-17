#!/bin/bash

export
(
for OUTPUTDIR in $GC_WORKDIR/output/*; do
	echo $(cat $OUTPUTDIR/output.vars1)
done
) | sort -g | tee $GC_TASK_CONF.list
