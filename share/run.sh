#!/bin/sh

export MY_JOB="$1"
if [ $2 == "LSF" ];then  
    export INDIR="$3"
    export INBOX="$INDIR/sandbox.tar.gz";
    export MODE="LSF"
fi;
export MY_ID="`whoami`"
export MY_REAL="`pwd`"

echo "JOBID=$MY_JOB"
echo "FULL PARAMETER=$@"
echo "INBOX=$INBOX"
echo

echo -n "grid-control running on: "
hostname
uname -a
cat /etc/redhat-release

shift
if [ $MODE == "LSF" ]; then
    shift
    shift
fi

sleep 30

#if [ -n "$TMPDIR" ]; then
#	export MY_SCRATCH="$TMPDIR/$$"
#else
#	export MY_SCRATCH="/tmp/$MY_ID/$$"
#fi

export MY_SCRATCH="$MY_REAL/$MY_ID/$$"

MY_MOVED=0

if [ -n $INBOX ]; then
    echo "cp $INBOX $MY_REAL"
    cp $INBOX $MY_REAL
fi

rm -Rf "$MY_SCRATCH" &> /dev/null
mkdir -p "$MY_SCRATCH" &> /dev/null
test -d "$MY_SCRATCH" && MY_MOVED=1
if [ $MY_MOVED -eq 1 ]; then
	cd "$MY_SCRATCH/"
else
	export MY_SCRATCH="$MY_REAL"
fi

cleanup() {
	if [ $MY_MOVED -eq 1 ]; then
		cd "$MY_REAL/"
		rm -Rf "$MY_SCRATCH" &> /dev/null
		rmdir "/tmp/$MY_ID" &> /dev/null
	fi
}

trap cleanup 0 1 2 3 15

if [ ! -f "$MY_REAL/sandbox.tar.gz" ]; then
	echo sandbox.tar.gz missing 1>&2
	exit 1
fi

echo ""
echo "---------------------------"
echo "Unpacking environment"
tar xvfz "$MY_REAL/sandbox.tar.gz" || exit 3

if ! test -f _config.sh; then
	echo "_config.sh missing" 1>&2
	exit 1
fi

source _config.sh

if ! test -n "$MY_RUNTIME"; then
	echo "MY_RUNTIME is not set" 1>&2
	exit 1
fi

echo "JOBID=$MY_JOB" > jobinfo.txt

eval "$MY_RUNTIME"
CODE=$?

echo "---------------------------"
echo "Job exit code: $CODE"

if [ $CODE -eq 0 ]; then
	if [ -n "$SE_OUTPUT_FILES" ]; then
	        echo "---------------------------"
		echo "Output to storage element enabled!"
	        echo "Copying the following files:"
	        eval "echo $SE_OUTPUT_FILES"
	        echo ""
	        echo "to the following SE path:"
	        echo $SE_PATH
		BADCOUNT=0
	        for i in $SE_OUTPUT_FILES; do 
			if ! eval "globus-url-copy file://`pwd`/\"\$i\" \"\$SE_PATH\"/job_\"\$MY_JOB\"_\"\$i\""; then
				BADCOUNT=$[$BADCOUNT+1]
			fi				
		done
		if [ $BADCOUNT -ne 0 ]; then
			CODE=1
		else
			CODE=0
		fi		
		echo "Copy exit code: $CODE"
	fi
fi

echo "EXITCODE=$CODE" >> jobinfo.txt

if [ $MY_MOVED -eq 1 ]; then
	for i in stderr.txt stdout.txt jobinfo.txt $SB_OUTPUT_FILES; do
		test -f "$i" && cp $i "$MY_REAL/"
	done
	cd "$MY_REAL/"
	rm -Rf "$MY_SCRATCH" &> /dev/null
	rmdir "/tmp/$MY_ID" &> /dev/null
fi

if [ $MODE=="LSF" ]; then
    for i in jobinfo.txt $SB_OUTPUT_FILES; do
	test -f "$i" && cp $i $INDIR/output/tmp/job_$MY_JOB
    done;
fi

exit $CODE
