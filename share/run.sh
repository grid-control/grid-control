#!/bin/sh

export MY_JOB="$1"
export MY_ID="`whoami`"
export MY_REAL="`pwd`"

echo "JOBID=$MY_JOB"
echo

echo -n "grid-control running on: "
hostname
uname -a
cat /etc/redhat-release

shift

if [ -n "$TMPDIR" ]; then
	export MY_SCRATCH="$TMPDIR/$$"
else
	export MY_SCRATCH="/tmp/$MY_ID/$$"
fi
MY_MOVED=0

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
echo "Exit code: $CODE"

echo "EXITCODE=$CODE" >> jobinfo.txt

if [ $MY_MOVED -eq 1 ]; then
	for i in stderr.txt stdout.txt jobinfo.txt $MY_OUT; do
		test -f "$i" && cp $i "$MY_REAL/"
	done
	cd "$MY_REAL/"
	rm -Rf "$MY_SCRATCH" &> /dev/null
	rmdir "/tmp/$MY_ID" &> /dev/null
fi

exit $CODE
