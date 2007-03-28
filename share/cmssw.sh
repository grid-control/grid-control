#!/bin/sh

echo "CMSSW module starting"
echo "---------------------"

source _config.sh

_find() {
	if test -f "$MY_SCRATCH/$1"; then
		echo "$MY_SCRATCH/$1"
	elif test -f "$MY_REAL/$1"; then
		echo "$MY_REAL/$1"
	else
		echo "$1 not found" 2>&1
		exit 1
	fi
}

if ! [ -n "$VO_CMS_SW_DIR" ]; then
	echo VO_CMS_SW_DIR undefined 2>&1
	exit 1
fi

if [ ! -f "$VO_CMS_SW_DIR/cmsset_default.sh" ]; then
	echo "$VO_CMS_SW_DIR/cmsset_default.sh" not found 2>&1
	exit 1
fi

saved_SCRAM_VERSION="$SCRAM_VERSION"
source "$VO_CMS_SW_DIR/cmsset_default.sh"
SCRAM_VERSION="$saved_SCRAM_VERSION"

SCRAM="`which \"\$SCRAM_VERSION\"`"
if [ -z "$SCRAM" ]; then
	echo "$SCRAM_VERSION not found" 2>&1
	exit 1
fi

if ! $SCRAM project CMSSW $SCRAM_PROJECTVERSION; then
	echo "SCRAM project area setup failed" 2>&1
	exit 1
fi

if ! test -d "$SCRAM_PROJECTVERSION"; then
	echo "SCRAM project area not found" 2>&1
	exit 1
fi

cd "$SCRAM_PROJECTVERSION"
if ! [ "$HAS_RUNTIME" = no ]; then
	tar xvfz "`_find runtime.tar.gz`"
fi

echo "---------------------------"

EVENTS="$1"
SKIP="$2"
FNAMES="\"$3\""
shift 3
for i in "$@"; do
	FNAMES="$FNAMES, \"$i\""
done

SEED_REPLACER=""
j=0
for i in $SEEDS; do
	SEED_REPLACER="$SEED_REPLACER -e s@__SEED_${j}__@$[i+MY_JOB]@"
	j=$[j+1]
done

for i in $CMSSW_CONFIG; do
	echo "*** $i:"
	sed -e "s@__FILE_NAMES__@$FNAMES@" \
	    -e "s@__MAX_EVENTS__@$EVENTS@" \
	    -e "s@__SKIP_EVENTS__@$SKIP@" \
	    $SEED_REPLACER \
	    < "`_find $i`"
done

echo "---------------------------"

eval `$SCRAM runtime -sh`

mkdir -p workdir &> /dev/null
cd workdir

eval "for i in $USER_INFILES; do mv \"\$MY_SCRATCH/\$i\" .; done"
ls -la

echo "---------------------------"

for i in $CMSSW_CONFIG; do
	sed -e "s@__FILE_NAMES__@$FNAMES@" \
	    -e "s@__MAX_EVENTS__@$EVENTS@" \
	    -e "s@__SKIP_EVENTS__@$SKIP@" \
	    < "`_find $i`" > "$i"

	if [ "$GZIP_OUT" = "yes" ]; then
		rm -f cmssw_out.txt
		mknod cmssw_out1.txt p

		gzip -9 -c cmssw_out1.txt > cmssw_out.txt.gz &
		ls -la
		cat /proc/mounts
		cmsRun "$i" &> cmssw_out1.txt
		CODE=$?
		wait
		rm -f cmssw_out1.txt
        else 
		cmsRun "$i"
		CODE=$?
	fi

	if [ $CODE -ne 0 ]; then
		exit $CODE
	else
		echo "---------------------------"
	fi
done

ls -la

eval "for i in $MY_OUT; do mv \"\$i\" \"\$MY_SCRATCH\" &> /dev/null; done"

exit 0
