OLDPWD=$PWD
cd $(dirname $0)
SEARCH="$2"
set +e
for EXE in `find | grep "$SEARCH" | grep TEST | grep -v pyc | sort | grep -v svn | grep -v scale | grep -v fuzz`; do
	grep -q ${EXE#./} ../.travis.yml || echo -n "not tested: "
	$1 $EXE 2>&1 > /dev/null
	RESULT=$?
	echo "$RESULT - $1 - ${EXE#./}"
done
