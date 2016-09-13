set -e
cd ..
export WRAPPER="$1"
cat .travis.yml | cut -d "-" -f 2- | grep WRAPPER | grep scripts | while read ENTRY; do
	echo $ENTRY
	eval $ENTRY
done
