set -e
cd ..
export WRAPPER="$1"
cat .travis.yml | grep script -A 1000 | cut -d "-" -f 2- | grep WRAPPER | grep scripts | while read ENTRY; do
	echo "========================================="
	echo $ENTRY
	eval $ENTRY
done
