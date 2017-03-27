set -e
cd ..
rm -rf docs/examples/work.*
export WRAPPER="$1"
export GC_SCRIPT="./go.py"
cat .travis.yml | grep script -A 1000 | cut -d "-" -f 2- | grep WRAPPER | grep -v TEST_ | grep -v export | while read ENTRY; do
	echo $ENTRY
	eval $ENTRY
done
echo "************************************************"
echo "ALL TESTS COMPLETED"
echo "************************************************"
