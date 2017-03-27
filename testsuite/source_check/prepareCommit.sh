BASEDIR=$PWD/../..
export PYTHONPATH=$BASEDIR/packages:$PYTHONPATH

echo "fixing version"
echo "-----------------------------"
python update_version.py

echo "fixing imports"
echo "-----------------------------"

python imports_all.py
python imports_check.py
python imports_sort.py

echo "============================="
echo "fixing compat imports"
echo "-----------------------------"

cd $BASEDIR
python packages/python_compat.py 2>&1 | grep -v requests | grep -v xmpp
cd - > /dev/null

echo "============================="
echo "updating plugins"
echo "-----------------------------"

cd $BASEDIR/packages
python grid_control_update.py
cd - > /dev/null

echo "============================="
echo "updating headers"
echo "-----------------------------"

python header_copyright.py | grep changed

echo "============================="
echo "updating notice"
echo "-----------------------------"

python commit_stats.py

echo "============================="
echo "updating documentation"
echo "-----------------------------"

python docgen_parse_code.py
python docgen_plugin_infos.py
python docgen.py stop > $BASEDIR/docs/config.rst
