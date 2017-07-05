import os, stat
from grid_control.utils import create_tarball
from python_compat import str2bytes


def create_shell_script(fn, ft_list, exec_fn):
	fp = open(fn, 'wb')
	try:
		fp.write(str2bytes("""#!/bin/sh
cd $(dirname $0)
tail -n+6 $0 | tar xz
test "$1" != "unpack" && exec %s
exit 0
""" % exec_fn))
		create_tarball(ft_list, fileobj=fp)
	finally:
		fp.close()
	os.chmod(fn, stat.S_IRWXU)
