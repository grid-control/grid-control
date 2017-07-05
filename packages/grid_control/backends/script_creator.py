# | Copyright 2017 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

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
