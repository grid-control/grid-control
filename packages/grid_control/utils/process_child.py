# | Copyright 2016 Karlsruhe Institute of Technology
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

import os, sys
from python_compat import exit_without_cleanup, irange

try:
	FD_MAX = os.sysconf('SC_OPEN_MAX')
except (AttributeError, ValueError):
	FD_MAX = 256

def close_safe(fd):
	try:
		os.close(fd)
	except OSError:
		pass

def run_process(cmd, args, fd_child_stdin, fd_child_stdout, fd_child_stderr, env):
	for fd_target, fd_source in enumerate([fd_child_stdin, fd_child_stdout, fd_child_stderr]):
		os.dup2(fd_source, fd_target) # set stdin/stdout/stderr
	for fd in irange(3, FD_MAX):
		close_safe(fd)
	try:
		os.execve(cmd, args, env)
	except Exception:
		err_msg = 'Error while calling os.execv(%s, %s):' % (repr(cmd), repr(args))
		sys.stderr.write(err_msg + repr(sys.exc_info()[1]))
		for fd in [0, 1, 2]:
			close_safe(fd)
		exit_without_cleanup(os.EX_OSERR)
	exit_without_cleanup(os.EX_OK)
