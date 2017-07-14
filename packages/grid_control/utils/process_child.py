# | Copyright 2016-2017 Karlsruhe Institute of Technology
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


def run_command(cmd, args, fd_map, env):  # run command by replacing the current process
	def _safe_close(file_descriptor):
		try:
			os.close(file_descriptor)
		except Exception:
			pass

	for fd_target, fd_source in fd_map.items():
		os.dup2(fd_source, fd_target)  # set stdin/stdout/stderr
	try:
		fd_max = os.sysconf('SC_OPEN_MAX')
	except Exception:
		fd_max = 256
	for fd_open in irange(3, fd_max):  # close inherited file descriptors except for std{in/out/err}
		_safe_close(fd_open)
	try:
		os.execve(cmd, args, env)  # replace process - this command DOES NOT RETURN if successful!
	except Exception:
		pass
	error_msg_list = [
		'== grid-control process error ==',
		'        pid: %s' % os.getpid(),
		'     fd map: %s' % repr(fd_map),
		'environment: %s' % repr(env),
		'    command: %s' % repr(cmd),
		'  arguments: %s' % repr(args),
		'  exception: %s' % repr(sys.exc_info()[1]),
	]
	sys.stderr.write(str.join('\n', error_msg_list))
	for fd_std in [0, 1, 2]:
		_safe_close(fd_std)
	exit_without_cleanup(os.EX_OSERR)  # exit forked process with OS error
