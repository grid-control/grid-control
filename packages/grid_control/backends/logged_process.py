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

import os, time, errno, signal, logging, python_compat_popen2
from grid_control.gc_exceptions import GCError
from grid_control.utils import DictFormat, abort
from grid_control.utils.file_tools import VirtualFile
from hpfwk import clear_current_exception, get_current_exception
from python_compat import tarfile


class LoggedProcess(object):
	def __init__(self, cmd, args='', nice_cmd=None, nice_args=None, shell=True):
		if nice_cmd:
			self.nice_cmd = nice_cmd
		else:
			self.nice_cmd = os.path.basename(cmd)
		if nice_args:
			self.nice_args = nice_args
		else:
			self.nice_args = args
		(self.stdout, self.stderr, self.cmd, self.args) = ([], [], cmd, args)
		self._logger = logging.getLogger('process.%s' % os.path.basename(cmd).lower())
		self._logger.log(logging.DEBUG1, 'External programm called: %s %s', self.nice_cmd, self.nice_args)
		self._stime = time.time()
		if shell:
			self.proc = python_compat_popen2.Popen3('%s %s' % (cmd, args), True)
		else:
			if isinstance(cmd, str):
				cmd = [cmd]
			if isinstance(args, str):
				args = args.split()
			self.proc = python_compat_popen2.Popen3(cmd + list(args), True)

	def get_all(self):
		self.stdout.extend(self.proc.fromchild.readlines())
		self.stderr.extend(self.proc.childerr.readlines())
		return (self.wait(), self.stdout, self.stderr)

	def get_error(self):
		self.stderr.extend(self.proc.childerr.readlines())
		return str.join('', self.stderr)

	def get_message(self):
		return self.get_output() + '\n' + self.get_error()

	def get_output(self, wait=False):
		if wait:
			self.wait()
		self.stdout.extend(self.proc.fromchild.readlines())
		return str.join('', self.stdout)

	def iter(self):
		while True:
			try:
				line = self.proc.fromchild.readline()
			except Exception:
				clear_current_exception()
				abort(True)
				break
			if not line:
				break
			self.stdout.append(line)
			yield line

	def kill(self):
		try:
			os.kill(self.proc.pid, signal.SIGTERM)
		except OSError:
			if get_current_exception().errno != errno.ESRCH:  # errno.ESRCH: no such process (already dead)
				raise
			clear_current_exception()

	def log_error(self, target, brief=False, **kwargs):
		# Can also log content of additional files via kwargs
		now = time.time()
		entry = '%s.%s' % (time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(now)),
			('%.5f' % (now - int(now)))[2:])
		self._logger.log_time(logging.WARNING, '%s failed with code %d', self.nice_cmd, self.wait())
		if not brief:
			self._logger.log_time(logging.WARNING, '\n%s', self.get_error())

		try:
			tar = tarfile.TarFile.open(target, 'a')
			data = {'exit_code': self.wait(), 'exec': self.cmd, 'args': self.args}
			files = [VirtualFile(os.path.join(entry, 'info'), DictFormat().format(data))]
			kwargs.update({'stdout': self.get_output(), 'stderr': self.get_error()})
			for key, value in kwargs.items():
				try:
					content = open(value, 'r').readlines()
				except Exception:
					clear_current_exception()
					content = [value]
				files.append(VirtualFile(os.path.join(entry, key), content))
			for file_obj in files:
				info, handle = file_obj.get_tar_info()
				tar.addfile(info, handle)
				handle.close()
			tar.close()
		except Exception:
			raise GCError('Unable to log errors of external process "%s" to "%s"' % (self.nice_cmd, target))
		self._logger.info('All logfiles were moved to %s', target)

	def poll(self):
		return self.proc.poll()

	def wait(self, timeout=-1, kill=True):
		if timeout <= 0:
			return self.proc.wait()
		while self.poll() < 0 and timeout > (time.time() - self._stime):
			time.sleep(1)
		if kill and timeout > (time.time() - self._stime):
			self.kill()
		return self.poll()
