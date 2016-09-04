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

import os, time, errno, signal, logging, python_compat_popen2
from grid_control.gc_exceptions import GCError
from grid_control.utils import DictFormat, QM, abort
from grid_control.utils.file_objects import VirtualFile
from hpfwk import get_current_exception
from python_compat import tarfile

class LoggedProcess(object):
	def __init__(self, cmd, args = '', niceCmd = None, niceArgs = None, shell = True):
		self.niceCmd = QM(niceCmd, niceCmd, os.path.basename(cmd))
		self.niceArgs = QM(niceArgs, niceArgs, args)
		(self.stdout, self.stderr, self.cmd, self.args) = ([], [], cmd, args)
		self._logger = logging.getLogger('process.%s' % os.path.basename(cmd).lower())
		self._logger.log(logging.DEBUG1, 'External programm called: %s %s', self.niceCmd, self.niceArgs)
		self.stime = time.time()
		if shell:
			self.proc = python_compat_popen2.Popen3('%s %s' % (cmd, args), True)
		else:
			if isinstance(cmd, str):
				cmd = [cmd]
			if isinstance(args, str):
				args = args.split()
			self.proc = python_compat_popen2.Popen3( cmd + list(args), True)

	def getOutput(self, wait = False):
		if wait:
			self.wait()
		self.stdout.extend(self.proc.fromchild.readlines())
		return str.join('', self.stdout)

	def getError(self):
		self.stderr.extend(self.proc.childerr.readlines())
		return str.join('', self.stderr)

	def getMessage(self):
		return self.getOutput() + '\n' + self.getError()

	def kill(self):
		try:
			os.kill(self.proc.pid, signal.SIGTERM)
		except OSError:
			if get_current_exception().errno != errno.ESRCH: # errno.ESRCH: no such process (already dead)
				raise

	def iter(self):
		while True:
			try:
				line = self.proc.fromchild.readline()
			except Exception:
				abort(True)
				break
			if not line:
				break
			self.stdout.append(line)
			yield line

	def wait(self, timeout = -1, kill = True):
		if not timeout > 0:
			return self.proc.wait()
		while self.poll() < 0 and timeout > ( time.time() - self.stime ):
			time.sleep(1)
		if kill and timeout > ( time.time() - self.stime ):
			self.kill()
		return self.poll()

	def poll(self):
		return self.proc.poll()

	def getAll(self):
		self.stdout.extend(self.proc.fromchild.readlines())
		self.stderr.extend(self.proc.childerr.readlines())
		return (self.wait(), self.stdout, self.stderr)

	def logError(self, target, brief=False, **kwargs): # Can also log content of additional files via kwargs
		now = time.time()
		entry = '%s.%s' % (time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(now)), ('%.5f' % (now - int(now)))[2:])
		self._logger.log_time(logging.WARNING, '%s failed with code %d', self.niceCmd, self.wait())
		if not brief:
			self._logger.log_time(logging.WARNING, '\n%s', self.getError())

		try:
			tar = tarfile.TarFile.open(target, 'a')
			data = {'retCode': self.wait(), 'exec': self.cmd, 'args': self.args}
			files = [VirtualFile(os.path.join(entry, 'info'), DictFormat().format(data))]
			kwargs.update({'stdout': self.getOutput(), 'stderr': self.getError()})
			for key, value in kwargs.items():
				try:
					content = open(value, 'r').readlines()
				except Exception:
					content = [value]
				files.append(VirtualFile(os.path.join(entry, key), content))
			for fileObj in files:
				info, handle = fileObj.getTarInfo()
				tar.addfile(info, handle)
				handle.close()
			tar.close()
		except Exception:
			raise GCError('Unable to log errors of external process "%s" to "%s"' % (self.niceCmd, target))
		self._logger.info('All logfiles were moved to %s', target)


