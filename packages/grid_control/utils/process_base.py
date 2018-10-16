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

import os, time, errno, fcntl, select, signal, logging, termios
from grid_control.utils.thread_tools import GCEvent, GCLock, GCQueue, start_daemon, start_thread
from hpfwk import AbstractError, clear_current_exception, get_current_exception, ignore_exception
from python_compat import bytes2str, imap, set, str2bytes


class ProcessError(Exception):
	pass


class ProcessTimeout(ProcessError):
	pass


class Process(object):
	def __init__(self, cmd, *args, **kwargs):
		self._time_started = None
		self._time_finished = None
		self._event_shutdown = GCEvent()
		self._event_finished = GCEvent()
		self._buffer_stdin = GCQueue()
		self._buffer_stdout = GCQueue()
		self._buffer_stderr = GCQueue()
		self._env_dict = kwargs.pop('env_dict', None) or dict(os.environ)
		# Stream setup
		self._do_log = kwargs.pop('logging', True) or None
		self.stdout = ProcessReadStream(self._buffer_stdout, self._do_log,
			self._event_shutdown, self._event_finished)
		self.stderr = ProcessReadStream(self._buffer_stderr, self._do_log,
			self._event_shutdown, self._event_finished)
		self.stdin = ProcessWriteStream(self._buffer_stdin, self._do_log)
		self.clear_logs()  # reset log to proper start value

		self._args = []
		for arg in args:
			self._args.append(str(arg))
		if not cmd:
			raise ProcessError('Invalid executable!')
		if not os.path.isabs(cmd):  # Resolve executable path
			for path in os.environ.get('PATH', '').split(os.pathsep):
				candidate = os.path.join(path, cmd)
				if os.path.exists(candidate) and os.access(candidate, os.X_OK):
					cmd = candidate
					break
		if not os.path.exists(cmd):
			raise ProcessError('%r does not exist' % cmd)
		if not os.access(cmd, os.X_OK):
			raise ProcessError('%r is not executable' % cmd)
		self._log = logging.getLogger('process.%s' % os.path.basename(cmd).lower())
		self._log.debug('External program called: %s %s', cmd, self._args)
		self._cmd = cmd
		self.start()

	def __repr__(self):
		msg = 'cmd = %s, args = %s, status = %s' % (self._cmd, repr(self._args), self.status(0))
		if self._do_log:
			msg += ', stdin log = %r, stdout log = %r, stderr log = %r' % (
				self.stdin.read_log(), self.stdout.read_log(), self.stderr.read_log())
		return '%s(%s)' % (self.__class__.__name__, msg)

	def clear_logs(self):
		self.stdout.clear_log()
		self.stderr.clear_log()
		self.stdin.clear_log()

	def finish(self, timeout):
		status = self.status_raise(timeout)
		return (status, self.stdout.read(timeout=0), self.stderr.read(timeout=0))

	def get_call(self):
		return str.join(' ', imap(repr, [self._cmd] + self._args))

	def get_output(self, timeout, raise_errors=True):
		status = self.status(timeout)
		result = self.stdout.read(timeout=0)
		if status is None:
			self.terminate(timeout=1)
		if raise_errors and (status is None):
			raise ProcessTimeout('Process is still running after waiting for %s seconds' % timeout)
		elif raise_errors and (status != 0):
			raise ProcessError('Command %s %s returned with exit code %s' % (
				self._cmd, repr(self._args), status))
		return result

	def get_runtime(self):
		if self._time_started is not None:
			if self._time_finished is not None:
				return self._time_finished - self._time_started
			return time.time() - self._time_started

	def kill(self, sig=signal.SIGTERM):
		raise AbstractError

	def restart(self, timeout):
		if self.status(timeout=0) is None:
			self.terminate(timeout)
		result = self.status(timeout=0)
		self.start()
		return result

	def start(self):
		self.clear_logs()
		self._event_shutdown.clear()
		self._event_finished.clear()
		self.stdout.reset_buffer()
		self.stderr.reset_buffer()
		self.stdin.reset_buffer()
		return self._start()

	def status(self, timeout, terminate=False):
		raise AbstractError

	def status_raise(self, timeout):
		status = self.status(timeout)
		if status is None:
			self.terminate(timeout=1)  # hard timeout
			raise ProcessTimeout('Process is still running after waiting for %s seconds' % timeout)
		return status

	def terminate(self, timeout):
		raise AbstractError

	def _start(self):
		raise AbstractError


class ProcessStream(object):
	def __init__(self, buffer, log):
		(self._buffer, self._log) = (buffer, log)

	def __repr__(self):
		return '%s(buffer = %r)' % (self.__class__.__name__, self.read_log())

	def clear_log(self):
		result = self._log
		if self._log is not None:
			self._log = ''
		return result

	def read_log(self):
		if self._log is not None:
			return self._log
		return ''

	def reset_buffer(self):
		self._buffer.reset()


class LocalProcess(Process):
	fd_creation_lock = GCLock()

	def __init__(self, cmd, *args, **kwargs):
		self._signal_dict = {}
		for attr in dir(signal):
			if attr.startswith('SIG') and ('_' not in attr):
				self._signal_dict[getattr(signal, attr)] = attr
		terminal = kwargs.pop('term', 'vt100')
		(self._status, self._runtime, self._pid) = (None, None, None)
		Process.__init__(self, cmd, *args, **kwargs)
		if terminal is not None:
			self._env_dict['TERM'] = terminal

	def kill(self, sig=signal.SIGTERM):
		if not self._event_finished.is_set():
			try:
				os.kill(self._pid, sig)
			except OSError:
				if get_current_exception().errno != errno.ESRCH:  # errno.ESRCH: no such process (already dead)
					raise
				clear_current_exception()

	def status(self, timeout, terminate=False):
		self._event_finished.wait(timeout, 'process to finish')
		if self._status is False:
			return 'OS_ABORT'
		elif self._status is not None:  # return either signal name or exit code
			if os.WIFSIGNALED(self._status):
				return self._signal_dict.get(os.WTERMSIG(self._status), 'SIG_UNKNOWN')
			elif os.WIFEXITED(self._status):
				return os.WEXITSTATUS(self._status)
		if terminate:
			return self.terminate(timeout=1)

	def terminate(self, timeout):
		status = self.status(timeout=0)
		if status is not None:
			return status
		self.kill(signal.SIGTERM)
		result = self.status(timeout, terminate=False)
		if result is not None:
			return result
		self.kill(signal.SIGKILL)
		return self.status(timeout, terminate=False)

	def _handle_input(cls, fd_write, buffer, event_shutdown):
		local_buffer = ''
		while not event_shutdown.is_set():
			if local_buffer:  # local buffer has leftover bytes from last write - just poll for more
				local_buffer = buffer.get(timeout=0, default='')
			else:  # empty local buffer - wait for data to process
				local_buffer = buffer.get(timeout=1, default='')
			if local_buffer:
				_wait_fd(fd_write_list=[fd_write])
				if not event_shutdown.is_set():
					written = ignore_exception(OSError, 0, os.write, fd_write, str2bytes(local_buffer))
					local_buffer = local_buffer[written:]
	_handle_input = classmethod(_handle_input)

	def _handle_output(cls, fd_read, buffer, event_shutdown):
		def _read_to_buffer():
			while True:
				try:
					tmp = bytes2str(os.read(fd_read, 32 * 1024))
				except OSError:
					break
				if not tmp:
					break
				buffer.put(tmp)
		while not event_shutdown.is_set():
			_wait_fd(fd_read_list=[fd_read])
			_read_to_buffer()
		_read_to_buffer()  # Final readout after process finished
	_handle_output = classmethod(_handle_output)

	def _interact_with_child(self, pid, fd_parent_stdin, fd_parent_stdout, fd_parent_stderr):
		thread_in = self._start_watcher('stdin', False, pid,
			self._handle_input, fd_parent_stdin, self._buffer_stdin, self._event_shutdown)
		thread_out = self._start_watcher('stdout', False, pid,
			self._handle_output, fd_parent_stdout, self._buffer_stdout, self._event_shutdown)
		thread_err = self._start_watcher('stderr', False, pid,
			self._handle_output, fd_parent_stderr, self._buffer_stderr, self._event_shutdown)
		while self._status is None:
			# blocking (with spurious wakeups!) - OSError=unable to wait for child - status=False => OS_ABORT
			(result_pid, status) = ignore_exception(OSError, (pid, False), os.waitpid, pid, 0)
			if result_pid == pid:
				self._status = status
		self._time_finished = time.time()
		self._event_shutdown.set()  # start shutdown of handlers and wait for it to finish
		self._buffer_stdin.finish()  # wakeup process input handler
		thread_in.join()
		thread_out.join()
		thread_err.join()
		for fd_open in set([fd_parent_stdin, fd_parent_stdout, fd_parent_stderr]):
			os.close(fd_open)  # fd_parent_stdin == fd_parent_stdout for pty
		self._buffer_stdout.finish()  # wakeup pending output buffer waits
		self._buffer_stderr.finish()
		self._event_finished.set()

	def _setup_terminal(self, fd_terminal):
		attr = termios.tcgetattr(fd_terminal)
		attr[1] = attr[1] & ~termios.ONLCR  # disable \n -> \r\n
		attr[3] = attr[3] & ~termios.ECHO  # disable terminal echo
		attr[3] = attr[3] | termios.ICANON  # enable canonical mode
		attr[3] = attr[3] | termios.ISIG  # enable signals
		self.stdin.EOF = bytes2str(termios.tcgetattr(fd_terminal)[6][termios.VEOF])
		termios.tcsetattr(fd_terminal, termios.TCSANOW, attr)

	def _start(self):
		(self._status, self._runtime, self._pid) = (None, None, None)
		# Setup of file descriptors - stdin / stdout via pty, stderr via pipe
		LocalProcess.fd_creation_lock.acquire()
		try:
			# terminal is used for stdin / stdout
			fd_parent_terminal, fd_child_terminal = os.openpty()
			fd_parent_stdin, fd_child_stdin = (fd_parent_terminal, fd_child_terminal)
			fd_parent_stdout, fd_child_stdout = (fd_parent_terminal, fd_child_terminal)
			fd_parent_stderr, fd_child_stderr = os.pipe()  # Returns (r, w) FDs
		finally:
			LocalProcess.fd_creation_lock.release()

		self._setup_terminal(fd_parent_terminal)
		for fd_setup in [fd_parent_stdout, fd_parent_stderr]:  # non-blocking operation on stdout/stderr
			fcntl.fcntl(fd_setup, fcntl.F_SETFL, os.O_NONBLOCK | fcntl.fcntl(fd_setup, fcntl.F_GETFL))

		pid = os.fork()
		self._time_started = time.time()
		self._time_finished = None
		fd_map = {0: fd_child_stdin, 1: fd_child_stdout, 2: fd_child_stderr}
		if pid == 0:  # We are in the child process - redirect streams and exec external program
			from grid_control.utils.process_child import run_command
			run_command(self._cmd, [self._cmd] + self._args, fd_map, self._env_dict)

		else:  # Still in the parent process - setup threads to communicate with external program
			os.close(fd_child_terminal)
			os.close(fd_child_stderr)
			self._pid = pid
			self._start_watcher('interact', True, pid,
				self._interact_with_child, pid, fd_parent_stdin, fd_parent_stdout, fd_parent_stderr)

	def _start_watcher(self, desc, daemon, pid, *args):
		desc += ' (%d:%r)' % (pid, [self._cmd] + self._args)
		if daemon:
			return start_daemon(desc, *args)
		return start_thread(desc, *args)


class ProcessReadStream(ProcessStream):
	def __init__(self, buffer, log, event_shutdown, event_finished):
		ProcessStream.__init__(self, buffer, log)
		(self._event_shutdown, self._event_finished) = (event_shutdown, event_finished)
		self._iter_buffer = ''

	def iter(self, timeout, timeout_soft=False, timeout_shutdown=10):
		waited_for_shutdown = False
		while True:
			# yield lines from buffer
			while self._iter_buffer.find('\n') != -1:
				pos_eol = self._iter_buffer.find('\n')
				if self._log is not None:
					self._log += self._iter_buffer[:pos_eol + 1]
				yield self._iter_buffer[:pos_eol + 1]
				self._iter_buffer = self._iter_buffer[pos_eol + 1:]
			# block until new data in buffer / timeout or process is finished
			tmp = self._buffer.get(timeout, default='')
			if tmp:  # new data
				self._iter_buffer += tmp
			elif self._event_shutdown.is_set() and not waited_for_shutdown:  # shutdown in progress
				waited_for_shutdown = True
				# wait for shutdown to complete
				self._event_finished.wait(timeout_shutdown, 'process shutdown to complete')
			elif self._event_finished.is_set() or timeout_soft:
				break  # process finished / soft timeout
			else:
				# hard timeout
				raise ProcessTimeout('Stream did not yield more lines after waiting for %s seconds' % timeout)
		if self._iter_buffer:  # return rest of buffer
			if self._log is not None:
				self._log += self._iter_buffer
			yield self._iter_buffer

	def read(self, timeout, full=True):
		# read (full/partial) content from buffer after waiting for up timeout seconds
		result = self._buffer.get(timeout, default='')
		while full:
			try:
				result += self._buffer.get(timeout=0)
			except IndexError:
				break
		if self._log is not None:
			self._log += result
		return result

	def read_cond(self, timeout, cond):
		# wait until stream fulfills condition
		result = ''
		if timeout is not None:
			t_end = time.time() + timeout
		while True:
			if timeout is not None:
				timeout_left = max(0, t_end - time.time())
			result += self.read(timeout=timeout_left, full=False)
			if cond(result) or self._event_finished.is_set():
				break
			if timeout_left <= 0:
				msg = 'Stream result %r did not fulfill condition after waiting for %s seconds'
				raise ProcessTimeout(msg % (result, timeout))
		return result

	def read_log(self):
		result = self.read(timeout=0)  # flush buffer
		return ProcessStream.read_log(self) or result

	def wait(self, timeout):
		return self._buffer.wait_get(timeout)


class ProcessWriteStream(ProcessStream):
	def __init__(self, buffer, log):
		ProcessStream.__init__(self, buffer, log)
		self.EOF = ''  # pylint:disable=invalid-name

	def close(self):
		self.write(self.EOF)

	def write(self, value, log=True):
		if log and (self._log is not None):
			self._log += value
		self._buffer.put(value)


def _wait_fd(fd_read_list=None, fd_write_list=None, timeout=0.2):
	return select.select(fd_read_list or [], fd_write_list or [], [], timeout)
