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

import os, time, errno, fcntl, select, signal, logging, termios
from grid_control.utils.thread_tools import GCEvent, GCLock, GCQueue, create_thread
from hpfwk import AbstractError, get_current_exception
from python_compat import bytes2str, imap, set, str2bytes

def waitFD(read = None, write = None, timeout = 0.2):
	return select.select(read or [], write or [], [], timeout)


class ProcessError(Exception):
	pass

class ProcessTimeout(ProcessError):
	pass


class ProcessStream(object):
	def __init__(self, buffer, log):
		(self._buffer, self._log) = (buffer, log)

	def read_log(self):
		if self._log is not None:
			return self._log
		return ''

	def reset_buffer(self):
		self._buffer.reset()

	def clear_log(self):
		result = self._log
		if self._log is not None:
			self._log = ''
		return result

	def __repr__(self):
		return '%s(buffer = %r)' % (self.__class__.__name__, self.read_log())


class ProcessReadStream(ProcessStream):
	def __init__(self, buffer, event_shutdown, event_finished, log = None):
		ProcessStream.__init__(self, buffer, log)
		(self._event_shutdown, self._event_finished, self._iter_buffer) = (event_shutdown, event_finished, '')

	def wait(self, timeout):
		return self._buffer.wait_get(timeout)

	def read(self, timeout):
		result = self._buffer.get(timeout, default = '')
		while True:
			try:
				result += self._buffer.get(timeout = 0)
			except IndexError:
				break
		if self._log is not None:
			self._log += result
		return result

	def read_log(self):
		result = self.read(0) # flush buffer
		return ProcessStream.read_log(self) or result

	# wait until stream fulfills condition
	def read_cond(self, timeout, cond):
		result = ''
		if timeout is not None:
			t_end = time.time() + timeout
		while True:
			if timeout is not None:
				timeout_left = max(0, t_end - time.time())
			result += self.read(timeout = timeout_left)
			if cond(result) or self._event_finished.is_set():
				break
			if timeout_left <= 0:
				raise ProcessTimeout('Stream result %r did not fulfill condition after waiting for %s seconds' % (result, timeout))
		return result

	def iter(self, timeout, timeout_soft = False, timeout_shutdown = 10):
		waitedForShutdown = False
		while True:
			# yield lines from buffer
			while self._iter_buffer.find('\n') != -1:
				posEOL = self._iter_buffer.find('\n')
				if self._log is not None:
					self._log += self._iter_buffer[:posEOL + 1]
				yield self._iter_buffer[:posEOL + 1]
				self._iter_buffer = self._iter_buffer[posEOL + 1:]
			# block until new data in buffer / timeout or process is finished
			tmp = self._buffer.get(timeout, default = '')
			if tmp: # new data
				self._iter_buffer += tmp
			elif self._event_shutdown.is_set() and not waitedForShutdown: # shutdown in progress
				waitedForShutdown = True
				self._event_finished.wait(timeout_shutdown, 'process shutdown to complete') # wait for shutdown to complete
			elif self._event_finished.is_set() or timeout_soft:
				break # process finished / soft timeout
			else:
				raise ProcessTimeout('Stream did not yield more lines after waiting for %s seconds' % timeout) # hard timeout
		if self._iter_buffer: # return rest of buffer
			if self._log is not None:
				self._log += self._iter_buffer
			yield self._iter_buffer


class ProcessWriteStream(ProcessStream):
	def __init__(self, buffer, log):
		ProcessStream.__init__(self, buffer, log)
		self.EOF = ''
		self.EOL = ''
		self.INTR = ''

	def write(self, value, log = True):
		if log and (self._log is not None):
			self._log += value
		self._buffer.put(value)

	def close(self):
		self.write(self.EOF)


class Process(object):
	def __init__(self, cmd, *args, **kwargs):
		self._time_started = None
		self._time_finished = None
		self._event_shutdown = GCEvent()
		self._event_finished = GCEvent()
		self._buffer_stdin = GCQueue()
		self._buffer_stdout = GCQueue()
		self._buffer_stderr = GCQueue()
		self._env = kwargs.pop('environment', None) or dict(os.environ)
		# Stream setup
		self._do_log = kwargs.pop('logging', True) or None
		self.stdout = ProcessReadStream(self._buffer_stdout, self._event_shutdown, self._event_finished, log = self._do_log)
		self.stderr = ProcessReadStream(self._buffer_stderr, self._event_shutdown, self._event_finished, log = self._do_log)
		self.stdin = ProcessWriteStream(self._buffer_stdin, log = self._do_log)
		self.clear_logs() # reset log to proper start value

		self._args = []
		for arg in args:
			self._args.append(str(arg))
		if not cmd:
			raise ProcessError('Invalid executable!')
		if not os.path.isabs(cmd): # Resolve executable path
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
		self._log.debug('External programm called: %s %s', cmd, self._args)
		self._cmd = cmd
		self.start()

	def get_runtime(self):
		if self._time_started is not None:
			if self._time_finished is not None:
				return self._time_finished - self._time_started
			return time.time() - self._time_started

	def __repr__(self):
		msg = 'cmd = %s, args = %s, status = %s' % (self._cmd, repr(self._args), self.status(0))
		if self._do_log:
			msg += ', stdin log = %r, stdout log = %r, stderr log = %r' % (self.stdin.read_log(), self.stdout.read_log(), self.stderr.read_log())
		return '%s(%s)' % (self.__class__.__name__, msg)

	def clear_logs(self):
		self.stdout.clear_log()
		self.stderr.clear_log()
		self.stdin.clear_log()

	def get_call(self):
		return str.join(' ', imap(repr, [self._cmd] + self._args))

	def start(self):
		self.clear_logs()
		self._event_shutdown.clear()
		self._event_finished.clear()
		self.stdout.reset_buffer()
		self.stderr.reset_buffer()
		self.stdin.reset_buffer()
		return self._start()

	def _start(self):
		raise AbstractError

	def terminate(self, timeout):
		raise AbstractError

	def kill(self, sig = signal.SIGTERM):
		raise AbstractError

	def restart(self, timeout):
		if self.status(timeout = 0) is None:
			self.terminate(timeout)
		result = self.status(0)
		self.start()
		return result

	def status(self, timeout, terminate = False):
		raise AbstractError

	def status_raise(self, timeout):
		status = self.status(timeout)
		if status is None:
			self.terminate(timeout = 1)
			raise ProcessTimeout('Process is still running after waiting for %s seconds' % timeout) # hard timeout
		return status

	def get_output(self, timeout, raise_errors = True):
		status = self.status(timeout)
		result = self.stdout.read(timeout = 0)
		if status is None:
			self.terminate(timeout = 1)
		if raise_errors and (status is None):
			raise ProcessTimeout('Process is still running after waiting for %s seconds' % timeout)
		elif raise_errors and (status != 0):
			raise ProcessError('Command %s %s returned with exit code %s' % (self._cmd, repr(self._args), status))
		return result

	def finish(self, timeout):
		status = self.status_raise(timeout)
		return (status, self.stdout.read(timeout = 0), self.stderr.read(timeout = 0))


class LocalProcess(Process):
	def __init__(self, cmd, *args, **kwargs):
		self._signal_dict = {}
		for attr in dir(signal):
			if attr.startswith('SIG') and ('_' not in attr):
				self._signal_dict[getattr(signal, attr)] = attr
		terminal = kwargs.pop('term', 'vt100')
		Process.__init__(self, cmd, *args, **kwargs)
		if terminal is not None:
			self._env['TERM'] = terminal

	def _start(self):
		(self._status, self._runtime, self._pid) = (None, None, None)
		# Setup of file descriptors - stdin / stdout via pty, stderr via pipe
		LocalProcess.fdCreationLock.acquire()
		try:
			self._fd_parent_terminal, fd_child_terminal = os.openpty() # terminal is used for stdin / stdout
			fd_parent_stdin, fd_child_stdin = (self._fd_parent_terminal, fd_child_terminal)
			fd_parent_stdout, fd_child_stdout = (self._fd_parent_terminal, fd_child_terminal)
			fd_parent_stderr, fd_child_stderr = os.pipe() # Returns (r, w) FDs
		finally:
			LocalProcess.fdCreationLock.release()

		self._setup_terminal()
		for fd in [fd_parent_stdout, fd_parent_stderr]: # enable non-blocking operation on stdout/stderr
			fcntl.fcntl(fd, fcntl.F_SETFL, os.O_NONBLOCK | fcntl.fcntl(fd, fcntl.F_GETFL))

		pid = os.fork()
		self._time_started = time.time()
		self._time_ended = None
		if pid == 0: # We are in the child process - redirect streams and exec external program
			from grid_control.utils.process_child import run_process
			run_process(self._cmd, [self._cmd] + self._args, fd_child_stdin, fd_child_stdout, fd_child_stderr, self._env)

		else: # Still in the parent process - setup threads to communicate with external program
			os.close(fd_child_terminal)
			os.close(fd_child_stderr)
			self._pid = pid
			self._start_thread('interact', True, pid, self._interact_with_child, pid, fd_parent_stdin, fd_parent_stdout, fd_parent_stderr)

	def _start_thread(self, desc, daemon, pid, *args):
		thread = create_thread(*args)
		thread.daemon = daemon
		thread.desc = desc + ' (%d:%r)' % (pid, [self._cmd] + self._args)
		thread.start()
		return thread

	def _interact_with_child(self, pid, fd_parent_stdin, fd_parent_stdout, fd_parent_stderr):
		thread_in = self._start_thread('stdin', False, pid, self._handle_input, fd_parent_stdin, self._buffer_stdin, self._event_shutdown)
		thread_out = self._start_thread('stdout', False, pid, self._handle_output, fd_parent_stdout, self._buffer_stdout, self._event_shutdown)
		thread_err = self._start_thread('stderr', False, pid, self._handle_output, fd_parent_stderr, self._buffer_stderr, self._event_shutdown)
		while self._status is None:
			try:
				(result_pid, status) = os.waitpid(pid, 0) # blocking (with spurious wakeups!)
			except OSError: # unable to wait for child
				(result_pid, status) = (pid, False) # False == 'OS_ABORT'
			if result_pid == pid:
				self._status = status
		self._time_ended = time.time()
		self._event_shutdown.set() # start shutdown of handlers and wait for it to finish
		self._buffer_stdin.finish() # wakeup process input handler
		thread_in.join()
		thread_out.join()
		thread_err.join()
		for fd in set([fd_parent_stdin, fd_parent_stdout, fd_parent_stderr]): # fd_parent_stdin == fd_parent_stdout for pty
			os.close(fd)
		self._buffer_stdout.finish() # wakeup pending output buffer waits
		self._buffer_stderr.finish()
		self._event_finished.set()

	def _handle_output(cls, fd, buffer, event_shutdown):
		def readToBuffer():
			while True:
				try:
					tmp = bytes2str(os.read(fd, 32*1024))
				except OSError:
					tmp = ''
				if not tmp:
					break
				buffer.put(tmp)
		while not event_shutdown.is_set():
			waitFD(read = [fd])
			readToBuffer()
		readToBuffer() # Final readout after process finished
	_handle_output = classmethod(_handle_output)

	def _handle_input(cls, fd, buffer, event_shutdown):
		local_buffer = ''
		while not event_shutdown.is_set():
			if local_buffer: # if local buffer has leftover bytes from last write - just poll for more
				local_buffer = buffer.get(timeout = 0, default = '')
			else: # empty local buffer - wait for data to process
				local_buffer = buffer.get(timeout = 1, default = '')
			if local_buffer:
				waitFD(write = [fd])
				if not event_shutdown.is_set():
					try:
						written = os.write(fd, str2bytes(local_buffer))
					except OSError:
						written = 0
					local_buffer = local_buffer[written:]
	_handle_input = classmethod(_handle_input)

	def _setup_terminal(self):
		attr = termios.tcgetattr(self._fd_parent_terminal)
		attr[1] = attr[1] & ~termios.ONLCR # disable \n -> \r\n
		attr[3] = attr[3] & ~termios.ECHO # disable terminal echo
		attr[3] = attr[3] | termios.ICANON # enable canonical mode
		attr[3] = attr[3] | termios.ISIG # enable signals
		self.stdin.EOF = bytes2str(termios.tcgetattr(self._fd_parent_terminal)[6][termios.VEOF])
		self.stdin.EOL = bytes2str(termios.tcgetattr(self._fd_parent_terminal)[6][termios.VEOL])
		self.stdin.INTR = bytes2str(termios.tcgetattr(self._fd_parent_terminal)[6][termios.VINTR])
		termios.tcsetattr(self._fd_parent_terminal, termios.TCSANOW, attr)

	def status(self, timeout, terminate = False):
		self._event_finished.wait(timeout, 'process to finish')
		if self._status is False:
			return 'OS_ABORT'
		elif self._status is not None: # return either signal name or exit code
			if os.WIFSIGNALED(self._status):
				return self._signal_dict.get(os.WTERMSIG(self._status), 'SIG_UNKNOWN')
			elif os.WIFEXITED(self._status):
				return os.WEXITSTATUS(self._status)
		if terminate:
			return self.terminate(timeout = 1)

	def terminate(self, timeout):
		status = self.status(timeout = 0)
		if status is not None:
			return status
		self.kill(signal.SIGTERM)
		result = self.status(timeout, terminate = False)
		if result is not None:
			return result
		self.kill(signal.SIGKILL)
		return self.status(timeout, terminate = False)

	def kill(self, sig = signal.SIGTERM):
		if not self._event_finished.is_set():
			try:
				os.kill(self._pid, sig)
			except OSError:
				if get_current_exception().errno != errno.ESRCH: # errno.ESRCH: no such process (already dead)
					raise

LocalProcess.fdCreationLock = GCLock()
