#-#  Copyright 2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, pty, sys, time, errno, fcntl, select, signal, logging, termios, threading
from grid_control.utils.thread_tools import GCEvent, GCLock, GCQueue
from hpfwk import AbstractError
from python_compat import ifilter, imap, irange, lmap

try:
	FD_MAX = os.sysconf('SC_OPEN_MAX')
except (AttributeError, ValueError):
	FD_MAX = 256

SIGNAL_DICT = dict(imap(lambda s: (getattr(signal, s), s),\
	ifilter(lambda c: c.startswith('SIG') and ('_' not in c), dir(signal))))

def safeClose(fd):
	try:
		os.close(fd)
	except OSError:
		pass

class ProcessTimeout(Exception):
	pass

class Process(object):
	def __init__(self, cmd, *args, **kwargs):
		self._process_finished = GCEvent()
		self._buffer_stdin = GCQueue(str)
		self._buffer_stdout = GCQueue(str)
		self._buffer_stderr = GCQueue(str)
		self._iter_buffer_stdout = ''
		# Stream logging setup
		self._logging = kwargs.get('logging', True)
		self.clear_logs()

		args = lmap(str, args)
		if not cmd:
			raise RuntimeError('Invalid executable!')
		if not os.path.isabs(cmd): # Resolve executable path
			for path in os.environ.get('PATH', '').split(os.pathsep):
				if os.path.exists(os.path.join(path, cmd)):
					cmd = os.path.join(path, cmd)
					break
		if not os.access(cmd, os.X_OK):
			raise OSError('Unable to execute %r' % cmd)
		self._log = logging.getLogger('process.%s' % os.path.basename(cmd))
		self._log.debug('External programm called: %s %s', cmd, args)
		(self._cmd, self._args) = (cmd, lmap(str, args))
		self.start()

	def clear_logs(self):
		(self._log_stdin, self._log_stdout, self._log_stderr) = ('', '', '')

	def __repr__(self):
		return '%s(status = %s, flushed stdout = %r, flushed stderr = %r)' % (
			self.__class__.__name__, self.status(0), self.read_stdout_log(), self.read_stderr_log())

	def get_call(self):
		return '%s %s' % (self._cmd, str.join(' ', self._args))

	def start(self):
		raise AbstractError

	def kill(self, sig = signal.SIGTERM):
		raise AbstractError

	def restart(self):
		if self.status(0) is None:
			self.kill()
		self.start()

	def status(self, timeout, terminate = False):
		raise AbstractError

	def finish(self, timeout):
		result = self.status(timeout)
		tmp = (result, self.read_stdout(timeout = 0), self.read_stderr(timeout = 0))
		if result is None:
			raise ProcessTimeout # hard timeout
		return tmp

	def _wait_stream(self, read_stream_fun, timeout, cond):
		result = ''
		state = None
		while True:
			t_start = time.time()
			result += read_stream_fun(timeout = timeout)
			timeout -= time.time() - t_start
			if cond(result):
				break
			if state is not None: # check before update to make at least one last read from stream
				break
			state = self.status(0)
			if timeout < 0:
				raise ProcessTimeout
		return result

	def read_stdout(self, timeout):
		result = self._buffer_stdout.get(timeout)
		if self._logging:
			self._log_stdout += result
		return result

	def read_stdout_log(self):
		self.read_stdout(0) # flush buffer into log
		return self._log_stdout

	def wait_stdout(self, timeout, cond):
		return self._wait_stream(self.read_stdout, timeout, cond)

	def read_stderr(self, timeout):
		result = self._buffer_stderr.get(timeout)
		if self._logging:
			self._log_stderr += result
		return result

	def read_stderr_log(self):
		self.read_stderr(0) # flush buffer into log
		return self._log_stderr

	def wait_stderr(self, timeout, cond):
		return self._wait_stream(self.read_stderr, timeout, cond)

	def write_stdin(self, value, log = True):
		if log and self._logging:
			self._log_stdin += value
		self._buffer_stdin.put(value)

	def read_stdin_log(self):
		return self._log_stdin

	def close_stdin(self):
		raise AbstractError

	def iter_stdout(self, timeout, timeout_soft = False, timeout_shutdown = 10):
		waitedForShutdown = False
		while True:
			# yield lines from buffer
			while self._iter_buffer_stdout.find('\n') != -1:
				posEOL = self._iter_buffer_stdout.find('\n')
				yield self._iter_buffer_stdout[:posEOL + 1]
				self._iter_buffer_stdout = self._iter_buffer_stdout[posEOL + 1:]
			# block until new data in buffer / timeout or process is finished
			tmp = self._buffer_stdout.get(timeout)
			if tmp: # new data
				self._iter_buffer_stdout += tmp
			elif self._process_shutdown.is_set() and not waitedForShutdown: # shutdown in progress
				waitedForShutdown = True
				self._process_finished.wait(timeout_shutdown, 'process shutdown to complete') # wait for shutdown to complete
			elif self._process_finished.is_set() or timeout_soft:
				break # process finished / soft timeout
			else:
				raise ProcessTimeout # hard timeout
		if self._iter_buffer_stdout: # return rest of buffer
			yield self._iter_buffer_stdout


class LocalProcess(Process):
	def __init__(self, cmd, *args, **kwargs):
		self._status = None
		Process.__init__(self, cmd, *args, **kwargs)

	def start(self):
		# Setup of file descriptors
		LocalProcess.fdCreationLock.acquire()
		try:
			fd_parent_stderr, fd_child_stderr = os.pipe() # Returns (r, w) FDs
		finally:
			LocalProcess.fdCreationLock.release()

		self._pid, self._fd_terminal = pty.fork()
		fd_parent_stdin = self._fd_terminal
		fd_parent_stdout = self._fd_terminal
		if self._pid == 0: # We are in the child process - redirect streams and exec external program
			os.environ['TERM'] = 'vt100'
			os.dup2(fd_child_stderr, 2)
			for fd in irange(3, FD_MAX):
				safeClose(fd)
			try:
				os.execv(self._cmd, [self._cmd] + self._args)
			except Exception:
				sys.stderr.write('Error while calling os.execvp: ' + repr(sys.exc_info()[1]))
				for fd in irange(0, 3):
					safeClose(fd)
				os._exit(os.EX_OSERR)
			os._exit(os.EX_OK)

		else: # Still in the parent process - setup threads to communicate with external program
			safeClose(fd_child_stderr)
			for fd in [fd_parent_stdout, fd_parent_stderr]:
				fcntl.fcntl(fd, fcntl.F_SETFL, os.O_NONBLOCK | fcntl.fcntl(fd, fcntl.F_GETFL))
			self._process_shutdown = GCEvent() # flag to start shutdown of output handlers
			self._process_shutdown_stdin = GCEvent() # flag to start shutdown of input handlers

			def handleOutput(fd, buffer):
				def readToBuffer():
					while True:
						try:
							tmp = os.read(fd, 32*1024)
						except OSError:
							tmp = ''
						if not tmp:
							break
						buffer.put(tmp)
				while not self._process_shutdown.is_set():
					try:
						select.select([fd], [], [], 0.2)
					except Exception:
						pass
					readToBuffer()
				readToBuffer() # Final readout after process finished
				safeClose(fd)

			def handleInput():
				local_buffer = ''
				while not self._process_shutdown.is_set():
					if local_buffer: # if local buffer ist leftover from last write - just poll for more
						local_buffer += self._buffer_stdin.get(timeout = 0)
					else: # empty local buffer - wait for data to process
						local_buffer = self._buffer_stdin.get(timeout = 1)
					if local_buffer:
						try:
							(rl, write_list, xl) = select.select([], [fd_parent_stdin], [], 0.2)
						except Exception:
							pass
						if write_list and not self._process_shutdown.is_set():
							written = os.write(fd_parent_stdin, local_buffer)
							local_buffer = local_buffer[written:]
					elif self._process_shutdown_stdin.is_set():
						break
				safeClose(fd_parent_stdin)

			def checkStatus():
				thread_in = threading.Thread(target = handleInput)
				thread_in.start()
				thread_out = threading.Thread(target = handleOutput, args = (fd_parent_stdout, self._buffer_stdout))
				thread_out.start()
				thread_err = threading.Thread(target = handleOutput, args = (fd_parent_stderr, self._buffer_stderr))
				thread_err.start()
				while self._status is None:
					try:
						(pid, status) = os.waitpid(self._pid, 0) # blocking (with spurious wakeups!)
					except OSError: # unable to wait for child
						(pid, status) = (self._pid, -1)
					if pid == self._pid:
						self._status = status
				self._process_shutdown.set() # start shutdown of handlers and wait for it to finish
				self._process_shutdown_stdin.set() # start shutdown of handlers and wait for it to finish
				self._buffer_stdin.finish() # wakeup process input handler
				thread_in.join()
				thread_out.join()
				thread_err.join()
				self._buffer_stdout.finish() # wakeup pending output buffer waits
				self._buffer_stderr.finish()
				self._process_finished.set()

			thread = threading.Thread(target = checkStatus)
			thread.daemon = True
			thread.start()
		self.setup_terminal()

	def setup_terminal(self):
		attr = termios.tcgetattr(self._fd_terminal)
		attr[1] = attr[1] & (~termios.ONLCR) | termios.ONLRET
		attr[3] = attr[3] & ~termios.ECHO
		termios.tcsetattr(self._fd_terminal, termios.TCSANOW, attr)

	def write_stdin_eof(self):
		self.write_stdin(chr(ord(termios.tcgetattr(self._fd_terminal)[6][termios.VEOF])))

	def status(self, timeout, terminate = False):
		self._process_finished.wait(timeout, 'process to finish')
		if self._status is not None: # return either signal name or exit code
			if os.WIFSIGNALED(self._status):
				return SIGNAL_DICT.get(os.WTERMSIG(self._status), 'SIG_UNKNOWN')
			elif os.WIFEXITED(self._status):
				return os.WEXITSTATUS(self._status)
		if terminate:
			self.kill(signal.SIGTERM)
			result = self.status(timeout = 1, terminate = False)
			if result is not None:
				return result
			self.kill(signal.SIGKILL)
			return self.status(timeout = 1, terminate = False)

	def kill(self, sig = signal.SIGTERM):
		if not self._process_finished.is_set():
			try:
				os.kill(self._pid, sig)
			except OSError:
				if sys.exc_info()[1].errno != errno.ESRCH: # errno.ESRCH: no such process (already dead)
					raise

LocalProcess.fdCreationLock = GCLock()
