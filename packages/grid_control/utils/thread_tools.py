# | Copyright 2015-2016 Karlsruhe Institute of Technology
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

import time, logging, threading
from hpfwk import ExceptionCollector, NestedException, get_current_exception
from python_compat import get_current_thread, get_thread_name


BLOCKING_EQUIVALENT = 60 * 60 * 24 * 7  # instead of blocking, we wait for a week ;)


def hang_protection(fun, timeout=5):
	# Function to protect against hanging system calls
	result = {}

	def hang_protection_wrapper():
		try:
			result[None] = fun()
		except Exception:
			result[None] = None
	thread = start_thread('hang protection for %s' % fun, hang_protection_wrapper)
	thread.join(timeout)
	if None not in result:
		raise TimeoutException
	return result[None]


def start_daemon(desc, fun, *args, **kwargs):
	return _start_thread(desc, True, fun, *args, **kwargs)


def start_thread(desc, fun, *args, **kwargs):
	return _start_thread(desc, False, fun, *args, **kwargs)


def tchain(iterable_list, timeout=None,
		ex_cls=NestedException, ex_msg='Caught exception during threaded chain'):
	# Combines multiple, threaded generators into single generator
	threads = []
	result = GCQueue()
	exc = ExceptionCollector()
	for idx, iterable in enumerate(iterable_list):
		def generator_thread(iterator):  # TODO: Python 3.5 hickup related to pep 479?
			try:
				try:
					for item in iterator:
						result.put(item)
				finally:
					result.put(GCQueue)  # Use GCQueue as end-of-generator marker
			except Exception:
				exc.collect()
		threads.append(start_daemon('generator thread %d' % idx, generator_thread, iterable))

	if timeout is not None:
		t_end = time.time() + timeout
	while len(threads):
		if timeout is not None:
			timeout = max(0, t_end - time.time())
		try:
			tmp = result.get(timeout)
		except IndexError:  # Empty queue after waiting for timeout
			break
		if tmp == GCQueue:
			threads.pop()  # which thread is irrelevant - only used as counter
		else:
			yield tmp
	exc.raise_any(ex_cls(ex_msg))


class GCEvent(object):
	# Event with blocking, interruptible wait and python >= 2.7 return value
	def __init__(self):
		self._cond = threading.Condition(threading.Lock())
		try:
			self._cond_notify_all = self._cond.notify_all
		except Exception:
			self._cond_notify_all = self._cond.notifyAll
		self._flag = False

	def clear(self):
		self._cond.acquire()
		try:
			self._flag = False
		finally:
			self._cond.release()
		return False

	def is_set(self):
		return self._flag

	def set(self):
		self._cond.acquire()
		try:
			self._flag = True
			self._cond_notify_all()
		finally:
			self._cond.release()
		return True

	def wait(self, timeout, description='event'):
		if timeout is None:
			timeout = BLOCKING_EQUIVALENT
		self._cond.acquire()
		try:
			try:
				if not self._flag:
					self._cond.wait(timeout)
				return self._flag  # return current flag state after wait / wakeup
			finally:
				self._cond.release()
		except KeyboardInterrupt:
			raise KeyboardInterrupt('Interrupted while waiting for %s' % description)


class GCLock(object):
	# Lock with optional acquire timeout
	def __init__(self, lock=None):
		self._lock = lock or threading.Lock()

	def acquire(self, timeout=None):
		try:
			if timeout == 0:  # Non-blocking
				return self._lock.acquire(False)
			if timeout is None:  # Blocking
				timeout = BLOCKING_EQUIVALENT
			# using the threading.Condition algorithm for polling the lock
			t_end = time.time() + timeout  # TODO: protect against time unloading during shutdown
			dt_sleep = 0.0005
			while True:
				lockstate = self._lock.acquire(False)
				if lockstate:
					return lockstate
				dt_remaining = t_end - time.time()
				if dt_remaining <= 0:
					raise TimeoutException
				dt_sleep = min(dt_sleep * 2, dt_remaining, 0.05)
				time.sleep(dt_sleep)
		except KeyboardInterrupt:
			raise KeyboardInterrupt('Interrupted while waiting to acquire lock')

	def release(self):
		self._lock.release()


class GCQueue(object):
	# thread-safe communication channel with put / get
	def __init__(self):
		self._lock = GCLock()
		self._notify = GCEvent()
		self._finished = GCEvent()
		self._queue = []

	def __repr__(self):
		return '%s(%r)' % (self.__class__.__name__, self._queue)

	def finish(self):
		self._finished.set()
		self._notify.set()

	def get(self, timeout, default=IndexError):  # IndexError is a magic value to raise an exception
		self._notify.wait(timeout)
		self._lock.acquire()
		try:
			if not self._queue:
				if default == IndexError:
					raise IndexError('Queue is empty!')
				result = default
			else:
				result = self._queue.pop(0)
			if (not self._finished.is_set()) and (not self._queue):
				self._notify.clear()
		finally:
			self._lock.release()
		return result

	def put(self, value):
		self._lock.acquire()
		try:
			self._queue.append(value)
			self._notify.set()
		finally:
			self._lock.release()

	def reset(self):
		self._lock.acquire()
		try:
			self._queue = []
			self._finished.clear()
			self._notify.clear()
		finally:
			self._lock.release()

	def wait_get(self, timeout):
		return self._notify.wait(timeout)


class GCThreadPool(object):
	# Class to manage a collection of threads
	def __init__(self):
		self._lock = GCLock()
		self._notify = GCEvent()
		self._token = 0
		self._token_time = {}
		self._token_desc = {}
		self._log = logging.getLogger('thread_pool')
		self._exc = ExceptionCollector(self._log)

	def start_daemon(self, desc, fun, *args, **kwargs):
		self._lock.acquire()
		try:
			self._token += 1
			self._token_time[self._token] = time.time()
			self._token_desc[self._token] = desc
		finally:
			self._lock.release()
		start_daemon(desc, self._run_thread, self._token, fun, args, kwargs)

	def wait_and_drop(self, timeout=None):
		while True:
			self._lock.acquire()
			try:
				t_current = time.time()
				# discard stale threads
				for token in list(self._token_time):
					if timeout and (t_current - self._token_time.get(token, 0) > timeout):
						self._token_time.pop(token, None)
						self._token_desc.pop(token, None)
				if not self._token_time:  # no active threads
					return True
				# drop all threads if timeout is reached
				if (timeout is not None) and (timeout <= 0):
					self._token_time = {}
					self._token_desc = {}
					return False
			finally:
				self._lock.release()
			# wait for thread to finish and adapt timeout for next round
			self._notify.wait(timeout)
			if timeout is not None:
				timeout -= time.time() - t_current

	def _run_thread(self, token, fun, args, kwargs):
		try:
			fun(*args, **kwargs)
		except Exception:
			self._lock.acquire()
			try:
				self._exc.collect(logging.ERROR, 'Exception in thread %r',
					self._token_desc[token], exc_info=get_current_exception())
			finally:
				self._lock.release()
		self._lock.acquire()
		try:
			self._token_time.pop(token, None)
			self._token_desc.pop(token, None)
		finally:
			self._lock.release()
		self._notify.set()


class TimeoutException(Exception):
	pass


def _start_thread(desc, daemon, fun, *args, **kwargs):
	# determine thread name (name contains parentage)
	_start_thread.lock.acquire()
	try:
		_start_thread.counter += 1
	finally:
		_start_thread.lock.release()
	thread_name_parent = get_thread_name(get_current_thread()).replace('Mainthread', 'T')
	thread_name_new = '%s-%d' % (thread_name_parent, _start_thread.counter)
	# create new thread
	thread = threading.Thread(name=thread_name_new, target=fun, args=args, kwargs=kwargs)
	thread.desc = desc
	thread.setDaemon(daemon)
	thread.start()
	return thread
_start_thread.counter = 0
_start_thread.lock = GCLock()
