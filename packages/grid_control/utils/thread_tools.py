# | Copyright 2015-2017 Karlsruhe Institute of Technology
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

import sys, time, logging, threading
from hpfwk import ExceptionCollector, NestedException, clear_current_exception, get_current_exception, get_thread_name, get_trace_fun, ignore_exception  # pylint:disable=line-too-long


BLOCKING_EQUIVALENT = 60 * 60 * 24 * 7  # instead of blocking, we wait for a week ;)


class TimeoutException(Exception):
	pass


def hang_protection(fun, timeout=5):
	# Function to protect against hanging system calls
	result = {}

	def _hang_protection_wrapper():
		result[None] = ignore_exception(Exception, None, fun)
	thread = start_thread('hang protection for %s' % fun, _hang_protection_wrapper)
	thread.join(timeout)
	if None not in result:
		raise TimeoutException
	return result[None]


def start_daemon(desc, fun, *args, **kwargs):
	return _start_thread(desc=desc, daemon=True,
		fun=_default_thread_wrapper(fun), args=args, kwargs=kwargs)


def start_thread(desc, fun, *args, **kwargs):
	return _start_thread(desc=desc, daemon=False,
		fun=_default_thread_wrapper(fun), args=args, kwargs=kwargs)


def tchain(iterable_iter, timeout=None, max_concurrent=None,
		ex_cls=NestedException, ex_msg='Caught exception during threaded chain'):
	# Combines multiple, threaded generators into single generator
	threads = []
	result = GCQueue()
	exc = ExceptionCollector()
	iterable_list = list(iterable_iter)

	def _start_generators():
		while iterable_list and ((max_concurrent is None) or (len(threads) < max_concurrent)):
			iterable = iterable_list.pop(0)
			threads.append(start_daemon('tchain generator thread (%s)' % repr(iterable)[:50],
				_tchain_thread, exc, iterable, result))
	_start_generators()

	if timeout is not None:
		t_end = time.time() + timeout
	while len(threads):
		if timeout is not None:
			timeout = max(0, t_end - time.time())
		try:
			tmp = result.get(timeout)
		except IndexError:  # Empty queue after waiting for timeout
			clear_current_exception()
			break
		if tmp == GCQueue:
			threads.pop()  # which thread is irrelevant - only used as counter
			_start_generators()
		else:
			yield tmp
	exc.raise_any(ex_cls(ex_msg))


def with_lock(lock, fun, *args, **kwargs):
	lock.acquire()
	try:
		return fun(*args, **kwargs)
	finally:
		lock.release()


class GCEvent(object):
	# Event with blocking, interruptible wait and python >= 2.7 return value
	def __init__(self, rlock=False):
		if rlock:  # signal handlers using events need to use rlock
			lock = threading.RLock()
		else:
			lock = threading.Lock()
		self._cond = threading.Condition(lock)
		try:
			self._cond_notify_all = self._cond.notify_all
		except Exception:
			clear_current_exception()
			self._cond_notify_all = self._cond.notifyAll
		self._flag = False

	def clear(self):
		def _clear_flag():
			self._flag = False
			return False
		return with_lock(self._cond, _clear_flag)

	def is_set(self):
		return self._flag

	def set(self):
		def _set_flag():
			self._flag = True
			self._cond_notify_all()
			return True
		return with_lock(self._cond, _set_flag)

	def wait(self, timeout, description='event'):
		def _wait(_timeout):
			try:
				if not self._flag:
					self._cond.wait(_timeout)
				return self._flag  # return current flag state after wait / wakeup
			except KeyboardInterrupt:
				raise KeyboardInterrupt('Interrupted while waiting for %s' % description)
		if timeout is None:
			timeout = BLOCKING_EQUIVALENT
		return with_lock(self._cond, _wait, timeout)


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
			t_end = time.time() + timeout
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
	def __init__(self, limit=None):
		self._lock = GCLock()
		self._notify = GCEvent()
		(self._limit, self._queue) = (limit, [])
		(self._token, self._token_time, self._token_desc) = (0, {}, {})
		self._log = logging.getLogger('thread_pool')
		self._exc = ExceptionCollector(self._log)

	def start_daemon(self, desc, fun, *args, **kwargs):
		self._queue.append((desc, fun, args, kwargs))
		with_lock(self._lock, self._queue_update)

	def wait_and_drop(self, timeout=None):
		while True:
			result = with_lock(self._lock, self._monitor_token, timeout)
			if result is not None:
				return result
			t_current = time.time()
			self._notify.wait(timeout)  # wait for thread to finish and adapt timeout for next round
			if timeout is not None:
				timeout -= time.time() - t_current
			with_lock(self._lock, self._queue_update)

	def _collect_exc(self, token, exc_info):
		self._exc.collect(logging.ERROR, 'Exception in thread %r',
			self._token_desc[token], exc_info=exc_info)

	def _monitor_token(self, timeout):
		t_current = time.time()
		# discard stale threads
		for token in list(self._token_time):
			if timeout and (t_current - self._token_time.get(token, 0) > timeout):
				self._unregister_token(token)
		if not self._token_time:  # no active threads
			return True
		# drop all threads if timeout is reached
		if (timeout is not None) and (timeout <= 0):
			self._token_time = {}
			self._token_desc = {}
			return False

	def _queue_update(self):
		while self._queue and ((self._limit is None) or (len(self._token_time) < self._limit)):
			(desc, fun, args, kwargs) = self._queue.pop(0)
			_start_thread(desc=desc, daemon=True, fun=self._run_thread,
				args=(self._register_token(desc), fun, args, kwargs), kwargs={})
		self._notify.clear()

	def _register_token(self, desc):
		self._token += 1
		self._token_time[self._token] = time.time()
		self._token_desc[self._token] = desc
		return self._token

	def _run_thread(self, token, fun, args, kwargs):
		trace_fun = get_trace_fun()
		if trace_fun:
			sys.settrace(trace_fun)
		try:
			fun(*args, **kwargs)
		except Exception:
			with_lock(self._lock, self._collect_exc, token, get_current_exception())
		with_lock(self._lock, self._unregister_token, token)
		with_lock(self._lock, self._notify.set)

	def _unregister_token(self, token):
		self._token_time.pop(token, None)
		self._token_desc.pop(token, None)


def _default_thread_wrapper(fun):
	def _run(*args, **kwargs):
		trace_fun = get_trace_fun()
		if trace_fun:
			sys.settrace(trace_fun)
		try:
			fun(*args, **kwargs)
		except (KeyboardInterrupt, SystemExit):
			raise
		except Exception:
			sys.excepthook(*sys.exc_info())
	return _run


def _start_thread(desc, daemon, fun, args, kwargs):
	# determine thread name (name contains parentage)
	_start_thread.lock.acquire()
	try:
		_start_thread.counter += 1
	finally:
		_start_thread.lock.release()
	thread_name_parent = str(get_thread_name()).replace('Mainthread', 'T')
	thread_name = '%s-%d' % (thread_name_parent, _start_thread.counter)
	# create new thread
	thread = threading.Thread(name=thread_name, target=fun, args=args, kwargs=kwargs)
	thread.desc = desc
	thread.setDaemon(daemon)
	thread.start()
	return thread
_start_thread.counter = 0  # <global-state>
_start_thread.lock = GCLock()  # <global-state>


def _tchain_thread(exc, iterable, result):
	try:
		try:
			for item in iterable:
				result.put(item)
		except Exception:  # first collect exception to avoid race condition
			exc.collect()
	finally:
		result.put(GCQueue)  # Use GCQueue as end-of-generator marker
