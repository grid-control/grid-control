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

blocking_equivalent = 60*60*24*7 # instead of blocking, we wait for a week

def start_thread(desc, fun, *args, **kwargs):
	thread = threading.Thread(target = fun, args = args, kwargs = kwargs)
	thread.setDaemon(True)
	thread.start()
	return thread

class TimeoutException(Exception):
	pass

# Event with blocking, interruptible wait and python >= 2.7 return value
class GCEvent(object):
	def __init__(self):
		self._cond = threading.Condition(threading.Lock())
		try:
			self._cond_notify_all = self._cond.notify_all
		except Exception:
			self._cond_notify_all = self._cond.notifyAll
		self._flag = False

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

	def clear(self):
		self._cond.acquire()
		try:
			self._flag = False
		finally:
			self._cond.release()
		return False

	def wait(self, timeout, description = 'event'):
		if timeout is None:
			timeout = blocking_equivalent
		self._cond.acquire()
		try:
			try:
				if not self._flag:
					self._cond.wait(timeout)
				return self._flag # return current flag state after wait / wakeup
			finally:
				self._cond.release()
		except KeyboardInterrupt:
			raise KeyboardInterrupt('Interrupted while waiting for %s' % description)

# Lock with optional acquire timeout
class GCLock(object):
	def __init__(self):
		self._lock = threading.Lock()

	def acquire(self, timeout = None):
		try:
			if timeout == 0: # Non-blocking
				return self._lock.acquire(False)
			if timeout is None: # Blocking
				timeout = blocking_equivalent
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

# thread-safe communication channel with put / get
class GCQueue(object):
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

	def wait_get(self, timeout):
		return self._notify.wait(timeout)

	def get(self, timeout, default = IndexError): # IndexError is a magic value to raise an exception
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

# Class to manage a collection of threads
class GCThreadPool(object):
	def __init__(self):
		self._lock = GCLock()
		self._notify = GCEvent()
		self._token = 0
		self._token_time = {}
		self._token_desc = {}
		self._log = logging.getLogger('thread_pool')

	def wait_and_drop(self, timeout = None):
		while True:
			self._lock.acquire()
			try:
				t_current = time.time()
				# discard stale threads
				for token in list(self._token_time):
					if timeout and (t_current - self._token_time.get(token, 0) > timeout):
						self._token_time.pop(token, None)
						self._token_desc.pop(token, None)
				if not self._token_time: # no active threads
					return True
			finally:
				self._lock.release()
			# wait for thread to finish and adapt timeout for next round
			if (timeout is not None) and (timeout <= 0):
				return False
			self._notify.wait(timeout)
			if timeout is not None:
				timeout -= time.time() - t_current

	def start_thread(self, desc, fun, *args, **kwargs):
		self._lock.acquire()
		try:
			self._token += 1
			self._token_time[self._token] = time.time()
			self._token_desc[self._token] = desc
		finally:
			self._lock.release()
		thread = threading.Thread(target = self._run_thread, args = (self._token, fun, args, kwargs))
		thread.setDaemon(True)
		thread.start()

	def _run_thread(self, token, fun, args, kwargs):
		try:
			fun(*args, **kwargs)
		except Exception:
			self._lock.acquire()
			try:
				self._log.exception('Exception in thread %r', self._token_desc[token])
			finally:
				self._lock.release()
		self._lock.acquire()
		try:
			self._token_time.pop(token, None)
			self._token_desc.pop(token, None)
		finally:
			self._lock.release()
		self._notify.set()


# Function to protect against hanging system calls
def hang_protection(fun, timeout = 5):
	result = {}
	def hang_protection_wrapper():
		try:
			result[None] = fun()
		except Exception:
			result[None] = None
	t = threading.Thread(target = hang_protection_wrapper)
	t.start()
	t.join(timeout)
	if None not in result:
		raise TimeoutException
	return result[None]
