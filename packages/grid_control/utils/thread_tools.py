#-#  Copyright 2015-2016 Karlsruhe Institute of Technology
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

import time, logging, threading

blocking_equivalent = 60*60*24*7 # instead of blocking, we wait for a week

def start_thread(desc, fun, *args, **kargs):
	thread = threading.Thread(target = fun, args = args, kwargs = kargs)
	thread.setDaemon(True)
	thread.start()
	return thread

class TimeoutException(Exception):
	pass

# Event with interruptible, blocking wait (implemented with polling)
class GCEvent(object):
	def __init__(self):
		self._event = threading.Event()

	def is_set(self):
		try:
			return self._event.is_set() # Python > 2.6
		except Exception:
			return self._event.isSet()

	def set(self):
		return self._event.set()

	def clear(self):
		return self._event.clear()

	def wait(self, timeout, description = 'event'):
		if timeout is None:
			timeout = blocking_equivalent
		try:
			return self._event.wait(timeout)
		except KeyboardInterrupt:
			raise KeyboardInterrupt('Interrupted while waiting for %s' % description)

# Lock with optional acquire timeout
class GCLock(object):
	def __init__(self, interval = 0.5):
		self._interval = interval
		self._lock = threading.Lock()

	def acquire(self, timeout = None):
		try:
			if timeout == 0: # Non-blocking
				return self._lock.acquire(False)
			if timeout is None: # Blocking
				timeout = blocking_equivalent
			start = time.time()
			while time.time() - start < timeout: # Polling lock until timeout
				lockstate = self._lock.acquire(False)
				if lockstate:
					return lockstate
				time.sleep(self._interval)
			raise TimeoutException
		except KeyboardInterrupt:
			raise KeyboardInterrupt('Interrupted while waiting to acquire lock')

	def release(self):
		self._lock.release()

# thread-safe communication channel with put / get
class GCQueue(object):
	def __init__(self, BufferObject):
		self._lock = GCLock()
		self._notify = GCEvent()
		self._finished = GCEvent()
		self._buffer_type = BufferObject
		self._buffer = self._buffer_type()

	def __repr__(self):
		return '%s(%r)' % (self.__class__.__name__, self._buffer)

	def finish(self):
		self._finished.set()
		self._notify.set()

	def wait(self, timeout):
		return self._notify.wait(timeout)

	def get(self, timeout):
		self._notify.wait(timeout)
		self._lock.acquire()
		result = self._buffer
		self._buffer = self._buffer_type()
		if not self._finished.is_set():
			self._notify.clear()
		self._lock.release()
		return result

	def put(self, value):
		self._lock.acquire()
		self._buffer += value
		self._notify.set()
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
			t_current = time.time()
			# discard stale threads
			for token in list(self._token_time):
				if timeout and (t_current - self._token_time.get(token, 0) > timeout):
					self._token_time.pop(token, None)
					self._token_desc.pop(token, None)
			if not self._token_time: # no active threads
				self._lock.release()
				return True
			self._lock.release()
			# wait for thread to finish and adapt timeout for next round
			if (timeout is not None) and (timeout <= 0):
				return False
			self._notify.wait(timeout)
			if timeout is not None:
				timeout -= time.time() - t_current

	def start_thread(self, desc, fun, *args, **kwargs):
		self._lock.acquire()
		self._token += 1
		self._token_time[self._token] = time.time()
		self._token_desc[self._token] = desc
		self._lock.release()
		thread = threading.Thread(target = self._run_thread, args = (self._token, fun, args, kwargs))
		thread.setDaemon(True)
		thread.start()

	def _run_thread(self, token, fun, args, kwargs):
		try:
			fun(*args, **kwargs)
		except Exception:
			self._lock.acquire()
			self._log.exception('Exception in thread %r', self._token_desc[token])
			self._lock.release()
		self._lock.acquire()
		self._token_time.pop(token, None)
		self._token_desc.pop(token, None)
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
