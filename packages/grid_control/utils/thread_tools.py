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

import time, threading

blocking_equivalent = 1e10 # instead of blocking, we wait for this long

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
