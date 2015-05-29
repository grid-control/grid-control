#-#  Copyright 2015 Karlsruhe Institute of Technology
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

class TimeoutException(Exception):
	pass

# Event with interruptible, blocking wait (implemented with polling)
class GCEvent:
	def __init__(self):
		self._event = threading.Event()

	def is_set(self):
		try:
			return self._event.is_set() # Python > 2.6
		except:
			return self._event.isSet()

	def set(self):
		return self._event.set()

	def clear(self):
		return self._event.clear()

	def wait(self, timeout, description = 'event'):
		if timeout == None:
			timeout = blocking_equivalent
		try:
			return self._event.wait(timeout)
		except KeyboardInterrupt:
			raise KeyboardInterrupt('Interrupted while waiting for %s' % description)

# Lock with optional acquire timeout
class GCLock:
	def __init__(self, interval = 0.5):
		self._interval = interval
		self._lock = threading.Lock()

	def acquire(self, timeout = None):
		try:
			if timeout == 0: # Non-blocking
				return self._lock.acquire(False)
			if timeout == None: # Blocking
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

# Function to protect against hanging system calls
def hang_protection(fun, timeout = 5):
	result = {}
	def hang_protection_wrapper():
		try:
			result[None] = fun()
		except:
			result[None] = None
	t = threading.Thread(target = hang_protection_wrapper)
	t.start()
	t.join(timeout)
	if None not in result:
		raise TimeoutException
	return result[None]
