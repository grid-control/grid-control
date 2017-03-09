# | Copyright 2017 Karlsruhe Institute of Technology
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

import sys, threading


def clear_current_exception():
	# automatic cleanup in >= py-3.0
	impl_detail(sys, 'exc_clear', args=(), default=None)


def get_thread_name(thread=None):
	if thread is None:
		thread = _get_current_thread()
	try:  # new lowercase name in >= py-2.6
		return thread.name
	except Exception:
		return thread.getName()


def get_thread_state(thread):
	try:  # new lowercase name in >= py-2.6
		return thread.is_alive()
	except Exception:
		return thread.isAlive()


def impl_detail(module, name, args, default, fun=lambda x: x):
	# access some python implementation detail with default
	try:
		return fun(getattr(module, name)(*args))
	except Exception:
		return default


def _get_current_thread():
	try:  # new lowercase name in >= py-2.6
		return threading.current_thread()
	except Exception:
		return threading.currentThread()
