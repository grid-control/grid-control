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

import time
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils import is_dumb_terminal
from grid_control.utils.activity import Activity
from hpfwk import AbstractError, clear_current_exception


class ActivityMonitor(ConfigurablePlugin):
	def __init__(self, config, stream, register_callback=False):
		ConfigurablePlugin.__init__(self, config)
		self._msg_len_max = config.get_int('activity max length', 75, on_change=None)
		(self._stream, self._register_cb) = (stream, register_callback)

	def disable(self):
		if self._register_cb:
			Activity.callbacks.remove(self.write)

	def enable(self):
		if self._register_cb:
			Activity.callbacks.append(self.write)

	def flush(self):
		return self._stream.flush()

	def write(self, value=''):
		raise AbstractError


class DefaultActivityMonitor(ActivityMonitor):
	alias_list = ['default_stream']

	def __new__(cls, config, stream, register_callback=False):
		if is_dumb_terminal(stream):
			return ActivityMonitor.create_instance('TimedActivityMonitor', config, stream, register_callback)
		try:  # try to pick up multi line activity stream
			return ActivityMonitor.create_instance('MultiActivityMonitor', config, stream, register_callback)
		except Exception:  # fall back to standard terminal activity stream
			clear_current_exception()
			return ActivityMonitor.create_instance('SingleActivityMonitor', config, stream, register_callback)


class NullOutputStream(ActivityMonitor):
	alias_list = ['null']

	def __init__(self, config, stream=None, register_callback=False):
		ActivityMonitor.__init__(self, config, stream, register_callback)

	def flush(self):
		pass

	def write(self, value=''):
		pass


class SingleActivityMonitor(ActivityMonitor):
	alias_list = ['single_stream']

	def __init__(self, config, stream, register_callback=False):
		ActivityMonitor.__init__(self, config, stream, register_callback)
		self._old_msg = None

	def write(self, value=''):
		activity_msg = None
		if Activity.root:
			for activity in Activity.root.get_children():
				activity_msg = activity.get_msg(truncate=self._msg_len_max)
		if self._old_msg is not None:
			self._stream.write('\r%s\r' % (' ' * len(self._old_msg)))
		self._old_msg = activity_msg
		result = self._stream.write(value)
		if (activity_msg is not None) and (value.endswith('\n') or not value):
			self._stream.write(activity_msg + '\r')
			self._stream.flush()
		return result


class TimedActivityMonitor(ActivityMonitor):
	alias_list = ['timed_stream']

	def __init__(self, config, stream, register_callback=False):
		ActivityMonitor.__init__(self, config, stream, register_callback)
		(self._time, self._last) = (0, '')
		self._interval = config.get_float('activity interval', 5., on_change=None)

	def write(self, value=''):
		if time.time() - self._time > self._interval:
			msg = ''
			for activity in Activity.root.get_children():
				msg += activity.get_msg(truncate=self._msg_len_max) + '\n'
			if msg != self._last:
				self._stream.write(msg)
				self._last = msg
			self._time = time.time()
		return self._stream.write(value)
