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

from grid_control.utils.activity import Activity
from hpfwk import AbstractError, Plugin


class ActivityStream(Plugin):
	def __init__(self, stream, register_callback=False):
		Plugin.__init__(self)
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


class NullOutputStream(ActivityStream):
	def __init__(self, stream=None, register_callback=False):
		ActivityStream.__init__(self, stream, register_callback)

	def flush(self):
		pass

	def write(self, value=''):
		pass


class SingleActivityStream(ActivityStream):
	alias_list = ['single']

	def __init__(self, stream, register_callback=False):
		ActivityStream.__init__(self, stream, register_callback)
		self._old_message = None

	def write(self, value=''):
		activity_message = None
		if Activity.root:
			for activity in Activity.root.get_children():
				activity_message = activity.get_message(truncate=75)
		if self._old_message is not None:
			self._stream.write('\r%s\r' % (' ' * len(self._old_message)))
		self._old_message = activity_message
		result = self._stream.write(value)
		if (activity_message is not None) and (value.endswith('\n') or not value):
			self._stream.write(activity_message + '\r')
			self._stream.flush()
		return result


class DefaultActivityStream(ActivityStream):
	alias_list = ['default']

	def __new__(cls, stream, register_callback=False):
		if (not hasattr(stream, 'isatty')) or not stream.isatty():
			return stream
		try:
			return ActivityStream.create_instance('simple', stream, register_callback)
		except Exception:
			return ActivityStream.create_instance('single', stream, register_callback)
