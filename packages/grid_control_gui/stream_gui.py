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

import re, threading
from grid_control.stream_base import ActivityStream
from grid_control.utils.activity import Activity
from grid_control.utils.thread_tools import GCLock, with_lock
from grid_control_gui.ansi import ANSI, Console, install_console_reset


class GUIStream(object):
	def __init__(self, stream, callback=None):
		(self._stream, self._callback) = (stream, callback)
		# This is a list of (regular expression, GUI attributes).  The
		# attributes are applied to matches of the regular expression in
		# the output written into this stream.  Lookahead expressions
		# should not overlap with other regular expressions.
		rcmp = re.compile
		self._regex_attr_list = [
			(rcmp(r'DONE(?!:)'), ANSI.color_blue + ANSI.bold),
			(rcmp(r'FAILED(?!:)'), ANSI.color_red + ANSI.bold),
			(rcmp(r'SUCCESS(?!:)'), ANSI.color_green + ANSI.bold),
			(rcmp(r'(?<=DONE:)\s+[1-9]\d*'), ANSI.color_blue + ANSI.bold),
			(rcmp(r'(?<=Failing jobs:)\s+[1-9]\d*'), ANSI.color_red + ANSI.bold),
			(rcmp(r'(?<=FAILED:)\s+[1-9]\d*'), ANSI.color_red + ANSI.bold),
			(rcmp(r'(?<=Successful jobs:)\s+[1-9]\d*'), ANSI.color_green + ANSI.bold),
			(rcmp(r'(?<=SUCCESS:)\s+[1-9]\d*'), ANSI.color_green + ANSI.bold),
		]

	def __getattr__(self, name):
		return self._stream.__getattribute__(name)

	def write(self, value):
		value = value.replace('\n', ANSI.erase_line + '\n')
		for (regex, attr) in self._regex_attr_list:
			value = regex.sub(lambda match: ANSI.reset + attr + match.group(0) + ANSI.reset, value)
		self._stream.write(value)
		if self._callback:
			self._callback()


class MultiActivityStream(ActivityStream):
	alias_list = ['multi']
	global_lock = GCLock(threading.RLock())

	def __init__(self, stream, register_callback=False):
		ActivityStream.__init__(self, stream, register_callback=False)
		install_console_reset()

	def write(self, value=''):
		with_lock(MultiActivityStream.global_lock, self._write, value)

	def _format_activity(self, width, level, activity, activity_list):
		return '  ' * level + activity.get_message(truncate=width - 5 - 2 * level)

	def _write(self, value):
		max_x = Console.getmaxyx()[1]
		value = value.replace('\n', ANSI.erase_line + '\n' + ANSI.erase_line)
		self._stream.write(value + '\n' + ANSI.wrap_off)
		activity_list = list(Activity.root.get_children())
		for activity in activity_list:
			msg = self._format_activity(max_x, activity.depth - 2, activity, activity_list)
			self._stream.write(ANSI.erase_line + msg + '\n')
		self._stream.write(ANSI.move_up(len(activity_list) + 1) + ANSI.wrap_on)
		self._stream.flush()


class SimpleActivityStream(MultiActivityStream):
	alias_list = ['simple', 'default']

	def _format_activity(self, width, level, activity, activity_list):
		msg = activity.get_message(truncate=width - 5 - 2 * level)
		return '  ' * level + ANSI.color_grayscale(1 - level / 5.) + msg + ANSI.reset
