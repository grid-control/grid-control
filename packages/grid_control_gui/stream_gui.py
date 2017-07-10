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

import threading
from grid_control.stream_base import ActivityMonitor
from grid_control.utils.activity import Activity
from grid_control.utils.thread_tools import GCLock, with_lock
from grid_control_gui.ansi import ANSI, Console, install_console_reset
from python_compat import lfilter


class MultiActivityMonitor(ActivityMonitor):
	alias_list = ['multi_stream']
	global_lock = GCLock(threading.RLock())

	def __init__(self, config, stream, register_callback=False):
		ActivityMonitor.__init__(self, config, stream, register_callback)
		self._fold = config.get_float('activity fold fraction', 0.5, on_change=None)
		install_console_reset()

	def write(self, value=''):
		with_lock(MultiActivityMonitor.global_lock, self._write, value)

	def _format_activity(self, width, level, activity, activity_list):
		msg = activity.get_msg(truncate=width - 5 - 2 * level)
		return '  ' * level + ANSI.color_grayscale(1 - level / 5.) + msg + ANSI.reset

	def _write(self, value):
		max_yx = Console.getmaxyx()
		max_x = min(max_yx[1], self._msg_len_max)
		value = value.replace('\n', ANSI.erase_line + '\n' + ANSI.erase_line)
		self._stream.write(value + '\n' + ANSI.wrap_off)
		activity_list = list(Activity.root.get_children())
		max_depth = int(max_yx[0] * self._fold)
		while len(activity_list) > int(max_yx[0] * self._fold):
			activity_list = lfilter(lambda activity: activity.depth - 1 < max_depth, activity_list)
			max_depth -= 1
		for activity in activity_list:
			msg = self._format_activity(max_x, activity.depth - 1, activity, activity_list)
			self._stream.write(ANSI.erase_line + msg + '\n')
		self._stream.write(ANSI.erase_down + ANSI.move_up(len(activity_list) + 1) + ANSI.wrap_on)
		self._stream.flush()
