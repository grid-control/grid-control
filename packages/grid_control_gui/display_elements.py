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

import time, logging
from grid_control.utils.activity import Activity
from grid_control_gui.ansi import Console
from hpfwk import AbstractError


class GUIElement(object):
	def __init__(self, layout, redraw_event):
		(self._layout, self._layout_pos, self._layout_height) = (layout, 0, 0)
		self._redraw_event = redraw_event

	def draw_finish(self):
		pass

	def get_height(self):
		raise AbstractError

	def redraw(self, force=False):
		raise AbstractError

	def set_layout(self, pos, height):
		(self._layout_pos, self._layout_height) = (pos, height)


class BasicGUIElement(GUIElement):
	def draw_init(self):
		Console.wrap_off()
		self._draw(force=True)
		Console.wrap_on()

	def redraw(self, force=False):
		Console.save_pos()
		Console.move(self._layout_pos)
		Console.wrap_off()
		self._draw(force)
		Console.wrap_on()
		Console.load_pos()

	def _draw(self, force):
		raise AbstractError


class LogElement(GUIElement):
	def __init__(self, layout, redraw_event):
		GUIElement.__init__(self, layout, redraw_event)
		self._log = logging.getLogger('console')

	def draw_init(self):
		self._log.info('\n' * (self._layout_height - 1))
		self.redraw()

	def get_height(self):
		return None

	def redraw(self, force=False):
		Console.save_pos()
		Console.setscrreg(self._layout_pos, self._layout_pos + self._layout_height)
		Console.load_pos()


class ActivityElement(BasicGUIElement):
	def __init__(self, layout, redraw_event):
		BasicGUIElement.__init__(self, layout, redraw_event)
		self._log = logging.getLogger('activity')
		(self._height_last, self._height_last_time, self._height_interval) = (0, 0, 2)

	def draw_finish(self):
		Activity.callbacks.remove(self._redraw_event.set)

	def draw_init(self):
		Activity.callbacks.append(self._redraw_event.set)
		BasicGUIElement.draw_init(self)

	def get_height(self):
		height = max(1, len(list(Activity.root.get_children())))
		if (height > self._height_last) or (time.time() - self._height_last_time > self._height_interval):
			self._height_last_time = time.time()
			self._height_last = height
		return self._height_last

	def _draw(self, force):
		activity_list = list(Activity.root.get_children())
		self._height_last = max(1, len(activity_list))
		for activity in activity_list:
			self._log.info(activity.get_message(truncate=None))
		if self._layout_height - len(activity_list) > 0:
			self._log.info('\n' * (self._layout_height - len(activity_list) - 1))


class ReportElement(BasicGUIElement):
	def __init__(self, layout, redraw_event, report, job_db):
		BasicGUIElement.__init__(self, layout, redraw_event)
		(self._job_db, self._report, self._report_last, self._report_interval) = (job_db, report, 0, 1)
		self._log = logging.getLogger('report')

	def get_height(self):
		return self._report.get_height()

	def _draw(self, force):
		if force or (time.time() - self._report_last > self._report_interval):
			self._report.show_report(self._job_db)
			self._report_last = time.time()
