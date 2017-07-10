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
from grid_control.gc_plugin import NamedPlugin
from grid_control_gui.ansi import ANSI
from hpfwk import AbstractError
from python_compat import imap, izip, lmap


class GUIElement(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['gui']
	config_tag_name = 'gui_element'

	def __init__(self, config, name, workflow, redraw_event, stream):
		NamedPlugin.__init__(self, config, name)
		(self._dirty, self._redraw_event, self._on_height_change) = (False, redraw_event, None)
		(self._layout_pos, self._layout_height, self._layout_width) = (0, 0, 0)
		self._dirty_interval = config.get_float('gui refresh interval', 0.2, on_change=None)
		self._height_interval = config.get_float('gui height interval', 10., on_change=None)
		(self._dirty_next, self._height_next) = (0, 0)
		self._stream = stream

	def draw_finish(self):
		pass

	def draw_startup(self):
		self._draw()

	def get_height(self):
		new_height = self._get_height()
		if (new_height and (new_height > self._layout_height)) or (time.time() > self._height_next):
			return new_height
		return self._layout_height

	def make_dirty(self):
		self._dirty = True
		self._redraw_event.set()

	def redraw(self):
		height = self.get_height()
		if (height is not None) and (height != self._layout_height) and self._on_height_change:
			self._on_height_change()
		if self._is_dirty():
			self._stream.write(ANSI.move(self._layout_pos))
			self._draw()
			self._dirty_next = time.time() + self._dirty_interval

	def set_layout(self, pos, height, width, on_height_change=None):  # return True when changed
		self._on_height_change = self._on_height_change or on_height_change
		(old_pos, old_height, old_width) = (self._layout_pos, self._layout_height, self._layout_width)
		(self._layout_pos, self._layout_height, self._layout_width) = (pos, height, width)
		self._dirty = self._dirty or ((old_pos, old_height, old_width) != (pos, height, width))

	def _draw(self):
		raise AbstractError

	def _get_height(self):
		raise AbstractError

	def _is_dirty(self):
		return self._dirty or (time.time() > self._dirty_next)


class MultiGUIElement(GUIElement):
	alias_list = ['multi']

	def __init__(self, config, name, element_list, workflow, redraw_event, stream):
		GUIElement.__init__(self, config, name, workflow, redraw_event, stream)
		(self._element_list, self._subelements_height) = (element_list, 0)

	def draw_finish(self):
		for element in self._element_list:
			element.draw_finish()

	def draw_startup(self):  # initial draw is based on console height - 1
		for (idx, element) in enumerate(self._element_list):
			element.draw_startup()
			if idx != len(self._element_list) - 1:
				self._stream.write('\n')

	def make_dirty(self):
		for element in self._element_list:
			element.make_dirty()

	def redraw(self):
		for element in self._element_list:
			element.redraw()

	def set_layout(self, pos, height, width, on_height_change=None):
		GUIElement.set_layout(self, pos, height, width, on_height_change)
		self._set_subelement_layout()

	def _get_height(self):
		return self._subelements_height

	def _set_subelement_layout(self):  # recalculate layout of subelements
		height_list = lmap(lambda element: element.get_height(), self._element_list)
		height_total = sum(imap(lambda element_height: element_height or 0, height_list))
		pos = self._layout_pos
		for (element, element_height) in izip(self._element_list, height_list):
			if element_height is None:
				element_height = max(0, self._layout_height - height_total)
			element.set_layout(pos, min(self._layout_height - pos, element_height),
				self._layout_width, self._set_subelement_layout)  # call this function if height changes
			pos += element_height
		self._subelements_height = pos - self._layout_pos
