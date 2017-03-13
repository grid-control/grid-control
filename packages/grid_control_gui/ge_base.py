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

import sys, logging
from grid_control.gc_plugin import NamedPlugin
from grid_control_gui.ansi import ANSI
from grid_control_gui.stream_gui import GUIStream
from hpfwk import AbstractError
from python_compat import StringBuffer, imap, izip, lmap, irange, partial


class GUIElement(NamedPlugin):
	config_section_list = ['gui']
	config_tag_name = 'gui_element'

	def __init__(self, config, name, workflow, redraw_event):
		NamedPlugin.__init__(self, config, name)
		(self._dirty, self._redraw_event, self._on_height_change) = (False, redraw_event, None)
		(self._layout_pos, self._layout_height, self._layout_width) = (0, 0, 0)

	def draw_finish(self):
		pass

	def draw_init(self):
		self._draw()

	def get_height(self):
		raise AbstractError

	def make_dirty(self):
		self._dirty = True
		self._redraw_event.set()

	def redraw(self):
		height = self.get_height()
		if (height is not None) and (height != self._layout_height) and self._on_height_change:
			self._on_height_change()
		if self._is_dirty():
			sys.stdout.write(ANSI.move(self._layout_pos))
			self._draw()

	def set_layout(self, pos, height, width, on_height_change=None):  # return True when changed
		self._on_height_change = self._on_height_change or on_height_change
		(old_pos, old_height, old_width) = (self._layout_pos, self._layout_height, self._layout_width)
		if height is None:
			height = self._layout_height
		(self._layout_pos, self._layout_height, self._layout_width) = (pos, height, width)
		self._dirty = self._dirty or ((old_pos, old_height, old_width) != (pos, height, width))

	def _draw(self):
		raise AbstractError

	def _is_dirty(self):
		return self._dirty


class BufferGUIElement(GUIElement):
	def __init__(self, config, name, workflow, redraw_event, truncate_back=True):
		GUIElement.__init__(self, config, name, workflow, redraw_event)
		(self._buffer, self._truncate_back) = (StringBuffer(), truncate_back)

	def _draw(self):
		self._update_buffer()
		self._buffer.flush()
		self._draw_buffer(self._buffer.getvalue())
		self._dirty = False

	def _draw_buffer(self, msg, prefix='', postfix='\r'):  # draw message while staying in layout area
		if self._layout_height > 0:
			output_list = list(self._process_lines(msg.splitlines()))
			if self._truncate_back:
				output_list = output_list[:self._layout_height]
			else:
				output_list = output_list[-self._layout_height:]
			output_list.extend([ANSI.erase_line] * (self._layout_height - len(output_list)))
			sys.stdout.write(prefix + str.join('\n', output_list) + postfix)

	def _process_lines(self, output_list):
		for line in output_list:
			line = line.rstrip()[:self._layout_width + len(line) - len(ANSI.strip_fmt(line))]
			yield line.rstrip(ANSI.esc) + ANSI.reset + ANSI.erase_line

	def _update_buffer(self):
		pass


class MultiGUIElement(GUIElement):
	def __init__(self, config, name, element_list, workflow, redraw_event):
		GUIElement.__init__(self, config, name, workflow, redraw_event)
		(self._element_list, self._subelements_height) = (element_list, 0)

	def draw_finish(self):
		for element in self._element_list:
			element.draw_finish()

	def draw_init(self):  # initial draw is based on console height - 1
		for (idx, element) in enumerate(self._element_list):
			element.draw_init()
			if idx != len(self._element_list) - 1:
				sys.stdout.write('\n')

	def get_height(self):
		return self._subelements_height

	def make_dirty(self):
		for element in self._element_list:
			element.make_dirty()

	def redraw(self):
		for element in self._element_list:
			element.redraw()

	def set_layout(self, pos, height, width, on_height_change=None):
		GUIElement.set_layout(self, pos, height, width, on_height_change)
		pos = self._layout_pos
		height_list = lmap(lambda element: element.get_height(), self._element_list)
		height_total = sum(imap(lambda element_height: element_height or 0, height_list))
		for (element, element_height) in izip(self._element_list, height_list):
			if element_height is None:
				element_height = self._layout_height - height_total
			element.set_layout(pos, min(height - pos, element_height), width,
				partial(self.set_layout, self._layout_pos, self._layout_height, self._layout_width))
			pos += element_height
		self._subelements_height = pos - self._layout_pos


class BasicGUIElement(BufferGUIElement):
	def __init__(self, config, name, workflow, redraw_event, log_name):
		BufferGUIElement.__init__(self, config, name, workflow, redraw_event)
		(self._log, self._old_log_state) = (logging.getLogger(log_name), None)

	def draw_finish(self):
		BufferGUIElement.draw_finish(self)
		(self._log.handlers, self._log.propagate) = self._old_log_state

	def draw_init(self):
		self._old_log_state = (self._log.handlers, self._log.propagate)
		self._log.handlers = [logging.StreamHandler(GUIStream(self._buffer))]
		self._log.propagate = False
		BufferGUIElement.draw_init(self)
