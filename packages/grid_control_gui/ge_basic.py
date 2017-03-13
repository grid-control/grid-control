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

import sys, time
from grid_control.logging_setup import GCStreamHandler
from grid_control.report import Report
from grid_control.stream_base import ActivityStream
from grid_control.utils.activity import Activity
from grid_control_gui.ansi import ANSI
from grid_control_gui.ge_base import BasicGUIElement, BufferGUIElement
from grid_control_gui.stream_gui import GUIStream
from python_compat import lmap, sorted


class AfterImageGUIElement(BufferGUIElement):
	def __init__(self, config, name, workflow, redraw_event, truncate_back=True):
		BufferGUIElement.__init__(self, config, name, workflow, redraw_event, truncate_back)
		self._old_lines = {}

	def _format_old_line(self, cur_time, line_idx, old_line_raw, old_line_age):
		line_brightness = max(0, 0.8 - (cur_time - old_line_age) / 4.)
		if (line_brightness == 0) or (line_idx > self._layout_height):
			self._old_lines.pop(line_idx)
		return ANSI.color_grayscale(line_brightness) + old_line_raw + ANSI.reset + ANSI.erase_line

	def _process_lines(self, output_list):
		cur_time = time.time()
		old_lines = dict(self._old_lines)
		for line_idx, cur_line in enumerate(BufferGUIElement._process_lines(self, output_list)):
			(old_line_raw, old_line_age) = old_lines.pop(line_idx, (None, None))
			cur_line_raw = ANSI.strip_fmt(cur_line)
			if old_line_raw and not cur_line_raw:
				yield self._format_old_line(cur_time, line_idx, old_line_raw, old_line_age)
			else:
				yield cur_line
				self._old_lines[line_idx] = (cur_line_raw, cur_time)
		for old_line_idx in sorted(old_lines):
			yield self._format_old_line(cur_time, old_line_idx, *old_lines.pop(old_line_idx))


class ReportGUIElement(BasicGUIElement):
	alias_list = ['report']

	def __init__(self, config, name, workflow, redraw_event):
		BasicGUIElement.__init__(self, config, name, workflow, redraw_event, 'console.report')
		(self._job_manager, self._report_last) = (workflow.job_manager, 0)
		report_config_str = config.get('report options', '', on_change=None)
		self._report_interval = config.get_float('report interval', 1., on_change=None)
		self._report = config.get_composited_plugin('report', 'HeaderReport BasicReport ColorBarReport',
			'MultiReport', cls=Report, on_change=None, pargs=(self._job_manager.job_db, workflow.task),
			pkwargs={'config_str': report_config_str})

	def draw_finish(self):
		BasicGUIElement.draw_finish(self)
		self._job_manager.remove_event_handler(self.make_dirty)

	def draw_init(self):
		self._job_manager.add_event_handler(self.make_dirty)
		BasicGUIElement.draw_init(self)

	def get_height(self):
		return self._report.get_height()

	def _is_dirty(self):
		return self._dirty or (time.time() - self._report_last > self._report_interval)

	def _update_buffer(self):
		self._buffer.truncate(0)
		self._report.show_report(self._job_manager.job_db)
		self._report_last = time.time()


class ActivityGUIElement(AfterImageGUIElement):
	alias_list = ['activity']

	def __init__(self, config, name, workflow, redraw_event):
		AfterImageGUIElement.__init__(self, config, name, workflow, redraw_event)
		self._stream = ActivityStream.create_instance('simple', self._buffer)
		self._height_max = config.get_int('activity height max', 5, on_change=None)
		self._height_min = config.get_int('activity height min', 1, on_change=None)
		self._height_interval = config.get_int('activity height interval', 5, on_change=None)
		(self._height_last, self._height_last_time) = (0, 0)

	def draw_finish(self):
		AfterImageGUIElement.draw_finish(self)
		Activity.callbacks.remove(self.make_dirty)

	def draw_init(self):
		Activity.callbacks.append(self.make_dirty)
		AfterImageGUIElement.draw_init(self)

	def get_height(self):
		if (self._height_last > self._layout_height) or (time.time() - self._height_last_time > self._height_interval):
			self._height_last_time = time.time()
			return self._height_last
		return self._layout_height

	def _update_buffer(self):
		self._buffer.truncate(0)
		self._stream.write()
		message = ANSI.strip_cmd(self._buffer.getvalue()).strip()
		self._buffer.truncate(0)
		self._height_last = max(self._layout_height, 1 + message.count('\n'))
		self._buffer.write(message)


class SpanGUIElement(BufferGUIElement):
	alias_list = ['span']

	def get_height(self):
		return None


class UserLogGUIElement(BufferGUIElement):
	alias_list = ['log']

	def __init__(self, config, name, workflow, redraw_event):
		BufferGUIElement.__init__(self, config, name, workflow, redraw_event, truncate_back=False)
		self._log_max = config.get_int('log length', 200, on_change=None)
		self._log_dump = config.get_bool('log dump', True, on_change=None)
		self._log_wrap = config.get_bool('log wrap', True, on_change=None)

	def draw_finish(self):
		BufferGUIElement.draw_finish(self)
		GCStreamHandler.pop_std_stream()
		if self._log_dump:
			sys.stdout.write('\n' + ANSI.erase_down + '-' * 20 + ' LOG HISTORY ' + '-' * 20 + '-\n')
			sys.stdout.write(self._buffer.getvalue())

	def draw_init(self):
		gui_stream = GUIStream(self._buffer, self.make_dirty)
		GCStreamHandler.push_std_stream(gui_stream, gui_stream)
		BufferGUIElement.draw_init(self)

	def get_height(self):
		return None

	def _process_lines(self, output_list):
		if self._log_wrap:
			output_list = self._wrap_lines(output_list)
		return BufferGUIElement._process_lines(self, output_list)

	def _update_buffer(self):
		msg_line_list = lmap(lambda line: line + '\n', self._buffer.getvalue().splitlines())
		self._buffer.truncate(0)
		self._buffer.write(str.join('', msg_line_list[-self._log_max:]))

	def _wrap_lines(self, output_list):
		for line in output_list:
			(tmp, tmp_len) = ('', 0)
			for token in line.split(' '):
				token_len = len(ANSI.strip_fmt(token))
				if tmp_len + token_len + 1 > self._layout_width:
					yield tmp
					(tmp, tmp_len) = ('', 0)
				if tmp:
					tmp += ' '
					tmp_len += 1
				tmp += token
				tmp_len += token_len
			if tmp:
				yield tmp
