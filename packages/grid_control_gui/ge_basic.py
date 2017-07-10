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
from grid_control.logging_setup import GCStreamHandler
from grid_control.report import Report
from grid_control.stream_base import ActivityMonitor
from grid_control.utils.activity import Activity
from grid_control.utils.file_tools import erase_content
from grid_control_gui.ansi import ANSI
from grid_control_gui.ge_base import GUIElement
from python_compat import StringBuffer, irange, lmap, sorted


class BufferGUIElement(GUIElement):
	def __init__(self, config, name, workflow, redraw_event, stream, truncate_back=True):
		GUIElement.__init__(self, config, name, workflow, redraw_event, stream)
		(self._buffer, self._truncate_back) = (StringBuffer(), truncate_back)

	def flush(self):  # implementing buffer interface
		self._buffer.flush()

	def write(self, value):  # implementing buffer interface
		self._buffer.write(value)
		self.make_dirty()

	def _draw(self):
		self._update_buffer()
		self._buffer.flush()
		self._draw_buffer(self._buffer.getvalue())
		self._dirty = False

	def _draw_buffer(self, msg, prefix='', postfix='\r'):  # draw message while staying in layout area
		if self._layout_height > 0:
			output_iter = self._trim_height(self._trim_width(self._pre_processing(msg.splitlines())))
			self._stream.write(self._post_processing(prefix + str.join('\n', output_iter) + postfix))

	def _post_processing(self, output_iter):
		return output_iter

	def _pre_processing(self, output_iter):
		return output_iter

	def _trim_height(self, output_iter):
		output_list = list(output_iter)
		if self._truncate_back:
			output_list = output_list[:self._layout_height]
		else:
			output_list = output_list[-self._layout_height:]
		output_list.extend([ANSI.erase_line] * (self._layout_height - len(output_list)))
		return output_list

	def _trim_width(self, output_iter):
		for line in output_iter:
			line = line.rstrip()[:self._layout_width + len(line) - len(ANSI.strip_fmt(line))]
			yield line.rstrip(ANSI.esc) + ANSI.reset + ANSI.erase_line

	def _update_buffer(self):
		pass


class AfterImageGUIElement(BufferGUIElement):
	def __init__(self, config, name, workflow, redraw_event, stream, truncate_back=True):
		BufferGUIElement.__init__(self, config, name, workflow, redraw_event, stream, truncate_back)
		self._old_lines = {}

	def _format_old_line(self, cur_time, line_idx, old_line_raw, old_line_age):
		line_brightness = max(0, 0.8 - (cur_time - old_line_age) / 4.)
		if (line_brightness == 0) or (line_idx > self._layout_height):
			self._old_lines.pop(line_idx)
		return ANSI.color_grayscale(line_brightness) + old_line_raw + ANSI.reset + ANSI.erase_line

	def _pre_processing(self, output_iter):
		cur_time = time.time()
		old_lines = dict(self._old_lines)
		for line_idx, cur_line in enumerate(output_iter):
			(old_line_raw, old_line_age) = old_lines.pop(line_idx, (None, None))
			cur_line_raw = ANSI.strip_fmt(cur_line)
			if old_line_raw and not cur_line_raw:
				yield self._format_old_line(cur_time, line_idx, old_line_raw, old_line_age)
			else:
				yield cur_line
				self._old_lines[line_idx] = (cur_line_raw, cur_time)
		for old_line_idx in sorted(old_lines):
			yield self._format_old_line(cur_time, old_line_idx, *old_lines.pop(old_line_idx))


class BasicGUIElement(BufferGUIElement):
	def __init__(self, config, name, workflow, redraw_event, stream, log_name):
		BufferGUIElement.__init__(self, config, name, workflow, redraw_event, stream)
		(self._log, self._old_log_state) = (logging.getLogger(log_name), None)

	def draw_finish(self):
		BufferGUIElement.draw_finish(self)
		(self._log.handlers, self._log.propagate) = self._old_log_state

	def draw_startup(self):
		self._old_log_state = (self._log.handlers, self._log.propagate)
		self._log.handlers = [logging.StreamHandler(self)]
		self._log.propagate = False
		BufferGUIElement.draw_startup(self)


class SpanGUIElement(BufferGUIElement):
	alias_list = ['span']

	def _get_height(self):
		return None


class UserLogGUIElement(BufferGUIElement):
	alias_list = ['log']

	def __init__(self, config, name, workflow, redraw_event, stream):
		BufferGUIElement.__init__(self, config, name, workflow, redraw_event, stream, truncate_back=False)
		self._log_max = config.get_int('log length', 200, on_change=None)
		self._log_dump = config.get_bool('log dump', True, on_change=None)
		self._log_wrap = config.get_bool('log wrap', True, on_change=None)
		self._text_attr = {}
		self._add_keyword(ANSI.bold + ANSI.color_cyan, 'DONE', ANSI.reset)
		self._add_keyword(ANSI.bold + ANSI.color_red, 'FAILED', ANSI.reset)
		self._add_keyword(ANSI.bold + ANSI.color_blue, 'RUNNING', ANSI.reset)
		self._add_keyword(ANSI.bold + ANSI.color_green, 'SUCCESS', ANSI.reset)

	def draw_finish(self):
		BufferGUIElement.draw_finish(self)
		GCStreamHandler.pop_std_stream()
		if self._log_dump:
			self._stream.write('\n' + ANSI.erase_down + '-' * 20 + ' LOG HISTORY ' + '-' * 20 + '-\n')
			self._stream.write(self._buffer.getvalue())

	def draw_startup(self):
		GCStreamHandler.push_std_stream(self, self)
		BufferGUIElement.draw_startup(self)

	def _add_keyword(self, prefix, keyword, suffix):
		self._text_attr[keyword] = prefix + keyword + suffix
		for kw_idx in irange(1, len(keyword)):
			keyword_broken = keyword[:kw_idx] + ANSI.reset + ANSI.erase_line + '\n' + keyword[kw_idx:]
			self._text_attr[keyword_broken] = prefix + keyword_broken.replace('\n', '\n' + prefix) + suffix

	def _get_height(self):
		return None

	def _post_processing(self, value):
		for keyword in self._text_attr:
			value = value.replace(keyword, self._text_attr[keyword])
		return BufferGUIElement._post_processing(self, value)

	def _pre_processing(self, output_iter):
		if self._log_wrap:
			return self._wrap_lines(output_iter)
		return output_iter

	def _update_buffer(self):  # truncate message log
		msg_line_list = lmap(lambda line: line + '\n', self._buffer.getvalue().splitlines())
		erase_content(self._buffer)
		self._buffer.write(str.join('', msg_line_list[-self._log_max:]))

	def _wrap_lines(self, output_iter):
		for line in output_iter:
			while True:
				truncated_line = line[:self._layout_width].rstrip()
				if not truncated_line:
					break
				yield truncated_line
				line = line[self._layout_width:]


class ActivityGUIElement(AfterImageGUIElement):
	alias_list = ['activity']

	def __init__(self, config, name, workflow, redraw_event, stream):
		AfterImageGUIElement.__init__(self, config, name, workflow, redraw_event, stream)
		self._activity_monitor = config.get_plugin('activity stream', 'MultiActivityMonitor',
			cls=ActivityMonitor, pargs=(self._buffer,), on_change=None)
		self._height_max = config.get_int('activity height max', 5, on_change=None)
		self._height_min = config.get_int('activity height min', 1, on_change=None)
		self._height_last = 0

	def draw_finish(self):
		AfterImageGUIElement.draw_finish(self)
		Activity.callbacks.remove(self.make_dirty)

	def draw_startup(self):
		Activity.callbacks.append(self.make_dirty)
		AfterImageGUIElement.draw_startup(self)

	def _get_height(self):
		return min(self._height_min, max(self._height_min, self._height_last))

	def _update_buffer(self):
		erase_content(self._buffer)
		self._activity_monitor.write()
		self._buffer.flush()
		msg = ANSI.strip_cmd(self._buffer.getvalue()).strip()
		erase_content(self._buffer)
		self._height_last = 1 + msg.count('\n')
		self._buffer.write(msg)


class ReportGUIElement(BasicGUIElement):
	alias_list = ['report']

	def __init__(self, config, name, workflow, redraw_event, stream):
		BasicGUIElement.__init__(self, config, name, workflow, redraw_event, stream, 'console.report')
		self._job_manager = workflow.job_manager
		self._report = config.get_composited_plugin('report', 'ANSITheme', 'MultiReport',
			cls=Report, on_change=None, pargs=(self._job_manager.job_db, workflow.task))
		self._last_report_height = 0

	def draw_finish(self):
		BasicGUIElement.draw_finish(self)
		self._job_manager.remove_event_handler(self.make_dirty)

	def draw_startup(self):
		self._job_manager.add_event_handler(self.make_dirty)
		BasicGUIElement.draw_startup(self)

	def _get_height(self):
		return self._last_report_height

	def _update_buffer(self):
		erase_content(self._buffer)
		self._report.show_report(self._job_manager.job_db, self._job_manager.job_db.get_job_list())
		self._last_report_height = self._buffer.getvalue().count('\n')
