# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import re, sys, signal, threading
from grid_control.gui import GUI
from grid_control.logging_setup import GCStreamHandler, StderrStreamHandler, StdoutStreamHandler
from grid_control.utils.thread_tools import GCEvent, GCLock, start_daemon
from grid_control_gui.ansi import Console
from grid_control_gui.display_elements import ActivityElement, LogElement, ReportElement
from hpfwk import DebugInterface, ignore_exception
from python_compat import lmap


class GUILayout(object):
	def __init__(self, console_lock):
		(self._console_lock, self._element_list, self._layout_list) = (console_lock, [], [])
		(self._redraw_event, self._redraw_thread, self._redraw_shutdown) = (GCEvent(), None, False)
		self._old_resize_handler = None
		DebugInterface.callback_list.append((self.finish_display, self.initial_display))

	def __del__(self):
		self._redraw_thread.join()

	def add_element(self, element_cls, *args):
		self._element_list.append(element_cls(self, self._redraw_event, *args))

	def finish_display(self):
		try:
			self._redraw_shutdown = True
			self._redraw_event.set()
			for element in self._element_list:
				element.draw_finish()
			self._redraw_thread.join()
		finally:
			Console.save_pos()
			Console.setscrreg()
			Console.load_pos()
			Console.show_cursor()
			Console.wrap_on()
		signal.signal(signal.SIGWINCH, self._old_resize_handler)

	def initial_display(self):
		self._redraw_shutdown = False
		self._old_resize_handler = signal.signal(signal.SIGWINCH, self._schedule_redraw)
		self._redraw_thread = start_daemon('GUI draw thread', self._redraw)
		self._console_lock.acquire()
		try:
			self._update_layout()
			for element in self._element_list:
				element.draw_init()
			for idx, element in enumerate(self._element_list):
				if isinstance(element, LogElement):
					Console.move(self._layout_list[idx][0])
		finally:
			self._console_lock.release()

	def _redraw(self):
		while not self._redraw_shutdown:
			force_redraw = self._redraw_event.wait(timeout=1)
			self._console_lock.acquire()
			try:
				force_redraw = self._update_layout() or force_redraw
				Console.hide_cursor()
				for element in self._element_list:
					element.redraw(force=force_redraw)
				Console.show_cursor()
				sys.stdout.flush()
				sys.stderr.flush()
				self._redraw_event.clear()
			finally:
				self._console_lock.release()

	def _schedule_redraw(self, *args, **kwargs):
		self._redraw_event.set()

	def _update_layout(self):
		pos = 0
		height_total = sum(lmap(lambda element: element.get_height() or 0, self._element_list))
		height_avail = Console.getmaxyx()[0] - 1
		layout_list = []
		for element in self._element_list:
			height = element.get_height()
			if height is None:
				height = height_avail - height_total
			element.set_layout(pos, height)
			layout_list.append((pos, height))
			pos += height
		if self._layout_list != layout_list:
			self._layout_list = layout_list
			return True


class GUIStream(object):
	def __init__(self, stream):
		self._stream = stream
		# This is a list of (regular expression, GUI attributes).  The
		# attributes are applied to matches of the regular expression in
		# the output written into this stream.  Lookahead expressions
		# should not overlap with other regular expressions.
		rcmp = re.compile
		self._regex_attr_list = [
			(rcmp(r'DONE(?!:)'), Console.COLOR_BLUE + Console.BOLD),
			(rcmp(r'FAILED(?!:)'), Console.COLOR_RED + Console.BOLD),
			(rcmp(r'SUCCESS(?!:)'), Console.COLOR_GREEN + Console.BOLD),
			(rcmp(r'(?<=DONE:)\s+[1-9]\d*'), Console.COLOR_BLUE + Console.BOLD),
			(rcmp(r'(?<=Failing jobs:)\s+[1-9]\d*'), Console.COLOR_RED + Console.BOLD),
			(rcmp(r'(?<=FAILED:)\s+[1-9]\d*'), Console.COLOR_RED + Console.BOLD),
			(rcmp(r'(?<=Successful jobs:)\s+[1-9]\d*'), Console.COLOR_GREEN + Console.BOLD),
			(rcmp(r'(?<=SUCCESS:)\s+[1-9]\d*'), Console.COLOR_GREEN + Console.BOLD),
		]

	def __getattr__(self, name):
		return self._stream.__getattribute__(name)

	def write(self, value):
		value = value.replace('\n', '\033[K\n')  # perform erase_line at each newline
		for (regex, attr) in self._regex_attr_list:
			value = regex.sub(lambda match: Console.RESET + attr + match.group(0) + Console.RESET, value)
		self._stream.write(value)


class ANSIGUI(GUI):
	def __new__(cls, config, workflow):
		if not sys.stdout.isatty():
			return GUI.create_instance('SimpleConsole', config, workflow)
		return GUI.__new__(cls)

	def __init__(self, config, workflow):
		config.set('report', 'BasicReport BarReport')
		GUI.__init__(self, config, workflow)
		self._console_lock = GCLock(threading.RLock())  # terminal output lock
		self._layout = GUILayout(self._console_lock)
		try:
			self._layout.add_element(ActivityElement)
			self._layout.add_element(ReportElement, self._report, workflow.job_manager.job_db)
			self._layout.add_element(LogElement)
			self._layout.initial_display()
		except Exception:
			self._layout.finish_display()
			raise

	def start_display(self):
		old_logout = self._set_gui_stream(StdoutStreamHandler, sys.stdout)  # ensure exclusive access for
		old_logerr = self._set_gui_stream(StderrStreamHandler, sys.stderr)  # logging to stdout/stderr
		try:
			try:
				self._workflow.process()
			finally:
				self._layout.finish_display()
				for (handler, stream) in [(StdoutStreamHandler, old_logout), (StderrStreamHandler, old_logerr)]:
					handler.stream = stream
					ignore_exception(Exception, None, lambda obj: obj.enable_activity_callback(), stream)
				GCStreamHandler.set_global_lock()
		except (KeyboardInterrupt, SystemExit, Exception):
			Console.move(Console.getmaxyx()[0])
			raise

	def _set_gui_stream(self, stream_handler_cls, stream):
		old_stream = stream_handler_cls.stream
		ignore_exception(Exception, None, lambda stream: stream.disable_activity_callback(), old_stream)
		stream_handler_cls.set_global_lock(self._console_lock)
		stream_handler_cls.stream = GUIStream(stream)
		return old_stream
