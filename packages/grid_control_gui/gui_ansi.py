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

import sys, signal, threading
from grid_control.gui import GUI, GUIException
from grid_control.logging_setup import GCStreamHandler
from grid_control.utils import abort, is_dumb_terminal
from grid_control.utils.thread_tools import GCEvent, GCLock, start_daemon, with_lock
from grid_control_gui.ansi import ANSI, Console, install_console_reset
from grid_control_gui.ge_base import GUIElement
from hpfwk import ExceptionCollector, rethrow
from python_compat import StringBuffer


class ANSIGUI(GUI):
	alias_list = ['ansi']

	def __new__(cls, config, workflow):
		if is_dumb_terminal():
			return GUI.create_instance('BasicConsoleGUI', config, workflow)
		return GUI.__new__(cls)

	def __init__(self, config, workflow):
		GUI.__init__(self, config, workflow)
		install_console_reset()
		self._console_lock = GCLock(threading.RLock())  # terminal output lock
		self._exc = ExceptionCollector()
		(self._redraw_thread, self._redraw_shutdown) = (None, False)
		(self._redraw_event, self._immediate_redraw_event) = (GCEvent(rlock=True), GCEvent(rlock=True))
		self._redraw_interval = config.get_float('gui redraw interval', 0.1, on_change=None)
		self._redraw_delay = config.get_float('gui redraw delay', 0.05, on_change=None)
		element = config.get_composited_plugin('gui element', 'report activity log',
			'MultiGUIElement', cls=GUIElement, on_change=None, bind_kwargs={'inherit': True},
			pargs=(workflow, self._redraw_event, sys.stdout))
		self._element = FrameGUIElement(config, 'gui', workflow,
			self._redraw_event, sys.stdout, self._immediate_redraw_event, element)

	def end_interface(self):  # lots of try ... except .. finally - for clean console state restore
		def _end_interface():
			try:
				self._finish_drawing()
			finally:
				GCStreamHandler.set_global_lock()
				Console.reset_console()
		rethrow(GUIException('GUI shutdown exception'), _end_interface)
		self._exc.raise_any(GUIException('GUI drawing exception'))

	def start_interface(self):
		GCStreamHandler.set_global_lock(self._console_lock)
		with_lock(self._console_lock, self._element.draw_startup)
		self._redraw_shutdown = False  # start redraw thread
		self._redraw_thread = start_daemon('GUI draw thread', self._redraw)

	def _finish_drawing(self):
		def _final_draw():
			try:
				self._element.make_dirty()
			finally:
				self._redraw_shutdown = True  # stop redraw thread
				self._redraw_event.set()
		try:
			try:
				with_lock(self._console_lock, _final_draw)  # last redraw
			finally:
				if self._redraw_thread:
					self._redraw_thread.join(5 + self._redraw_interval)
		finally:
			with_lock(self._console_lock, self._element.draw_finish)  # draw finish

	def _redraw(self):
		try:
			while not self._redraw_shutdown:
				self._redraw_event.wait(timeout=self._redraw_interval)
				self._immediate_redraw_event.wait(timeout=self._redraw_delay)
				with_lock(self._console_lock, self._element.redraw)
				self._immediate_redraw_event.clear()
				self._redraw_event.clear()
		except Exception:
			self._exc.collect()
			abort(True)


class FrameGUIElement(GUIElement):
	def __init__(self, config, name, workflow, redraw_event, stream, immediate_redraw_event, element):
		GUIElement.__init__(self, config, name, workflow, redraw_event, stream)
		(self._immediate_redraw_event, self._element) = (immediate_redraw_event, element)
		self._dump_stream = config.get_bool('gui dump stream', True, on_change=None)
		self._console_dim_fn = Console.getmaxyx
		(self._std_stream, self._old_resize_handler) = (StringBuffer(), None)

	def draw_finish(self):
		signal.signal(signal.SIGWINCH, self._old_resize_handler)  # restore old resize handler
		try:
			self._element.draw_finish()
		finally:
			GCStreamHandler.pop_std_stream()
			if self._dump_stream:  # display logging output
				sys.stderr.write(self._std_stream.getvalue())
			self._stream.write('\n')

	def draw_startup(self):
		GUIElement.draw_startup(self)
		self._old_resize_handler = signal.signal(signal.SIGWINCH, self._on_size_change)  # resize handler
		GCStreamHandler.push_std_stream(self._std_stream, self._std_stream)  # hide all logging output
		self._on_size_change()  # initial setup of element size
		self._element.draw_startup()

	def make_dirty(self):
		self._element.make_dirty()

	def redraw(self):  # redraw and go to waiting position
		self._element.redraw()
		self._stream.write(ANSI.move(self._element.get_height() or self._console_dim_fn()[0], 0))
		sys.stdout.flush()
		sys.stderr.flush()

	def set_layout(self, pos, height, width, on_height_change=None):
		return self._element.set_layout(pos, height, width, on_height_change)

	def _draw(self):
		pass

	def _on_size_change(self, *args, **kwargs):
		self.set_layout(0, *self._console_dim_fn())
		self._immediate_redraw_event.set()  # all locks (incl. event lock) in signals have to be RLocks!
		self._redraw_event.set()  # trigger immediate redraw
