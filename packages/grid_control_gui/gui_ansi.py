# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

import re, sys, time, signal, threading
from grid_control import utils
from grid_control.gui import GUI
from grid_control.utils.activity import Activity
from grid_control.utils.thread_tools import GCLock, start_thread
from grid_control_gui.ansi import Console
from python_compat import identity, ifilter, imap, itemgetter, lmap

class GUIStream(object):
	def __init__(self, stream, console, lock):
		(self._stream, self._console, self.logged, self._log, self._lock) = (stream, console, True, [None] * 100, lock)

		# This is a list of (regular expression, GUI attributes).  The
		# attributes are applied to matches of the regular expression in
		# the output written into this stream.  Lookahead expressions
		# should not overlap with other regular expressions.
		attrs = [
			(r'DONE(?!:)', [Console.COLOR_BLUE, Console.BOLD]),
			(r'FAILED(?!:)', [Console.COLOR_RED, Console.BOLD]),
			(r'SUCCESS(?!:)', [Console.COLOR_GREEN, Console.BOLD]),
			(r'(?<=DONE:)\s+[1-9]\d*', [Console.COLOR_BLUE, Console.BOLD]),
			(r'(?<=Failing jobs:)\s+[1-9]\d*', [Console.COLOR_RED, Console.BOLD]),
			(r'(?<=FAILED:)\s+[1-9]\d*', [Console.COLOR_RED, Console.BOLD]),
			(r'(?<=Successful jobs:)\s+[1-9]\d*', [Console.COLOR_GREEN, Console.BOLD]),
			(r'(?<=SUCCESS:)\s+[1-9]\d*', [Console.COLOR_GREEN, Console.BOLD]),
		]
		self._match_any_attr = re.compile('(%s)' % '|'.join(imap(itemgetter(0), attrs)))
		self._attrs = lmap(lambda expr_attr: (re.compile(expr_attr[0]), expr_attr[1]), attrs)

	def _text_attributes(self, value, pos):
		""" Retrieve the attributes for a match in value at position pos. """
		for (regex, attr) in self._attrs:
			match = regex.search(value)
			if match and match.start() == pos:
				return attr

	def write(self, data):
		self._lock.acquire()
		try:
			if self.logged:
				self._log.pop(0)
				self._log.append(data)
			idx = 0
			match = self._match_any_attr.search(data[idx:])
			while match:
				self._console.addstr(data[idx:idx + match.start()])
				self._console.addstr(match.group(0), self._text_attributes(data[idx:], match.start()))
				idx += match.end()
				match = self._match_any_attr.search(data[idx:])
			self._console.addstr(data[idx:])
			self._console.eraseLine()
			return True
		finally:
			self._lock.release()

	def __getattr__(self, name):
		return self._stream.__getattribute__(name)

	def dump(self):
		stored_logged = self.logged
		self.logged = False
		for data in str.join('', ifilter(identity, self._log)).splitlines():
			self._console.eraseLine()
			self.write(data + '\n')
		self.logged = stored_logged


class ANSIGUI(GUI):
	def __init__(self, config, workflow):
		config.set('report', 'BasicReport BarReport')
		(self._stored_stdout, self._stored_stderr) = (sys.stdout, sys.stderr)
		GUI.__init__(self, config, workflow)
		self._reportHeight = 0
		self._statusHeight = 1
		self._old_message = None
		self._lock = GCLock(threading.RLock()) # drawing lock
		self._last_report = 0
		self._old_size = None

	def _draw(self, fun):
		new_size = self._console.getmaxyx()
		if self._old_size != new_size:
			self._old_size = new_size
			self._schedule_update_layout()
		self._lock.acquire()
		self._console.hideCursor()
		self._console.savePos()
		try:
			fun()
		finally:
			self._console.loadPos()
			self._console.showCursor()
			self._lock.release()

	# Event handling for resizing
	def _update_layout(self):
		(sizey, sizex) = self._console.getmaxyx()
		self._old_size = (sizey, sizex)
		self._reportHeight = self._report.getHeight()
		self._console.erase()
		self._console.setscrreg(min(self._reportHeight + self._statusHeight + 1, sizey), sizey)
		utils.printTabular.wraplen = sizex - 5
		self._update_all()

	def _schedule_update_layout(self, sig = None, frame = None):
		start_thread('update layout', self._draw, self._update_layout) # using new thread to ensure RLock is free

	def _wait(self, timeout):
		oldHandler = signal.signal(signal.SIGWINCH, self._schedule_update_layout)
		result = utils.wait(timeout)
		signal.signal(signal.SIGWINCH, oldHandler)
		return result

	def _update_report(self):
		if time.time() - self._last_report < 1:
			return
		self._last_report = time.time()
		self._console.move(0, 0)
		self._new_stdout.logged = False
		self._report.display()
		self._new_stdout.logged = True

	def _update_status(self):
		activity_message = None
		for activity in Activity.root.get_children():
			activity_message = activity.getMessage() + '...'
			if len(activity_message) > 75:
				activity_message = activity_message[:37] + '...' + activity_message[-35:]

		self._console.move(self._reportHeight + 1, 0)
		self._new_stdout.logged = False
		if self._old_message:
			self._stored_stdout.write(self._old_message.center(65) + '\r')
			self._stored_stdout.flush()
		self._old_message = activity_message
		if activity_message:
			self._stored_stdout.write('%s' % activity_message.center(65))
			self._stored_stdout.flush()
		self._new_stdout.logged = True

	def _update_log(self):
		self._console.move(self._reportHeight + 2, 0)
		self._console.eraseDown()
		self._new_stdout.dump()

	def _update_all(self):
		self._last_report = 0
		self._update_report()
		self._update_status()
		self._update_log()

	def _schedule_update_report_status(self):
		self._draw(self._update_report)
		self._draw(self._update_status)

	def displayWorkflow(self):
		if not sys.stdout.isatty():
			return self._workflow.process(self._wait)

		self._console = Console(sys.stdout)
		self._new_stdout = GUIStream(sys.stdout, self._console, self._lock)
		self._new_stderr = GUIStream(sys.stderr, self._console, self._lock)
		Activity.callbacks.append(self._schedule_update_report_status)
		try:
			# Main cycle - GUI mode
			(sys.stdout, sys.stderr) = (self._new_stdout, self._new_stderr)
			self._console.erase()
			self._schedule_update_layout()
			self._workflow.process(self._wait)
		finally:
			(sys.stdout, sys.stderr) = (self._stored_stdout, self._stored_stderr)
			self._console.setscrreg()
			self._console.erase()
			self._update_all()
