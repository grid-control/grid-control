#-#  Copyright 2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import sys, re, signal
from ansi import Console
from grid_control import GUI, utils, Report

class GUIStream:
	def __init__(self, stream, screen):
		(self.stream, self.screen, self.logged) = (stream, screen, True)

		# This is a list of (regular expression, GUI attributes).  The
		# attributes are applied to matches of the regular expression in
		# the output written into this stream.  Lookahead expressions
		# should not overlap with other regular expressions.
		self.attrs = [
			('DONE(?!:)', [Console.COLOR_BLUE, Console.BOLD]),
			('FAILED(?!:)', [Console.COLOR_RED, Console.BOLD]),
			('SUCCESS(?!:)', [Console.COLOR_GREEN, Console.BOLD]),
			('(?<=DONE:)\s+[1-9]\d*', [Console.COLOR_BLUE, Console.BOLD]),
			('(?<=Failing jobs:)\s+[1-9]\d*', [Console.COLOR_RED, Console.BOLD]),
			('(?<=FAILED:)\s+[1-9]\d*', [Console.COLOR_RED, Console.BOLD]),
			('(?<=Successful jobs:)\s+[1-9]\d*', [Console.COLOR_GREEN, Console.BOLD]),
			('(?<=SUCCESS:)\s+[1-9]\d*', [Console.COLOR_GREEN, Console.BOLD]),
		]
		self.regex = re.compile('(%s)' % '|'.join(map(lambda (a, b): a, self.attrs)))

	def _attributes(self, string, pos):
		""" Retrieve the attributes for a match in string at position pos. """
		for (expr, attr) in self.attrs:
			match = re.search(expr, string)
			if match and match.start() == pos:
				return attr
		return 0

	def write(self, data):
		if self.logged:
			GUIStream.backlog.pop(0)
			GUIStream.backlog.append(data)
		idx = 0
		match = self.regex.search(data[idx:])
		while match:
			self.screen.addstr(data[idx:idx + match.start()])
			self.screen.addstr(match.group(0), self._attributes(data[idx:], match.start()))
			idx += match.end()
			match = self.regex.search(data[idx:])
		self.screen.addstr(data[idx:])
		return True

	def __getattr__(self, name):
		return self.stream.__getattribute__(name)

	def dump(cls):
		for data in filter(lambda x: x, GUIStream.backlog):
			sys.stdout.write(data)
		sys.stdout.write('\n')
	dump = classmethod(dump)
GUIStream.backlog = [None] * 100


class ANSIGUI(GUI):
	def __init__(self, config, workflow):
		config.set('report', 'BasicBarReport', override = False)
		GUI.__init__(self, config, workflow)
		self._report = self._reportClass.getInstance(self._workflow.jobManager.jobDB, self._workflow.task)

	def displayWorkflow(self):
		report = self._report
		workflow = self._workflow
		def wrapper(screen):
			# Event handling for resizing
			def onResize(sig, frame):
				screen.savePos()
				(sizey, sizex) = screen.getmaxyx()
				screen.setscrreg(min(report.getHeight() + 2, sizey), sizey)
				utils.printTabular.wraplen = sizex - 5
				screen.loadPos()
			screen.erase()
			onResize(None, None)

			def guiWait(timeout):
				onResize(None, None)
				oldHandler = signal.signal(signal.SIGWINCH, onResize)
				result = utils.wait(timeout)
				signal.signal(signal.SIGWINCH, oldHandler)
				return result

			# Wrapping ActivityLog functionality
			class GUILog:
				def __init__(self, message):
					self.message = '%s...' % message
					self.show(self.message.center(65))

				def __del__(self):
					if hasattr(sys.stdout, 'logged'):
						self.show(' ' * len(self.message))

				def show(self, message):
					screen.savePos()
					screen.move(0, 0)
					sys.stdout.logged = False
					report.display()
					screen.move(report.getHeight() + 1, 0)
					sys.stdout.write('%s\n' % message)
					sys.stdout.logged = True
					screen.loadPos()

			# Main cycle - GUI mode
			saved = (sys.stdout, sys.stderr, utils.ActivityLog)
			try:
				utils.ActivityLog = GUILog
				sys.stdout = GUIStream(saved[0], screen)
				sys.stderr = GUIStream(saved[1], screen)
				workflow.jobCycle(guiWait)
			finally:
				if sys.modules['__main__'].log: del sys.modules['__main__'].log
				sys.stdout, sys.stderr, utils.ActivityLog = saved
				screen.setscrreg()
				screen.move(1, 0)
				screen.eraseDown()
				report.display()
				sys.stdout.write('\n')
		try:
			wrapper(Console())
		finally:
			GUIStream.dump()
