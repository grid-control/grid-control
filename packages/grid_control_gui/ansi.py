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

import sys, fcntl, termios, array

class Console:
	attr = {'COLOR_BLACK': '30', 'COLOR_RED': '31', 'COLOR_GREEN': '32',
		'COLOR_YELLOW': '33', 'COLOR_BLUE': '34', 'COLOR_MAGENTA': '35',
		'COLOR_CYAN': '36', 'COLOR_WHITE': '37', 'BOLD': '1', 'RESET': '0'}
	cmd = {'savePos': '7', 'loadPos': '8', 'eraseDown': '[J', 'erase': '[2J'}
	for (name, esc) in attr.items():
		locals()[name] = esc

	def fmt(cls, data, attr = []):
		class ColorString:
			def __init__(self, value, attr):
				(self._data, self._attr) = (data, attr)
			def __len__(self):
				return len(self._data)
			def __str__(self):
				return '\033[%sm%s\033[0m' % (str.join(';', [Console.RESET] + self._attr), self._data)
		if sys.stdout.isatty():
			return ColorString(data, attr)
		return data
	fmt = classmethod(fmt)

	def __init__(self):
		(self.stdout, self.stdin) = (sys.stdout, sys.stdin)
		def callFactory(x):
			return lambda: self._esc(x)
		for (proc, esc) in Console.cmd.items():
			setattr(self, proc, callFactory(esc))

	def _esc(self, data):
		self.stdout.write('\033' + data)
		self.stdout.flush()

	def getmaxyx(self):
		size = array.array('B', [0, 0, 0, 0])
		fcntl.ioctl(0, termios.TIOCGWINSZ, size, True)
		return (size[0], size[2])

	def move(self, row, col):
		self._esc('[%d;%dH' % (row, col))

	def setscrreg(self, top = 0, bottom = 0):
		self._esc('[%d;%dr' % (top, bottom))

	def addstr(self, data, attr = []):
		self.stdout.write(str(Console.fmt(data, attr)))
		self.stdout.flush()
