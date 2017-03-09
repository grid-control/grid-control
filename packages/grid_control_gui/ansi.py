# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

import re, sys, fcntl, struct, termios
from hpfwk import ignore_exception


def console_ctrl(fmt_str, fmt_args=None):
	if fmt_args is None:
		def _fmt_args():
			return tuple()
		fmt_args = _fmt_args

	def _fmt_fun(cls, *args):
		sys.stdout.write('\033' + fmt_str % fmt_args(*args))
		sys.stdout.flush()
	if sys.stdout.isatty():
		return classmethod(_fmt_fun)
	return fmt_args


class Console(object):
	attr = {
		'COLOR_BLACK': '30', 'COLOR_RED': '31', 'COLOR_GREEN': '32',
		'COLOR_YELLOW': '33', 'COLOR_BLUE': '34', 'COLOR_MAGENTA': '35',
		'COLOR_CYAN': '36', 'COLOR_WHITE': '37', 'BOLD': '1', 'RESET': '0',
	}
	for (name, esc) in attr.items():
		locals()[name] = '\033[%sm' % esc

	reset = console_ctrl('[0m')
	inverse = console_ctrl('[7m')
	erase = console_ctrl('[2J')
	erase_down = console_ctrl('[J')
	erase_line = console_ctrl('[K')
	show_cursor = console_ctrl('[?25h')
	hide_cursor = console_ctrl('[?25l')
	load_pos = console_ctrl('8')
	save_pos = console_ctrl('7')
	wrap_on = console_ctrl('[?7h')
	wrap_off = console_ctrl('[?7l')
	move = console_ctrl('[%d;%dH', lambda row, col=0: (row + 1, col + 1))
	# default: disable regional scrolling - (it would move to 0,0)
	# outside of selected region, last line will scroll with height 1
	setscrreg = console_ctrl('[%d;%dr', lambda top=-1, bottom=-1: (top + 1, bottom + 1))

	def fmt(cls, data, attr=None, force_ansi=False):
		if force_ansi or sys.stdout.isatty():
			return Console.RESET + str.join('', attr or []) + data + Console.RESET
		return data
	fmt = classmethod(fmt)

	def fmt_strip(cls, value):
		return re.sub(r'\x1b(>|=|\[[^A-Za-z]*[A-Za-z])', '', value)
	fmt_strip = classmethod(fmt_strip)

	def getmaxyx(cls):
		def _getmaxyx():
			winsize_ptr = fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack("HHHH", 0, 0, 0, 0))
			winsize = struct.unpack('HHHH', winsize_ptr)
			return (winsize[0], winsize[1])
		return ignore_exception(Exception, (24, 80), _getmaxyx)  # 24x80 is vt100 default
	getmaxyx = classmethod(getmaxyx)
