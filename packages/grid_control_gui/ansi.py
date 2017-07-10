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

import re, sys, fcntl, atexit, struct, termios
from grid_control.utils import is_dumb_terminal
from hpfwk import ignore_exception


def create_ansi(value):
	if is_dumb_terminal():
		return ''
	return value


def create_ansi_fun(fmt_str, fmt_args_fun):
	if is_dumb_terminal():
		def _fmt_fun(*args):
			return ''
	else:
		def _fmt_fun(*args):
			return fmt_str % fmt_args_fun(*args)
	return staticmethod(_fmt_fun)


def create_grayscale_fun():
	term_caps = is_dumb_terminal()
	if term_caps is False:  # high_color
		def _fmt_grayscale(value):
			return chr(27) + '[38;5;%dm' % (232 + int(value * 23))
	elif term_caps is None:  # low color
		def _fmt_grayscale(value):
			return {0: ANSI.bold + ANSI.color_black, 1: ANSI.reset + ANSI.color_white,
				2: ANSI.bold + ANSI.color_white}[int(round(value * 2))]
	else:  # dumb terminal
		def _fmt_grayscale(value):
			return ANSI.color_white
	return staticmethod(_fmt_grayscale)


def fmt_scroll_args(top=-1, bottom=-1):  # no scrolling outside of region (incl. last line)
	return (top + 1, bottom + 1)  # default: disable regional scrolling - (it would move to 0,0)


def install_console_reset():
	try:
		install_console_reset.enabled
	except Exception:
		atexit.register(Console.reset_console)
		install_console_reset.enabled = True


class ANSI(object):
	back_black = create_ansi(chr(27) + '[40m')
	back_blue = create_ansi(chr(27) + '[44m')
	back_cyan = create_ansi(chr(27) + '[46m')
	back_default = create_ansi(chr(27) + '[49m')
	back_green = create_ansi(chr(27) + '[42m')
	back_magenta = create_ansi(chr(27) + '[45m')
	back_red = create_ansi(chr(27) + '[41m')
	back_white = create_ansi(chr(27) + '[47m')
	back_yellow = create_ansi(chr(27) + '[43m')
	blink = create_ansi(chr(27) + '[5m')
	bold = create_ansi(chr(27) + '[1m')
	color_black = create_ansi(chr(27) + '[30m')
	color_blue = create_ansi(chr(27) + '[34m')
	color_cyan = create_ansi(chr(27) + '[36m')
	color_default = create_ansi(chr(27) + '[39m')
	color_grayscale = create_grayscale_fun()
	color_green = create_ansi(chr(27) + '[32m')
	color_magenta = create_ansi(chr(27) + '[35m')
	color_red = create_ansi(chr(27) + '[31m')
	color_white = create_ansi(chr(27) + '[37m')
	color_yellow = create_ansi(chr(27) + '[33m')
	erase_line = create_ansi(chr(27) + '[K')
	erase = create_ansi(chr(27) + '[2J')
	erase_down = create_ansi(chr(27) + '[J')
	esc = create_ansi(chr(27) + '')
	invert = create_ansi(chr(27) + '[7m')
	move = create_ansi_fun(chr(27) + '[%d;%dH', lambda row, col=0: (row + 1, col + 1))
	move_up = create_ansi_fun(chr(27) + '[%dA', lambda rows: rows)
	pos_load = create_ansi(chr(27) + '8')
	pos_save = create_ansi(chr(27) + '7')
	reset = create_ansi(chr(27) + '[0m')
	show_cursor = create_ansi(chr(27) + '[?25h')
	set_scroll = create_ansi_fun(pos_save + chr(27) + '[%d;%dr' + pos_load, fmt_scroll_args)
	underline = create_ansi(chr(27) + '[4m')
	wrap_off = create_ansi(chr(27) + '[?7l')
	wrap_on = create_ansi(chr(27) + '[?7h')

	regex_strip_cmd = re.compile(chr(27) + r'(7|8|\[(\?)*([0-9]*;)*[0-9]*(A|H|J|K|h|l|r))')
	regex_strip_fmt = re.compile(chr(27) + r'(7|8|\[(\?)*([0-9]*;)*[0-9]*(A|H|J|K|h|l|r|m))')

	def strip_cmd(cls, value):
		return ANSI.regex_strip_cmd.sub('', value)
	strip_cmd = classmethod(strip_cmd)

	def strip_fmt(cls, value):
		return ANSI.regex_strip_fmt.sub('', value)
	strip_fmt = classmethod(strip_fmt)


class Console(object):
	COUNTER = 0

	def getmaxyx(cls):
		def _getmaxyx(fd_term=None):
			winsize_ptr = fcntl.ioctl(fd_term or sys.stdout.fileno(),
				termios.TIOCGWINSZ, struct.pack("HHHH", 0, 0, 0, 0))
			winsize = struct.unpack('HHHH', winsize_ptr)
			return (winsize[0], winsize[1])
		return ignore_exception(Exception, (24, 80), _getmaxyx)  # 24x80 is vt100 default
	getmaxyx = classmethod(getmaxyx)

	def reset_console(cls):
		try:
			sys.stdout.write(ANSI.set_scroll() + ANSI.erase_down + ANSI.show_cursor + ANSI.wrap_on)
			sys.stdout.flush()
		except Exception:
			pass
	reset_console = classmethod(reset_console)
