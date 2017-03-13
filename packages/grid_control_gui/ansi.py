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
from hpfwk import ignore_exception


def create_fmt_fun(fmt_str, fmt_args_fun=None):
	if fmt_args_fun is None:
		def _fmt_args_fun():
			return tuple()
		fmt_args_fun = _fmt_args_fun

	if hasattr(sys.stdout, 'isatty') and sys.stdout.isatty():
		def _fmt_fun(*args):
			return fmt_str % fmt_args_fun(*args)
	else:
		def _fmt_fun(*args):
			return ''
	return staticmethod(_fmt_fun)


def fmt_scroll_args(top=-1, bottom=-1):  # no scrolling outside of region (incl. last line)
	return (top + 1, bottom + 1)  # default: disable regional scrolling - (it would move to 0,0)


def install_console_reset():
	try:
		install_console_reset.enabled
	except Exception:
		atexit.register(Console.reset_console)
		install_console_reset.enabled = True


class ANSI(object):
	back_black = chr(27) + '[40m'
	back_blue = chr(27) + '[44m'
	back_cyan = chr(27) + '[46m'
	back_default = chr(27) + '[49m'
	back_green = chr(27) + '[42m'
	back_magenta = chr(27) + '[45m'
	back_red = chr(27) + '[41m'
	back_white = chr(27) + '[47m'
	back_yellow = chr(27) + '[43m'
	blink = chr(27) + '[5m'
	bold = chr(27) + '[1m'
	color_black = chr(27) + '[30m'
	color_blue = chr(27) + '[34m'
	color_cyan = chr(27) + '[36m'
	color_default = chr(27) + '[39m'
	color_grayscale = create_fmt_fun(chr(27) + '[38;5;%dm', lambda value: 232 + int(value * 23))
	color_green = chr(27) + '[32m'
	color_magenta = chr(27) + '[35m'
	color_red = chr(27) + '[31m'
	color_white = chr(27) + '[37m'
	color_yellow = chr(27) + '[33m'
	erase_line = chr(27) + '[K'
	erase = chr(27) + '[2J'
	erase_down = chr(27) + '[J'
	esc = chr(27) + ''
	invert = chr(27) + '[7m'
	move = create_fmt_fun(chr(27) + '[%d;%dH', lambda row, col=0: (row + 1, col + 1))
	move_up = create_fmt_fun(chr(27) + '[%dA', lambda rows: rows)
	pos_load = chr(27) + '8'
	pos_save = chr(27) + '7'
	reset = chr(27) + '[0m'
	show_cursor = chr(27) + '[?25h'
	set_scroll = create_fmt_fun(pos_save + chr(27) + '[%d;%dr' + pos_load, fmt_scroll_args)
	underline = chr(27) + '[4m'
	wrap_off = chr(27) + '[?7l'
	wrap_on = chr(27) + '[?7h'

	def fmt(cls, data, attr=None, force_ansi=False):
		if force_ansi or (hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()):
			return str.join('', [ANSI.reset] + (attr or []) + [data, ANSI.reset])
		return data
	fmt = classmethod(fmt)

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
		def _getmaxyx():
			winsize_ptr = fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack("HHHH", 0, 0, 0, 0))
			winsize = struct.unpack('HHHH', winsize_ptr)
			return (winsize[0], winsize[1])
		return ignore_exception(Exception, (24, 80), _getmaxyx)  # 24x80 is vt100 default
	getmaxyx = classmethod(getmaxyx)

	def reset_console(cls):
		sys.stdout.write(ANSI.set_scroll() + ANSI.erase_down + ANSI.show_cursor + ANSI.wrap_on)
		sys.stdout.flush()
	reset_console = classmethod(reset_console)
