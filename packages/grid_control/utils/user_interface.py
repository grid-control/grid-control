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

import sys, signal, logging
from grid_control.utils.parsing import parse_bool
from python_compat import get_user_input, imap


class UserInputInterface(object):
	def __init__(self, user_input_fun=get_user_input):
		self._user_input_fun = user_input_fun
		self._log = logging.getLogger('console.input')

	def prompt_bool(self, text, default):
		default_str = 'no'
		if default:
			default_str = 'yes'
		return self._input_loop(text, default_str, ['yes', 'no'], parse_bool)

	def prompt_text(self, text, default=None, raise_error=True):
		try:
			return self._user_input_fun(self._output(text)).strip()
		except Exception:
			sys.stdout.write('\n')  # continue on next line
			if raise_error:
				raise
			return default

	def _input_loop(self, text, default, choices, parser):
		while True:
			handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
			userinput = self.prompt_text(self._output('%s %s: ' % (text, '[%s]' % default)))
			signal.signal(signal.SIGINT, handler)
			if userinput == '':
				return parser(default)
			if parser(userinput) is not None:
				return parser(userinput)
			valid = str.join(', ', imap(lambda x: '"%s"' % x, choices[:-1]))
			self._log.critical('Invalid input "%s"! Answer with %s or "%s"', userinput, valid, choices[-1])

	def _output(self, text):
		msg_str_list = text.splitlines()
		if len(msg_str_list) > 1:
			self._log.info(str.join('\n', msg_str_list[:-1]))
			return msg_str_list[-1]
		return text
