# | Copyright 2013-2017 Karlsruhe Institute of Technology
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

import os
from grid_control.config import TriggerInit
from grid_control.utils import Result
from python_compat import unspecified


class TaskExecutableWrapper(object):
	def __init__(self, config, prefix='', executable_default=unspecified):
		init_sandbox = TriggerInit('sandbox')
		self._executable_send = config.get_bool('%s send executable' % prefix, True,
			on_change=init_sandbox)
		if self._executable_send:
			self._executable = config.get_fn('%s executable' % prefix, executable_default,
				on_change=init_sandbox)
		else:
			self._executable = config.get('%s executable' % prefix, executable_default,
				on_change=init_sandbox)
		self._arguments = config.get('%s arguments' % prefix, '', on_change=init_sandbox)

	def get_arguments(self):
		return self._arguments

	def get_command(self):
		if self._executable_send:
			cmd = os.path.basename(self._executable)
			return 'chmod u+x %s; ./%s $@' % (cmd, cmd)
		return '%s $@' % str.join('; ', self._executable.splitlines())

	def get_sb_in_fpi_list(self):
		if self._executable_send and self._executable:
			return [Result(path_abs=self._executable, path_rel=os.path.basename(self._executable))]
		return []

	def is_active(self):
		return self._executable != ''
