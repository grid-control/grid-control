# | Copyright 2013-2016 Karlsruhe Institute of Technology
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
from grid_control import utils
from grid_control.config import TriggerInit
from python_compat import unspecified


class TaskExecutableWrapper:
	def __init__(self, config, prefix='', executable_default=unspecified):
		initSandbox = TriggerInit('sandbox')
		self._executable_send = config.get_bool('%s send executable' % prefix, True, on_change=initSandbox)
		if self._executable_send:
			self._executable = config.get_path('%s executable' % prefix, executable_default, on_change=initSandbox)
		else:
			self._executable = config.get('%s executable' % prefix, executable_default, on_change=initSandbox)
		self._arguments = config.get('%s arguments' % prefix, '', on_change=initSandbox)


	def is_active(self):
		return self._executable != ''


	def get_command(self):
		if self._executable_send:
			cmd = os.path.basename(self._executable)
			return 'chmod u+x %s; ./%s $@' % (cmd, cmd)
		return '%s $@' % str.join('; ', self._executable.splitlines())


	def getArguments(self):
		return self._arguments


	def getSBInFiles(self):
		if self._executable_send and self._executable:
			return [utils.Result(path_abs=self._executable, path_rel=os.path.basename(self._executable))]
		return []
