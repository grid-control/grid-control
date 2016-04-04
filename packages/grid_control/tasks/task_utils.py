# | Copyright 2013-2015 Karlsruhe Institute of Technology
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
from grid_control.config import changeInitNeeded, noDefault

class TaskExecutableWrapper:
	def __init__(self, config, prefix = '', exeDefault = noDefault):
		initSandbox = changeInitNeeded('sandbox')
		self._executableSend = config.getBool('%s send executable' % prefix, True, onChange = initSandbox)
		if self._executableSend:
			self._executable = config.getPath('%s executable' % prefix, exeDefault, onChange = initSandbox)
		else:
			self._executable = config.get('%s executable' % prefix, exeDefault, onChange = initSandbox)
		self._arguments = config.get('%s arguments' % prefix, '', onChange = initSandbox)


	def isActive(self):
		return self._executable != ''


	def getCommand(self):
		if self._executableSend:
			cmd = os.path.basename(self._executable)
			return 'chmod u+x %s; ./%s $@' % (cmd, cmd)
		return '%s $@' % str.join('; ', self._executable.splitlines())


	def getArguments(self):
		return self._arguments


	def getSBInFiles(self):
		if self._executableSend and self._executable:
			return [utils.Result(pathAbs = self._executable, pathRel = os.path.basename(self._executable))]
		return []
