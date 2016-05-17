# | Copyright 2010-2016 Karlsruhe Institute of Technology
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
from grid_control.config import ConfigError, changeInitNeeded
from grid_control.tasks.task_user import UserTask

class ROOTTask(UserTask):
	alias = ['ROOTMod', 'root']
	configSections = UserTask.configSections + ['ROOTMod', 'ROOTTask']

	def __init__(self, config, name):
		# Determine ROOT path from previous settings / environment / config file
		self._rootpath = config.get('root path', os.environ.get('ROOTSYS', ''), persistent = True, onChange = changeInitNeeded('sandbox'))
		if not self._rootpath:
			raise ConfigError('Either set environment variable "ROOTSYS" or set option "root path"!')
		utils.vprint('Using the following ROOT path: %s' % self._rootpath, -1)

		# Special handling for executables bundled with ROOT
		self._executable = config.get('executable', onChange = changeInitNeeded('sandbox'))
		exeFull = os.path.join(self._rootpath, 'bin', self._executable.lstrip('/'))
		self.builtIn = os.path.exists(exeFull)
		if self.builtIn:
			config.set('send executable', 'False')
			# store resolved built-in executable path?

		# Apply default handling from UserTask
		UserTask.__init__(self, config, name)
		self.updateErrorDict(utils.pathShare('gc-run.root.sh'))

		# Collect lib files needed by executable
		self.libFiles = []


	def getTaskConfig(self):
		return utils.mergeDicts([UserTask.getTaskConfig(self), {'GC_ROOTSYS': self._rootpath}])


	def getCommand(self):
		cmd = './gc-run.root.sh %s $@ > job.stdout 2> job.stderr' % self._executable
		return utils.QM(self.builtIn, '', 'chmod u+x %s; ' % self._executable) + cmd


	def getSBInFiles(self):
		return UserTask.getSBInFiles(self) + self.libFiles + [
			utils.Result(pathAbs = utils.pathShare('gc-run.root.sh'), pathRel = 'gc-run.root.sh')]
