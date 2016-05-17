# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.tasks.task_data import DataTask
from grid_control.tasks.task_utils import TaskExecutableWrapper
from python_compat import lmap

class UserTask(DataTask):
	alias = ['UserMod', 'user', 'script']
	configSections = DataTask.configSections + ['UserMod', 'UserTask']

	def __init__(self, config, name):
		DataTask.__init__(self, config, name)
		self._exeWrap = TaskExecutableWrapper(config)


	def getCommand(self):
		return '(%s) > job.stdout 2> job.stderr' % self._exeWrap.getCommand()


	def getJobArguments(self, jobNum):
		return DataTask.getJobArguments(self, jobNum) + ' ' + self._exeWrap.getArguments()


	def getSBInFiles(self):
		return DataTask.getSBInFiles(self) + self._exeWrap.getSBInFiles()


	def getSBOutFiles(self):
		tmp = lmap(lambda s: s + utils.QM(self.gzipOut, '.gz', ''), ['job.stdout', 'job.stderr'])
		return DataTask.getSBOutFiles(self) + tmp
