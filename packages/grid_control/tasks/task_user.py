import os.path
from grid_control import QM, datasets
from task_data import DataTask
from task_utils import TaskExecutableWrapper

class UserTask(DataTask):
	def __init__(self, config):
		DataTask.__init__(self, config)
		self._exeWrap = TaskExecutableWrapper(config.getScoped([self.__class__.__name__]))


	def getCommand(self):
		return '(%s) > job.stdout 2> job.stderr' % self._exeWrap.getCommand()


	def getJobArguments(self, jobNum):
		return DataTask.getJobArguments(self, jobNum) + ' ' + self._exeWrap.getArguments()


	def getSBInFiles(self):
		return DataTask.getSBInFiles(self) + self._exeWrap.getSBInFiles()


	def getSBOutFiles(self):
		tmp = map(lambda s: s + QM(self.gzipOut, '.gz', ''), ['job.stdout', 'job.stderr'])
		return DataTask.getSBOutFiles(self) + tmp


class UserMod(UserTask):
	pass
