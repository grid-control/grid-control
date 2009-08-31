import os.path, random, time
from grid_control import DataMod

class UserMod(DataMod):
	def __init__(self, config):
		DataMod.__init__(self, config)
		self._executable = config.getPath('UserMod', 'executable')
		self._arguments = config.get('UserMod', 'arguments', '')


	def getCommand(self):
		cmd = os.path.basename(self._executable)
		return 'chmod u+x %s; ./%s $@' % (cmd, cmd)


	def getJobArguments(self, jobNum):
		if self.dataSplitter != None:
			return DataMod.getJobArguments(self, jobNum)
		return self._arguments


	def getInFiles(self):
		return DataMod.getInFiles(self) + [ self._executable ]
