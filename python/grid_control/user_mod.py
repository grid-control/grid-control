import os.path, random, time
from grid_control import Module

class UserMod(Module):
	def __init__(self, config, proxy):
		Module.__init__(self, config, proxy)
		self._executable = config.getPath('UserMod', 'executable')
		self._arguments = config.get('UserMod', 'arguments', '')


	def getCommand(self):
		cmd = os.path.basename(self._executable)
		return 'chmod u+x %s; ./%s $@' % (cmd, cmd)


	def getJobArguments(self, jobNum):
		return self._arguments


	def getInFiles(self):
		return Module.getInFiles(self) + [ self._executable ]


	def onJobSubmit(self, job, id, dbmessage = [{}]):
		Module.onJobSubmit(self, job, id, dbmessage + [dict.fromkeys(["application", "exe"], "shellscript")])
