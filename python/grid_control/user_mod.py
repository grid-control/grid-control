import os.path
from grid_control import Module

class UserMod(Module):
	def __init__(self, config, init):
		Module.__init__(self, config, init)
		self._executable = config.getPath('UserMod', 'executable')
		self._arguments = config.get('UserMod', 'arguments')


	def getInFiles(self):
		return Module.getInFiles(self) + [ self._executable ]


	def getCommand(self):
		cmd = os.path.basename(self._executable)
		return 'chmod u+x %s; ./%s "$@"' % (cmd, cmd)


	def getJobArguments(self, job):
		return self._arguments
