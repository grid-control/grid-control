import os.path
from grid_control import Module

class UserMod(Module):
	def __init__(self, config, init):
		Module.__init__(self, config, init)

		self.executable = config.getPath('UserMod', 'executable')
		self.arguments = config.get('UserMod', 'arguments')



	def getCommand(self):
		return './%s "$@"' % self.executable


	def getInFiles(self):
		return Module.getInFiles(self) + [ self.executable ]


	def getJobArguments(self, job):
		return self.arguments
