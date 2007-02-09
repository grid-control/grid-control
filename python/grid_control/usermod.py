import os.path
from grid_control import Module

class UserMod(Module):
	def __init__(self, config):
		Module.__init__(self, config)

		self.executable = config.getPath('UserMod', 'executable')
		self.inFiles = config.get('UserMod', 'input files').split()
		self.outFiles = config.get('UserMod', 'output files').split()


	def getInFiles(self):
		return self.inFiles


	def getOutFiles(self):
		return self.outFiles


	def getJobArguments(self, job):
		return "%d %s" % (job, os.path.basename(self.executable))
