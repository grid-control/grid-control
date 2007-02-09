import os.path
from grid_control import Module

class UserMod(Module):
	def __init__(self, config):
		Module.__init__(self, config)
		self.config = config
		self.executable = config.getPath('UserMod', 'executable')
		self.inFiles = config.get('UserMod', 'inputfiles').split()
		self.outFiles = config.get('UserMod', 'outputfiles').split()
		self.userParameters = config.get('UserMod', 'userparameters')
		
	def getSoftwareMembers(self):
		return ('0')
		
	def getInFiles(self):
		return self.inFiles
			
			
	def getOutFiles(self):
		return self.outFiles
			
			
	def getJobArguments(self, job):
		return "%d %s %s" % (job, os.path.basename(self.executable),
		                     self.userParameters)
