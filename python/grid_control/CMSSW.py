import os, popen2
from grid_control import ConfigError, Module

class CMSSW(Module):
	def __init__(self, config):
		self.projectArea = config.getPath('CMSSW', 'projectArea')
		self.checkProjectArea()

	def checkProjectArea(self):
		if os.path.exists(self.projectArea):
			print ("Project area found in: %s" % self.projectArea) 
		else:
			raise ConfigError("Specified config area '%s' does not exist!" % self.projectArea)
