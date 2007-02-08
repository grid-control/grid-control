import os, popen2
from grid_control import ConfigError, Module

class CMSSW(Module):
	def __init__(self, config):
		self.projectArea = config.getPath('CMSSW', 'project area')
		self.configFile  = config.getPath('CMSSW', 'config file')
		self.checkProjectArea()

	def checkProjectArea(self):
		if os.path.exists(self.projectArea):
			print ("Project area found in: %s" % self.projectArea) 
		else:
			raise ConfigError("Specified config area '%s' does not exist!" % self.projectArea)

		# check, if the specified project area is a CMSSW project area
		if os.file.exists('%s/.SCRAM/Environment.xml2' % self.projectArea):
			print ("Blubb")
