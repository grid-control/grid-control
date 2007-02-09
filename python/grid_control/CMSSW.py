import os
from xml.dom import minidom
from grid_control import ConfigError, Module

class CMSSW(Module):
	def __init__(self, config):
		self.projectArea = config.getPath('CMSSW', 'project area')
		self.configFile  = config.getPath('CMSSW', 'config file')

		if os.path.exists(self.projectArea):
			print "Project area found in: %s" % self.projectArea
		else:
			raise ConfigError("Specified config area '%s' does not exist!" % self.projectArea)

		# check, if the specified project area is a CMSSW project area
		envFile = os.path.join(self.projectArea, '.SCRAM', 'Environment.xml')
		if not os.path.exists(envFile):
			raise ConfigError("Project area is not a SCRAM area.")

		# try to open it
		try:
			fp = open(envFile, 'r')
		except IOError, e:
			raise ConfigError("Project area .SCRAM/Environment.xml cannot be opened: %s" + str(e))

		# try to parse it
		try:
			xml = minidom.parse(fp)
		except :
			raise ConfigError("Project area .SCRAM/Environment.xml file invalid.")
		fp.close()

		# find entries
		self.scramEnv = {}
		try:
			for node in xml.childNodes[0].childNodes:
				if node.nodeType != minidom.Node.ELEMENT_NODE:
					continue
				if node.nodeName != 'environment':
					continue

				for key, value in node.attributes.items():
					if type(key) == unicode:
						key = key.encode('utf-8')
					if type(value) == unicode:
						value = value.encode('utf-8')

					self.scramEnv[key] = value

		except:
			raise ConfigError("Project area .SCRAM/Environment.xml has bad XML structure.")

		if not self.scramEnv.has_key('SCRAM_PROJECTNAME') or \
		   not self.scramEnv.has_key('SCRAM_PROJECTVERSION') or \
		   self.scramEnv['SCRAM_PROJECTNAME'] != 'CMSSW':
			raise ConfigError("Project area not a valid CMSSW project area.")


	def getSoftwareMembers(self):
		return ('VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION'],)

	def getJobArguments(self, job):
		return "%d" % job
