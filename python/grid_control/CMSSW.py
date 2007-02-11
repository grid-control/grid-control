import os
from fnmatch import fnmatch
from xml.dom import minidom
from grid_control import ConfigError, Module, utils

class CMSSW(Module):
	def __init__(self, config):
		Module.__init__(self, config)

		self.projectArea = config.getPath('CMSSW', 'project area')
		self.configFile = config.getPath('CMSSW', 'config file')

		self.pattern = config.get('CMSSW', 'files').split()

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

		if not os.path.exists(self.configFile):
			raise ConfigError("Config file '%s' not found." % self.configFile)


	def init(self):
		# walk directory in project area
		def walk(dir):
			for file in os.listdir(os.path.join(self.projectArea, dir)):
				if len(dir):
					name = os.path.join(dir, file)
				else:
					name = file
				for match in self.pattern:
					neg = match[0] == '-'
					if neg: match = match[1:]
					if fnmatch(name, match):
						break
				else:
					if os.path.isdir(os.path.join(self.projectArea, name)):
						walk(name)
					continue

				if not neg:
					files.append(name)

		# walk project area subdirectories and find files
		files = []
		walk('')
		utils.genTarball(os.path.join(self.workDir, 'runtime.tar.gz'), 
		                 self.projectArea, files)

		try:
			fp = open(os.path.join(self.workDir, 'config.sh'), 'w')
			fp.write('SCRAM_VERSION="scramv1"\n');
			fp.write('SCRAM_PROJECTVERSION="%s"\n'
			         % self.scramEnv['SCRAM_PROJECTVERSION'])
			fp.truncate()
			fp.close()
		except IOError, e:
			raise InstallationError("Could not write config.sh: %s", str(e))


	def getInFiles(self):
		files = ['runtime.tar.gz', 'config.sh', self.configFile]
		def relocate(path):
			if not os.path.isabs(path):
				return os.path.join(self.workDir, path)
			else:
				return path
		return map(relocate, files)

	def getSoftwareMembers(self):
		return ('VO-cms-%s' % self.scramEnv['SCRAM_PROJECTVERSION'],)
