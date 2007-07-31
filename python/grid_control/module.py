# Generic base class for job modules
# instantiates named class instead (default is UserMod)

import os.path, cStringIO, StringIO
from grid_control import ConfigError, AbstractObject, utils, WMS

class Module(AbstractObject):
	def __init__(self, config, init):
		self.config = config
		self.workDir = config.getPath('global', 'workdir')
		wallTime = config.getInt('jobs', 'wall time') * 60 * 60
		cpuTime = config.getInt('jobs', 'cpu time', 10 * 60)
		memory = config.getInt('jobs', 'memory', 512)
		self.requirements = [ (WMS.WALLTIME, wallTime),
				      (WMS.CPUTIME, cpuTime),
				      (WMS.MEMORY, memory) ]


	def getConfig(self):
		return {}


	def getRequirements(self, job):
		return self.requirements


	def makeConfig(self):
		data = self.getConfig()
		data['MY_RUNTIME'] = self.getCommand()
		data['MY_OUT'] = str.join(' ', self.getOutFiles())

		fp = cStringIO.StringIO()
		for key, value in data.items():
			fp.write("%s=%s\n" % (key, utils.shellEscape(value)))

		class FileObject(StringIO.StringIO):
			def __init__(self, value, name):
				StringIO.StringIO.__init__(self, value)
				self.name = name
				self.size = len(value)

		fp = FileObject(fp.getvalue(), '_config.sh')
		return fp


	def getInFiles(self):
		name = self.__class__.__name__
		def fileMap(file):
			if not os.path.isabs(file):
				path = os.path.join(self.config.baseDir, file)
			else:
				path = file
			return path
		return map(fileMap, self.config.get(name, 'input files', '').split())


	def getOutFiles(self):
		name = self.__class__.__name__
		return self.config.get(name, 'output files', '').split()


	def getJobArguments(self, job):
		return ""
