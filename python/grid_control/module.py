# Generic base class for job modules
# instantiates named class instead (default is UserMod)

import cStringIO, StringIO
from grid_control import ConfigError, AbstractObject, utils, WMS

class Module(AbstractObject):
	def __init__(self, config, init):
		self.config = config
		self.workDir = config.getPath('global', 'workdir')
		wallTime = config.getInt('jobs', 'wall time') * 60 * 60
		self.requirements = [ (WMS.WALLTIME, wallTime) ]


	def getConfig(self):
		return {}


	def getRequirements(self):
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
		return self.config.get(name, 'input files', '').split()


	def getOutFiles(self):
		name = self.__class__.__name__
		return self.config.get(name, 'output files', '').split()


	def getJobArguments(self, job):
		return ""
