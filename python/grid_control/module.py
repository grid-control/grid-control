# Generic base class for job modules
# instantiates named class instead (default is UserMod)

from grid_control import ConfigError, AbstractObject

class Module(AbstractObject):
	def __init__(self, config):
		self.config = config
		self.workDir = config.getPath('global', 'workdir')

	def init(self):
		pass

	def getInFiles(self):
		return []

	def getSoftwareMembers(self):
		return ()

	def getJobArguments(self, job):
		return "%d" % job
