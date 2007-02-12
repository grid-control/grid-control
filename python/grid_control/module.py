# Generic base class for job modules
# instantiates named class instead (default is UserMod)

from grid_control import ConfigError, AbstractObject, WMS

class Module(AbstractObject):
	def __init__(self, config):
		self.config = config
		self.workDir = config.getPath('global', 'workdir')
		wallTime = config.getInt('jobs', 'wall time') * 60 * 60
		self.requirements = [ (WMS.WALLTIME, wallTime) ]

	def init(self):
		pass

	def getInFiles(self):
		return []

	def getRequirements(self):
		return self.requirements

	def getJobArguments(self, job):
		return "%d" % job
