# Generic base class for job modules
# instantiates named class instead (default is UserMod)

import cStringIO, StringIO
from grid_control import ConfigError, AbstractObject, WMS

class Module(AbstractObject):
	def __init__(self, config, init):
		self.config = config
		self.workDir = config.getPath('global', 'workdir')
		wallTime = config.getInt('jobs', 'wall time') * 60 * 60
		self.requirements = [ (WMS.WALLTIME, wallTime) ]


	def getConfig(self):
		fp = cStringIO.StringIO()
		self.makeConfig(fp)
		fp.write('MY_RUNTIME="./cmssw.sh \\"\\$@\\""\n');
		fp.write('MY_OUT="%s"' % str.join(' ', self.getOutFiles()))

		class FileObject(StringIO.StringIO):
			def __init__(self, value, name):
				StringIO.StringIO.__init__(self, value)
				self.name = name
				self.size = len(value)

		fp = FileObject(fp.getvalue(), 'config.sh')
		return fp


	def makeConfig(self, fp):
		pass


	def getInFiles(self):
		return []


	def getOutFiles(self):
		return []


	def getRequirements(self):
		return self.requirements


	def getJobArguments(self, job):
		return "%d" % job
