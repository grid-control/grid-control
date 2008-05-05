# Generic base class for job modules
# instantiates named class instead (default is UserMod)

import os.path, cStringIO, StringIO
from grid_control import ConfigError, AbstractObject, utils, WMS

class Module(AbstractObject):
	# Read configuration options and init vars
	def __init__(self, config, init):
		self.config = config
		self.workDir = config.getPath('global', 'workdir')
		self.wallTime = config.getInt('jobs', 'wall time') * 60 * 60
		self.cpuTime = config.getInt('jobs', 'cpu time', 10 * 60)
		self.memory = config.getInt('jobs', 'memory', 512)

		# TODO: Convert the following into requirements
		self.seInputFiles = config.get('storage', 'se input files', '').split()
		self.seInputPattern = config.get('storage', 'se input pattern', '__X__')

		try:
			self.seOutputFiles = config.get('storage', 'se output files').split()
		except:
			# TODO: remove backwards compatibility
			self.seOutputFiles = config.get('CMSSW', 'se output files', '').split()
		self.seOutputPattern = config.get('storage', 'se output pattern', 'job___MY_JOB_____X__')

		self.seMinSize = config.getInt('storage', 'se min size', -1)

		self.seSDUpperLimit = config.getInt('storage', 'scratch space used', 5000)
		self.seSDLowerLimit = config.getInt('storage', 'scratch space left', 1000)
		self.seLZUpperLimit = config.getInt('storage', 'landing zone space used', 100)
		self.seLZLowerLimit = config.getInt('storage', 'landing zone space left', 50)

		try:
			self.sePath = config.get('storage', 'se path')
		except:
			# TODO: remove backwards compatibility
			self.sePath = config.get('CMSSW', 'se path', '')


	# Get environment variables for _config.sh
	def getConfig(self):
		return {
			# Space limits
			'SCRATCH_UL': str(self.seSDUpperLimit),
			'SCRATCH_LL': str(self.seSDLowerLimit),
			'LANDINGZONE_UL': str(self.seLZUpperLimit),
			'LANDINGZONE_LL': str(self.seLZLowerLimit),
			# Storage element
			'SE_PATH': self.sePath,
			'SE_MINFILESIZE': str(self.seMinSize),
			'SE_OUTPUT_FILES': str.join(' ', self.seOutputFiles),
			'SE_INPUT_FILES': str.join(' ', self.seInputFiles),
			'SE_OUTPUT_PATTERN': self.seOutputPattern,
			'SE_INPUT_PATTERN': self.seInputPattern,
			# TODO: remove backwards compatibility
			'MY_OUT': str.join(' ', self.getOutFiles()),
			# Sandbox
			'SB_OUTPUT_FILES': str.join(' ', self.getOutFiles()),
			'SB_INPUT_FILES': str.join(' ', map(lambda x: utils.shellEscape(os.path.basename(x)), self.getInFiles())),
			# Runtime
			'MY_RUNTIME': self.getCommand()
		}


	# Create _config.sh from module config
	def makeConfig(self):
		data = self.getConfig()

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


	# Get job requirements
	def getRequirements(self, job):
		return [
			(WMS.WALLTIME, self.wallTime),
			(WMS.CPUTIME, self.cpuTime),
			(WMS.MEMORY, self.memory)
		]


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


	def getMaxJobs(self):
		return None
