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

		# TODO: Convert the following into requirements
		try:
			self.seInputFiles = config.get('storage', 'se input files', '').split()
		except:
			self.seInputFiles = ""

		try:
			self.seInputPattern = config.get('storage', 'se input pattern', '')
		except:
			self.seInputPattern = "__X__"

		try:
			self.seOutputFiles = config.get('storage', 'se output files', '').split()
		except:
			# TODO: remove backwards compatibility
			try:
				self.seOutputFiles = config.get('CMSSW', 'se output files', '').split()
			except:
				self.seOutputFiles = ""

		try:
			self.seOutputPattern = config.get('storage', 'se output pattern', '')
		except:
			self.seOutputPattern = "job___MY_JOB_____X__"

		try:
			self.seMinSize = config.getInt('storage', 'se min size', '')
		except:
			self.seMinSize = -1

		try:
			self.seSDUpperLimit = config.getInt('storage', 'scratch space used', '')
		except:
			self.seSDUpperLimit = 5000

		try:
			self.seSDLowerLimit = config.getInt('storage', 'scratch space left', '')
		except:
			self.seSDLowerLimit = 1000

		try:
			self.seLZUpperLimit = config.getInt('storage', 'landing zone space used', '')
		except:
			self.seLZUpperLimit = 100

		try:
			self.seLZLowerLimit = config.getInt('storage', 'landing zone space left', '')
		except:
			self.seLZLowerLimit = 50

		try:
			self.sePath = config.get('storage', 'se path')
		except:
			# TODO: remove backwards compatibility
			try:
				self.sePath = config.get('CMSSW', 'se path')
			except:
				self.sePath = ""			
			self.sePath = ""			


	def getConfig(self):
		return {
			'SE_OUTPUT_FILES': str.join(' ', self.seOutputFiles),
			'SE_INPUT_FILES': str.join(' ', self.seInputFiles),
			'SE_OUTPUT_PATTERN': self.seOutputPattern,
			'SE_INPUT_PATTERN': self.seInputPattern,
			'SE_MINFILESIZE': str(self.seMinSize),
			'SE_PATH': self.sePath,
			'SCRATCH_UL': str(self.seSDUpperLimit),
			'SCRATCH_LL': str(self.seSDLowerLimit),
			'LANDINGZONE_UL': str(self.seLZUpperLimit),
			'LANDINGZONE_LL': str(self.seLZLowerLimit),
		}


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


	def getMaxJobs(self):
		return None
