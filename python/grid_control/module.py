import os, random
from python_compat import *
from grid_control import ConfigError, AbstractError, AbstractObject, QM, utils, WMS, Job
from time import time, localtime, strftime

class Module(AbstractObject):
	# Read configuration options and init vars
	def __init__(self, config):
		self.config = config

		wallTime = config.get('jobs', 'wall time', volatile=True)
		self.wallTime = utils.parseTime(wallTime)
		self.cpuTime = utils.parseTime(config.get('jobs', 'cpu time', wallTime, volatile=True))
		self.nodeTimeout = utils.parseTime(config.get('jobs', 'node timeout', ''))

		self.cpus = config.getInt('jobs', 'cpus', 1, volatile=True)
		self.memory = config.getInt('jobs', 'memory', -1, volatile=True)

		# Try to read task info file
		taskInfo = utils.PersistentDict(os.path.join(config.workDir, 'task.dat'), ' = ')

		# Compute / get task ID
		self.taskID = taskInfo.get('task id', 'GC' + md5(str(time())).hexdigest()[:12])
		utils.vprint('Current task ID: %s' % self.taskID, -1, once = True)

		# Set random seeds (args override config)
		self.seeds = map(int, utils.parseList(config.get('jobs', 'seeds', '')))
		if len(self.seeds) == 0:
			# args specified => gen seeds
			if 'seeds' in taskInfo:
				self.seeds = map(int, str(taskInfo['seeds']).split())
			else:
				self.seeds = map(lambda x: random.randint(0, 10000000), range(10))
				utils.vprint('Creating random seeds... %s' % self.seeds, -1, once = True)

		# Write task info file
		taskInfo.write({'task id': self.taskID, 'seeds': str.join(' ', map(str, self.seeds))})

		# Storage setup - in case a directory is give, prepend dir specifier
		self.sePaths = utils.parseList(config.get('storage', 'se path', '', noVar=True), None, onEmpty = [])
		self.sePaths = map(lambda x: QM(x[0] == '/', 'dir:///%s' % x.lstrip('/'), x), self.sePaths)
		self.seMinSize = config.getInt('storage', 'se min size', -1)

		self.taskVariables = {
			# Space limits
			'SCRATCH_UL' : config.getInt('storage', 'scratch space used', 5000),
			'SCRATCH_LL' : config.getInt('storage', 'scratch space left', 1),
			'LANDINGZONE_UL': config.getInt('storage', 'landing zone space used', 100),
			'LANDINGZONE_LL': config.getInt('storage', 'landing zone space left', 1),
		}

		self.seInputFiles = config.get('storage', 'se input files', '', noVar=False).split()
		self.seInputPattern = config.get('storage', 'se input pattern', '@X@', noVar=False)
		self.seOutputFiles = config.get('storage', 'se output files', '', noVar=False).split()
		self.seOutputPattern = config.get('storage', 'se output pattern', '@NICK@job_@MY_JOBID@_@X@', noVar=False)

		self.sbInputFiles = config.get(self.__class__.__name__, 'input files', '').split()
		self.sbOutputFiles = config.get(self.__class__.__name__, 'output files', '').split()
		self.gzipOut = config.getBool(self.__class__.__name__, 'gzip output', True)

		# Define constants for job
		self.constants = {}
		if config.parser.has_section('constants'):
			for var in config.parser.options('constants'):
				self.constants[var] = config.get('constants', var, '').strip()
		for var in map(str.strip, config.get(self.__class__.__name__, 'constants', '').split()):
			self.constants[var] = config.get(self.__class__.__name__, var, '').strip()
		self.substFiles = config.get(self.__class__.__name__, 'subst files', '').split()

		self.dependencies = config.get(self.__class__.__name__, 'depends', '').lower().split()
		if True in map(lambda x: not x.startswith('dir'), self.sePaths):
			self.dependencies.append('glite')

		# Get error messages from gc-run.lib comments
		self.errorDict = dict(self.updateErrorDict(utils.pathGC('share', 'gc-run.lib')))


	# Read comments with error codes at the beginning of file
	def updateErrorDict(self, fileName):
		for line in filter(lambda x: x.startswith('#'), open(fileName, 'r').readlines()):
			try:
				transform = lambda (x, y): (int(x.strip('# ')), y)
				yield transform(map(str.strip, line.split(' - ', 1)))
			except:
				pass


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		taskConfig = {
			# Storage element
			'SE_PATH': str.join(' ', self.sePaths),
			'SE_MINFILESIZE': self.seMinSize,
			'SE_OUTPUT_FILES': str.join(' ', self.seOutputFiles),
			'SE_INPUT_FILES': str.join(' ', self.seInputFiles),
			'SE_OUTPUT_PATTERN': self.seOutputPattern,
			'SE_INPUT_PATTERN': self.seInputPattern,
			# Sandbox
			'SB_OUTPUT_FILES': str.join(' ', self.getOutFiles()),
			'SB_INPUT_FILES': str.join(' ', map(os.path.basename, self.getInFiles())),
			# Runtime
			'DOBREAK': self.nodeTimeout,
			'MY_RUNTIME': self.getCommand(),
			'GC_DEPFILES': str.join(' ', self.getDependencies()),
			# Seeds and substitutions
			'SEEDS': str.join(' ', map(str, self.seeds)),
			'SUBST_FILES': str.join(' ', map(os.path.basename, self.getSubstFiles())),
			# Task infos
			'TASK_ID': self.taskID,
			'GC_CONF': self.config.confName,
			'GC_VERSION': utils.getVersion(),
		}
		return utils.mergeDicts([taskConfig, self.taskVariables, self.constants])
	getTaskConfig = lru_cache(getTaskConfig)


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		tmp = map(lambda (x, seed): ('SEED_%d' % x, seed + jobNum), enumerate(self.seeds))
		return dict([('MY_JOBID', jobNum)] + tmp)


	def getTransientVars(self):
		hx = str.join("", map(lambda x: "%02x" % x, map(random.randrange, [256]*16)))
		return {'MYDATE': strftime("%F"), 'MYTIMESTAMP': strftime("%s"),
			'MYGUID': '%s-%s-%s-%s-%s' % (hx[:8], hx[8:12], hx[12:16], hx[16:20], hx[20:]),
			'RANDOM': str(random.randrange(0, 900000000))}


	def getVarMapping(self):
		# Take task variables and just the variables from the first job
		envvars = self.getTaskConfig().keys() + self.getJobConfig(0).keys()
		# Map vars: Eg. __MY_JOB__ will access $MY_JOBID
		mapping = [('DATE', 'MYDATE'), ('TIMESTAMP', 'MYTIMESTAMP'),
			('MY_JOB', 'MY_JOBID'), ('CONF', 'GC_CONF'), ('GUID', 'MYGUID')]
		return dict(mapping + zip(envvars, envvars))


	def substVars(self, inp, jobNum = None, addDict = {}, check = True):
		allVars = utils.mergeDicts([addDict, self.getTaskConfig()])
		if jobNum != None:
			allVars.update(self.getJobConfig(jobNum))
		def substInternal(result):
			for (virtual, real) in self.getVarMapping().items() + zip(addDict, addDict):
				for delim in ['@', '__']:
					result = result.replace(delim + virtual + delim, str(allVars.get(real, '')))
			return result
		result = substInternal(substInternal(str(inp)))
		return utils.checkVar(result, "'%s' contains invalid variable specifiers: '%s'" % (inp, result), check)


	def validateVariables(self):
		for x in self.getTaskConfig().values() + self.getJobConfig(0).values():
			self.substVars(x, 0, dict.fromkeys(['X', 'XBASE', 'XEXT', 'MYDATE', 'MYTIMESTAMP', 'RANDOM'], ''))


	# Get job requirements
	def getRequirements(self, jobNum):
		return [
			(WMS.WALLTIME, self.wallTime),
			(WMS.CPUTIME, self.cpuTime),
			(WMS.MEMORY, self.memory),
			(WMS.CPUS, self.cpus)
		]


	# Get files for input sandbox
	def getInFiles(self):
		return map(lambda p: utils.resolvePath(p, [self.config.baseDir], False), self.sbInputFiles)


	# Get files for output sandbox
	def getOutFiles(self):
		return self.sbOutputFiles[:]


	# Get files whose content will be subject to variable substitution
	def getSubstFiles(self):
		return self.substFiles[:]


	def getCommand(self):
		raise AbstractError


	def getJobArguments(self, jobNum):
		return ''


	def getMaxJobs(self):
		return None


	def getDependencies(self):
		return self.dependencies[:]


	def getTaskType(self):
		return ''


	def report(self, jobNum):
		return {' ': 'All jobs'}


	def onTaskFinish(self):
		return True


	def canSubmit(self, jobNum):
		return True


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		return {}


	# Intervene in job management
	def getIntervention(self):
		return None

Module.dynamicLoaderPath(['grid_control.modules'])
