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

		# Set random seeds (args override config, explicit seeds override generation via nseeds)
		self.seeds = map(int, config.getList('jobs', 'seeds', []))
		self.nseeds = config.getInt('jobs', 'nseeds', 10)
		if len(self.seeds) == 0:
			# args specified => gen seeds
			if 'seeds' in taskInfo:
				self.seeds = map(int, str(taskInfo['seeds']).split())
			else:
				self.seeds = map(lambda x: random.randint(0, 10000000), range(self.nseeds))
			utils.vprint('Creating random seeds... %s' % self.seeds, -1, once = True)

		# Write task info file
		taskInfo.write({'task id': self.taskID, 'seeds': str.join(' ', map(str, self.seeds))})

		self.taskVariables = {
			# Space limits
			'SCRATCH_UL': config.getInt('storage', 'scratch space used', 5000),
			'SCRATCH_LL': config.getInt('storage', 'scratch space left', 1),
			'LANDINGZONE_UL': config.getInt('storage', 'landing zone space used', 100),
			'LANDINGZONE_LL': config.getInt('storage', 'landing zone space left', 1),
		}

		# Storage setup - in case a directory is give, prepend dir specifier
		normSEPaths = lambda seList: map(lambda x: QM(x[0] == '/', 'dir:///%s' % x.lstrip('/'), x), seList)
		self.sePaths = normSEPaths(config.getList('storage', 'se path', [], noVar=True))
		self.seMinSize = config.getInt('storage', 'se min size', -1)

		self.seInputPaths = normSEPaths(config.getList('storage', 'se input path', self.sePaths, noVar=True))
		self.seInputFiles = config.getList('storage', 'se input files', [], noVar=False)
		self.seInputPattern = config.get('storage', 'se input pattern', '@X@', noVar=False)
		self.seInputTimeout = utils.parseTime(config.get('storage', 'se input timeout', '0:30'))
		self.seOutputPaths = normSEPaths(config.getList('storage', 'se output path', self.sePaths, noVar=True))
		self.seOutputFiles = config.getList('storage', 'se output files', [], noVar=False)
		self.seOutputPattern = config.get('storage', 'se output pattern', '@NICK@job_@MY_JOBID@_@X@', noVar=False)
		self.seOutputTimeout = utils.parseTime(config.get('storage', 'se output timeout', '2:00'))

		self.sbInputFiles = config.getList(self.__class__.__name__, 'input files', [])
		self.sbOutputFiles = config.getList(self.__class__.__name__, 'output files', [])
		self.gzipOut = config.getBool(self.__class__.__name__, 'gzip output', True)

		# Define constants for job
		self.constants = dict(map(lambda var: (var.upper(), config.get('constants', var, '').strip()),
			config.getOptions('constants')))
		for var in map(str.strip, config.getList(self.__class__.__name__, 'constants', [])):
			self.constants[var] = config.get(self.__class__.__name__, var, '').strip()
		self.substFiles = config.getList(self.__class__.__name__, 'subst files', [])

		self.dependencies = map(str.lower, config.getList(self.__class__.__name__, 'depends', []))
		if True in map(lambda x: not x.startswith('dir'), self.sePaths):
			self.dependencies.append('glite')

		# Get error messages from gc-run.lib comments
		self.errorDict = dict(self.updateErrorDict(utils.pathShare('gc-run.lib')))


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
			'SE_MINFILESIZE': self.seMinSize,
			'SE_INPUT_PATH': str.join(' ', self.seInputPaths),
			'SE_OUTPUT_PATH': str.join(' ', self.seOutputPaths),
			'SE_OUTPUT_FILES': str.join(' ', self.seOutputFiles),
			'SE_INPUT_FILES': str.join(' ', self.seInputFiles),
			'SE_OUTPUT_PATTERN': self.seOutputPattern,
			'SE_INPUT_PATTERN': self.seInputPattern,
			'SE_INPUT_TIMEOUT': self.seInputTimeout,
			'SE_OUTPUT_TIMEOUT': self.seOutputTimeout,
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
		return dict([('MY_JOBID', jobNum), ('JOB_RANDOM', random.randint(1e6, 1e7-1))] + tmp)


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
		subst = lambda x: utils.replaceDict(x, allVars, self.getVarMapping().items() + zip(addDict, addDict))
		result = subst(subst(str(inp)))
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


	def getDescription(self, jobNum): # (task name, job name, job type)
		return (self.taskID, self.taskID[:10] + '.' + str(jobNum), '')


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
