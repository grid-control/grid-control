import os, random
from python_compat import *
from grid_control import ConfigError, AbstractError, AbstractObject, QM, utils, WMS, Job
from grid_control.parameters import ParameterFactory, ParameterInfo
from time import time, localtime, strftime

class TaskModule(AbstractObject):
	# Read configuration options and init vars
	def __init__(self, config):
		self.config = config

		self.wallTime = config.getTime('jobs', 'wall time', mutable=True)
		self.cpuTime = config.getTime('jobs', 'cpu time', self.wallTime, mutable=True)
		self.nodeTimeout = config.getTime('jobs', 'node timeout', -1)

		self.cpus = config.getInt('jobs', 'cpus', 1, mutable=True)
		self.memory = config.getInt('jobs', 'memory', -1, mutable=True)

		# Compute / get task ID
		self.taskID = config.get('task', 'task id', 'GC' + md5(str(time())).hexdigest()[:12], persistent = True)
		self.taskDate = config.get('task', 'task date', strftime("%F"), persistent = True)

		self.taskVariables = {
			# Space limits
			'SCRATCH_UL': config.getInt('storage', 'scratch space used', 5000),
			'SCRATCH_LL': config.getInt('storage', 'scratch space left', 1),
			'LANDINGZONE_UL': config.getInt('storage', 'landing zone space used', 100),
			'LANDINGZONE_LL': config.getInt('storage', 'landing zone space left', 1),
		}

		# Storage setup - in case a directory is give, prepend dir specifier
		config.set('storage', 'se output pattern', 'job_@MY_JOBID@_@X@', override=False)
		self.seMinSize = config.getInt('storage', 'se min size', -1)
		self.sbInputFiles = config.getList(self.__class__.__name__, 'input files', [])
		self.sbOutputFiles = config.getList(self.__class__.__name__, 'output files', [])
		self.gzipOut = config.getBool(self.__class__.__name__, 'gzip output', True)

		self.substFiles = config.getList(self.__class__.__name__, 'subst files', [])
		self.dependencies = map(str.lower, config.getList(self.__class__.__name__, 'depends', []))

		# Get error messages from gc-run.lib comments
		self.errorDict = dict(self.updateErrorDict(utils.pathShare('gc-run.lib')))

		# Init plugin manager / parameter source
		pmName = config.get([self.__class__.__name__, 'parameters'], 'parameter factory', 'SimpleParameterFactory')
		pm = ParameterFactory.open(pmName, config, [self.__class__.__name__, 'parameters'])
		self.setupJobParameters(config, pm)
		self.source = pm.getSource(config)


	def setupJobParameters(self, config, pm):
		pass


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
			# Sandbox
			'SB_OUTPUT_FILES': str.join(' ', self.getSBOutFiles()),
			'SB_INPUT_FILES': str.join(' ', map(os.path.basename, self.getSBInFiles())),
			# Runtime
			'DOBREAK': self.nodeTimeout,
			'MY_RUNTIME': self.getCommand(),
			# Seeds and substitutions
			'SUBST_FILES': str.join(' ', map(os.path.basename, self.getSubstFiles())),
			# Task infos
			'TASK_ID': self.taskID,
			'GC_TASK_DATE': self.taskDate,
			'GC_CONF': self.config.confName,
			'GC_VERSION': utils.getVersion(),
		}
		return utils.mergeDicts([taskConfig, self.taskVariables])
	getTaskConfig = lru_cache(getTaskConfig)


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		tmp = self.source.getJobInfo(jobNum)
		return dict(map(lambda key: (key, tmp.get(key, '')), self.source.getJobKeys()))


	def getTransientVars(self):
		hx = str.join("", map(lambda x: "%02x" % x, map(random.randrange, [256]*16)))
		return {'GC_DATE': strftime("%F"), 'GC_TIMESTAMP': strftime("%s"),
			'GC_GUID': '%s-%s-%s-%s-%s' % (hx[:8], hx[8:12], hx[12:16], hx[16:20], hx[20:]),
			'RANDOM': str(random.randrange(0, 900000000))}


	def getVarNames(self):
		# Take task variables and the variables from the parameter source
		return self.getTaskConfig().keys() + list(self.source.getJobKeys())


	def getVarMapping(self):
		# Map vars: Eg. __MY_JOB__ will access $MY_JOBID
		mapping = [('DATE', 'GC_DATE'), ('TIMESTAMP', 'GC_TIMESTAMP'), ('GUID', 'GC_GUID'),
			('GC_DATE', 'GC_DATE'), ('GC_TIMESTAMP', 'GC_TIMESTAMP'), ('GC_GUID', 'GC_GUID'),
			('MY_JOBID', 'MY_JOBID'), ('MY_JOB', 'MY_JOBID'), ('CONF', 'GC_CONF')]
		envvars = self.getVarNames()
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
			self.substVars(x, 0, dict.fromkeys(['X', 'XBASE', 'XEXT', 'GC_DATE', 'GC_TIMESTAMP', 'GC_GUID', 'RANDOM'], ''))


	# Get job requirements
	def getRequirements(self, jobNum):
		return [
			(WMS.WALLTIME, self.wallTime),
			(WMS.CPUTIME, self.cpuTime),
			(WMS.MEMORY, self.memory),
			(WMS.CPUS, self.cpus)
		] + self.source.getJobInfo(jobNum)[ParameterInfo.REQS]


	def getSEInFiles(self):
		return []


	# Get files for input sandbox
	def getSBInFiles(self):
		return map(lambda p: self.config.parsePath(p, mustExist = False), self.sbInputFiles)


	# Get files for output sandbox
	def getSBOutFiles(self):
		return self.sbOutputFiles[:]


	# Get files whose content will be subject to variable substitution
	def getSubstFiles(self):
		return self.substFiles[:]


	def getCommand(self):
		raise AbstractError


	def getJobArguments(self, jobNum):
		return ''


	def getMaxJobs(self):
		return self.source.getMaxJobs()


	def getDependencies(self):
		return self.dependencies[:]


	def getDescription(self, jobNum): # (task name, job name, job type)
		return (self.taskID, self.taskID[:10] + '.' + str(jobNum), None)


	def report(self, jobNum):
		keys = filter(lambda k: k.untracked == False, self.source.getJobKeys())
		return utils.filterDict(self.source.getJobInfo(jobNum), kF = lambda k: k in keys)


	def canFinish(self):
		return True


	def canSubmit(self, jobNum):
		return self.source.canSubmit(jobNum)


	# Called on job submission
	def getSubmitInfo(self, jobNum):
		return {}


	# Intervene in job management - return None or (redoJobs, disableJobs)
	def getIntervention(self):
		return self.source.resync()

TaskModule.dynamicLoaderPath()
