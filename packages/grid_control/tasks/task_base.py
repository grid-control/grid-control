import os, random
from python_compat import lru_cache, md5
from grid_control import ConfigError, AbstractError, NamedObject, QM, utils, WMS, Job, changeInitNeeded
from grid_control.parameters import ParameterFactory, ParameterInfo
from time import time, localtime, strftime

class TaskModule(NamedObject):
	getConfigSections = NamedObject.createFunction_getConfigSections(['task'])

	# Read configuration options and init vars
	def __init__(self, config, name):
		NamedObject.__init__(self, config, name)
		initSandbox = changeInitNeeded('sandbox')

		# Task requirements
		job_config = config.addSections(['jobs']).addTags([self]) # Move this into parameter manager?
		self.wallTime = job_config.getTime('wall time', onChange = None)
		self.cpuTime = job_config.getTime('cpu time', self.wallTime, onChange = None)
		self.cpus = job_config.getInt('cpus', 1, onChange = None)
		self.memory = job_config.getInt('memory', -1, onChange = None)
		self.nodeTimeout = job_config.getTime('node timeout', -1, onChange = initSandbox)

		# Compute / get task ID
		self.taskID = config.get('task id', 'GC' + md5(str(time())).hexdigest()[:12], persistent = True)
		self.taskDate = config.get('task date', strftime('%Y-%m-%d'), persistent = True, onChange = initSandbox)
		self.taskConfigName = config.confName

		# Storage setup
		storage_config = config.addSections(['storage']).addTags([self])
		self.taskVariables = {
			# Space limits
			'SCRATCH_UL': storage_config.getInt('scratch space used', 5000, onChange = initSandbox),
			'SCRATCH_LL': storage_config.getInt('scratch space left', 1, onChange = initSandbox),
			'LANDINGZONE_UL': storage_config.getInt('landing zone space used', 100, onChange = initSandbox),
			'LANDINGZONE_LL': storage_config.getInt('landing zone space left', 1, onChange = initSandbox),
		}
		storage_config.set('se output pattern', 'job_@MY_JOBID@_@X@', override = False)
		self.seMinSize = storage_config.getInt('se min size', -1, onChange = initSandbox)

		self.sbInputFiles = config.getPaths('input files', [], onChange = initSandbox)
		self.sbOutputFiles = config.getList('output files', [], onChange = initSandbox)
		self.gzipOut = config.getBool('gzip output', True, onChange = initSandbox)

		self.substFiles = config.getList('subst files', [], onChange = initSandbox)
		self.dependencies = map(str.lower, config.getList('depends', [], onChange = initSandbox))

		# Get error messages from gc-run.lib comments
		self.errorDict = dict(self.updateErrorDict(utils.pathShare('gc-run.lib')))

		# Init plugin manager / parameter source
		pm = config.getClass('parameter factory', 'SimpleParameterFactory', cls = ParameterFactory).getInstance()
		configParam = config.addSections(['parameters']).addTags([self])
		self.setupJobParameters(configParam, pm)
		self.source = pm.getSource(configParam)


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
			'GC_CONF': self.taskConfigName,
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
		return list(self.sbInputFiles)


	# Get files for output sandbox
	def getSBOutFiles(self):
		return list(self.sbOutputFiles)


	# Get files whose content will be subject to variable substitution
	def getSubstFiles(self):
		return list(self.substFiles)


	def getCommand(self):
		raise AbstractError


	def getJobArguments(self, jobNum):
		return ''


	def getMaxJobs(self):
		return self.source.getMaxJobs()


	def getDependencies(self):
		return list(self.dependencies)


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

TaskModule.registerObject(tagName = 'task')
