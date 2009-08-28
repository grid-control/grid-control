# Generic base class for job modules
# instantiates named class instead (default is UserMod)

import os, md5, random, threading
from grid_control import ConfigError, AbstractObject, utils, WMS, Job
from time import time, localtime, strftime
from DashboardAPI import DashboardAPI

class Module(AbstractObject):
	# Read configuration options and init vars
	def __init__(self, config, proxy):
		self.config = config
		self.proxy = proxy
		self.hookenv = None

		wallTime = config.get('jobs', 'wall time', volatile=True)
		self.wallTime = utils.parseTime(wallTime)
		self.cpuTime = utils.parseTime(config.get('jobs', 'cpu time', wallTime, volatile=True))
		self.nodeTimeout = utils.parseTime(config.get('jobs', 'node timeout', ''))

		self.memory = config.getInt('jobs', 'memory', 512, volatile=True)

		# Try to read task info file
		try:
			taskInfoFile = os.path.join(self.config.workDir, 'task.dat')
			taskInfo = utils.DictFormat(" = ").parse(open(taskInfoFile))
		except:
			taskInfo = {}

		# Compute / get task ID
		self.taskID = taskInfo.get('task id', 'GC' + md5.md5(str(time())).hexdigest()[:12])
		utils.vprint('Current task ID: %s' % self.taskID, -1, once = True)

		# Set random seeds (args override config)
		seedarg = config.get('jobs', 'seeds', '')
		if config.opts.seed != None:
			seedarg = config.opts.seed.rstrip('S')
		if seedarg != '':
			self.seeds = map(int, seedarg.split(','))
		else:
			# args specified => gen seeds
			if taskInfo.has_key('seeds') and (config.opts.seed == None):
				self.seeds = map(int, taskInfo['seeds'].split())
			else:
				self.seeds = map(lambda x: random.randint(0, 10000000), range(10))
				print "Creating random seeds...", self.seeds

		# Write task info file
		tmp = { 'task id': self.taskID, 'seeds': str.join(' ', map(str, self.seeds)) }
		open(taskInfoFile, 'w').writelines(utils.DictFormat(" = ").format(tmp))

		self.dashboard = config.getBool('jobs', 'monitor job', False, volatile=True)

		self.evtSubmit = config.getPath('events', 'on submit', '', volatile=True)
		self.evtStatus = config.getPath('events', 'on status', '', volatile=True)
		self.evtOutput = config.getPath('events', 'on output', '', volatile=True)

		self.seSDUpperLimit = config.getInt('storage', 'scratch space used', 5000)
		self.seSDLowerLimit = config.getInt('storage', 'scratch space left', 1)
		self.seLZUpperLimit = config.getInt('storage', 'landing zone space used', 100)
		self.seLZLowerLimit = config.getInt('storage', 'landing zone space left', 1)

		# Storage setup
		self.sePath = config.get('storage', 'se path', '').strip()
		self.seMinSize = config.getInt('storage', 'se min size', -1)

		self.seInputFiles = config.get('storage', 'se input files', '').split()
		self.seInputPattern = config.get('storage', 'se input pattern', '__X__')
		self.seOutputFiles = config.get('storage', 'se output files', '').split()
		self.seOutputPattern = config.get('storage', 'se output pattern', '@NICK@job_@MY_JOBID@_@X@')

		self.sbInputFiles = config.get(self.__class__.__name__, 'input files', '').split()
		self.sbOutputFiles = config.get(self.__class__.__name__, 'output files', '').split()
		self.substFiles = config.get(self.__class__.__name__, 'subst files', '').split()

		self.dependencies = config.get(self.__class__.__name__, 'depends', '').lower().split()
		if self.sePath and not self.sePath.startswith('dir'):
			self.dependencies.append('glite')

		if config.get('CMSSW', 'se output files', 'DEPRECATED') != 'DEPRECATED':
			utils.deprecated("Please specify se output files only in the [storage] section")
			self.seOutputFiles = config.get('CMSSW', 'se output files').split()
		if config.get('CMSSW', 'seeds', 'DEPRECATED') != 'DEPRECATED':
			utils.deprecated("Please specify seeds only in the [jobs] section")
			self.setSeed(str.join(',', config.get('CMSSW', 'seeds').split()))
		if config.get('CMSSW', 'se path', 'DEPRECATED') != 'DEPRECATED':
			utils.deprecated("Please specify se path only in the [storage] section")
			self.sePath = config.get('CMSSW', 'se path')


	# Get both task and job config / state dicts
	def setEventEnviron(self, jobObj, jobNum):
		tmp = {}
		tmp.update(self.getTaskConfig())
		tmp.update(self.getJobConfig(jobNum))
		tmp.update(jobObj.getAll())
		tmp.update({'WORKDIR': self.config.workDir})
		if self.hookenv:
			self.hookenv(tmp, jobNum)
		for key, value in tmp.iteritems():
			os.environ["GC_%s" % key] = str(value)


	def publishToDashboard(self, jobObj, jobNum, usermsg):
		if self.dashboard:
			dashId = "%s_%s" % (jobNum, jobObj.wmsId)
			dashboard = DashboardAPI(self.taskID, dashId)
			msg = { "taskId": self.taskID, "jobId": dashId, "sid": dashId }
			msg = dict(filter(lambda (x,y): y != None, reduce(lambda x,y: x+y, map(dict.items, [msg] + usermsg))))
			dashboard.publish(**msg)


	# Called on job submission
	def onJobSubmit(self, jobObj, jobNum, dbmessage = [{}]):
		if self.evtSubmit != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s" % (self.evtSubmit, jobNum, jobObj.wmsId)
			threading.Thread(target = os.system, args = (params,)).start()

		threading.Thread(target = self.publishToDashboard, args = (jobObj, jobNum, [{
			"tool": "grid-control", "GridName": self.proxy.getUsername(), "scheduler": "gLite",
			"taskType": "analysis", "vo": self.proxy.getVO(), "user": os.environ['LOGNAME'] }] +
			[dict.fromkeys(["application", "exe"], "shellscript")] + dbmessage,)).start()


	# Called on job status update
	def onJobUpdate(self, jobObj, jobNum, data, dbmessage = [{}]):
		if self.evtStatus != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s %s" % (self.evtStatus, jobNum, jobObj.wmsId, Job.states[jobObj.state])
			threading.Thread(target = os.system, args = (params,)).start()

		threading.Thread(target = self.publishToDashboard, args = (jobObj, jobNum, [{
			"StatusValue": data.get('status', 'pending').upper(),
			"StatusValueReason": data.get('reason', data.get('status', 'pending')).upper(),
			"StatusEnterTime": data.get('timestamp', strftime("%Y-%m-%d_%H:%M:%S", localtime())),
			"StatusDestination": data.get('dest', "") }] + dbmessage,)).start()


	# Called on job status update
	def onJobOutput(self, jobObj, jobNum, retCode):
		if self.evtOutput != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s %d" % (self.evtOutput, jobNum, jobObj.wmsId, retCode)
			threading.Thread(target = os.system, args = (params,)).start()


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		return {
			# Space limits
			'SCRATCH_UL' : self.seSDUpperLimit,
			'SCRATCH_LL' : self.seSDLowerLimit,
			'LANDINGZONE_UL': self.seLZUpperLimit,
			'LANDINGZONE_LL': self.seLZLowerLimit,
			# Storage element
			'SE_PATH': self.sePath,
			'SE_MINFILESIZE': self.seMinSize,
			'SE_OUTPUT_FILES': str.join(' ', self.seOutputFiles),
			'SE_INPUT_FILES': str.join(' ', self.seInputFiles),
			'SE_OUTPUT_PATTERN': self.seOutputPattern,
			'SE_INPUT_PATTERN': self.seInputPattern,
			# Sandbox
			'SB_OUTPUT_FILES': str.join(' ', self.getOutFiles()),
			'SB_INPUT_FILES': str.join(' ', map(lambda x: utils.shellEscape(os.path.basename(x)), self.getInFiles())),
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
			'TASK_USER': self.proxy.getUsername(),
			'DASHBOARD': ('no', 'yes')[self.dashboard],
			'DB_EXEC': 'shellscript'
		}


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		tmp = [('MY_JOBID', jobNum)]
		tmp += map(lambda (x, seed): ("SEED_%d" % x, seed + jobNum), enumerate(self.seeds))
		return dict(tmp)


	def getVarMapping(self):
		# Take task variables and just the variables from the first job
		envvars = self.getTaskConfig().keys() + self.getJobConfig(0).keys()

		# Map vars: Eg. __MY_JOB__ will access $MY_JOBID
		mapping = [('DATE', 'MYDATE'), ('TIMESTAMP', 'MYTIMESTAMP'),
			('MY_JOB', 'MY_JOBID'), ('CONF', 'GC_CONF')]
		mapping += zip(envvars, envvars)
		return dict(mapping)


	# Get job requirements
	def getRequirements(self, jobNum):
		return [
			(WMS.WALLTIME, self.wallTime),
			(WMS.CPUTIME, self.cpuTime),
			(WMS.MEMORY, self.memory)
		]


	# Get files for input sandbox
	def getInFiles(self):
		def fileMap(file):
			if not os.path.isabs(file):
				path = os.path.join(self.config.baseDir, file)
			else:
				path = file
			return path
		files = map(fileMap, self.sbInputFiles)
		if self.dashboard:
			for file in ('DashboardAPI.py', 'Logger.py', 'ProcInfo.py', 'apmon.py', 'report.py'):
				files.append(utils.atRoot('python/DashboardAPI', file))
		return files


	# Get files for output sandbox
	def getOutFiles(self):
		return self.sbOutputFiles


	# Get files whose content will be subject to variable substitution
	def getSubstFiles(self):
		return self.substFiles


	def getCommand(self):
		raise AbstractError


	def getJobArguments(self, jobNum):
		raise AbstractError


	def getMaxJobs(self):
		return None


	def getDependencies(self):
		return self.dependencies


	def report(self, jobNum):
		return {" ": "All jobs"}
