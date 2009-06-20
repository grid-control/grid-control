# Generic base class for job modules
# instantiates named class instead (default is UserMod)

import os, os.path, cStringIO, StringIO, md5, gzip, cPickle, random, threading
from grid_control import ConfigError, AbstractObject, utils, WMS, Job
from time import time

class Module(AbstractObject):
	# Read configuration options and init vars
	def __init__(self, workDir, config, opts):
		self.config = config

		self.wallTime = utils.parseTime(config.get('jobs', 'wall time'))
		self.cpuTime = utils.parseTime(config.get('jobs', 'cpu time', config.get('jobs', 'wall time')))
		self.memory = config.getInt('jobs', 'memory', 512)

		self.nodeTimeout = utils.parseTime(config.get('jobs', 'node timeout', ''))

		# Set random seeds
		seedarg = config.get('jobs', 'seeds', '')
		if opts.seed != None:
			seedarg = opts.seed
		if seedarg != '':
			self.seeds = map(lambda x: int(x), seedarg.split(','))
		else:
			self.seeds = map(lambda x: random.randint(0, 10000000), range(10))
			print "Creating random seeds... ", self.seeds

		# Compute / get task ID
		self.taskID = None
		self.taskID = self.getTaskID(workDir)
		print 'Current task ID %s' % (self.taskID)

		self.dashboard = config.getBool('jobs', 'monitor job', False)
		self.username = "unknown"

		self.evtSubmit = config.getPath('events', 'on submit', '')
		self.evtStatus = config.getPath('events', 'on status', '')
		self.evtOutput = config.getPath('events', 'on output', '')

		self.sePath = config.get('storage', 'se path', '').replace('dir://', 'file://')
		self.storageInfo = {
			# Space limits (TODO: Convert the following into requirements)
			'SCRATCH_UL' :       config.getInt('storage', 'scratch space used', 5000),
			'SCRATCH_LL' :       config.getInt('storage', 'scratch space left', 1),
			'LANDINGZONE_UL':    config.getInt('storage', 'landing zone space used', 100),
			'LANDINGZONE_LL':    config.getInt('storage', 'landing zone space left', 1),
			# Storage element
			'SE_MINFILESIZE':    config.getInt('storage', 'se min size', -1),
			'SE_OUTPUT_PATTERN': config.get('storage', 'se output pattern', 'job___MY_JOB_____NICK_____X__'),
			'SE_INPUT_PATTERN':  config.get('storage', 'se input pattern', '__X__'),
		}

		self.seInputFiles = config.get('storage', 'se input files', '').split()
		self.seOutputFiles = config.get('storage', 'se output files', '').split()

		self.sbInputFiles = config.get(self.__class__.__name__, 'input files', '').split()
		self.sbOutputFiles = config.get(self.__class__.__name__, 'output files', '').split()

		if config.get('CMSSW', 'se output files', 'FAIL') != 'FAIL':
			utils.deprecated("Please specify se output files only in the [storage] section")
			self.seOutputFiles = config.get('CMSSW', 'se output files').split()
		if config.get('CMSSW', 'seeds', 'FAIL') != 'FAIL':
			utils.deprecated("Please specify seeds only in the [jobs] section")
			self.setSeed(str.join(',', config.get('CMSSW', 'seeds').split()))
		if config.get('CMSSW', 'se path', 'FAIL') != 'FAIL':
			utils.deprecated("Please specify se path only in the [storage] section")
			self.storageInfo['SE_PATH'] = config.get('CMSSW', 'se path')


	# Get persistent task id for monitoring
	def getTaskID(self, workDir):
		if self.taskID == None:
			taskfile = os.path.join(workDir, 'task.dat')
			try:
				self.taskID = utils.DictFormat(" = ").parse(open(taskfile))['task id']
			except:
				self.taskID = 'GC' + md5.md5(str(time())).hexdigest()[:12]
			tmp = { 'task id': self.taskID }
			open(taskfile, 'w').writelines(utils.DictFormat(" = ").format(tmp))
		return self.taskID


	# Get both task and job config / state dicts
	def setEventEnviron(self, jobObj, jobNum):
		tmp = {}
		tmp.update(self.getTaskConfig())
		tmp.update(self.getJobConfig(jobNum))
		tmp.update(jobObj.getAll())
		for key, value in tmp.iteritems():
			os.environ["GC_%s" % key] = str(value)


	# Called on job submission
	def onJobSubmit(self, jobObj, jobNum):
		if self.evtSubmit != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s" % (self.evtSubmit, jobNum, jobObj.id)
			threading.Thread(target = os.system, args = (params,)).start()
		return None


	# Called on job status update
	def onJobUpdate(self, jobObj, jobNum, data):
		if self.evtStatus != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s %s" % (self.evtStatus, jobNum, jobObj.id, Job.states[jobObj.state])
			threading.Thread(target = os.system, args = (params,)).start()
		return None


	# Called on job status update
	def onJobOutput(self, jobObj, jobNum, retCode):
		if self.evtOutput != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s %d" % (self.evtOutput, jobNum, jobObj.id, retCode)
			threading.Thread(target = os.system, args = (params,)).start()
		return None


	# Get environment variables for gc_config.sh
	def getTaskConfig(self):
		self.storageInfo.update({
			# SE files
			'SE_PATH': self.sePath,
			'SE_OUTPUT_FILES': str.join(' ', self.seOutputFiles),
			'SE_INPUT_FILES': str.join(' ', self.seInputFiles),
			# Sandbox
			'SB_OUTPUT_FILES': str.join(' ', self.getOutFiles()),
			'SB_INPUT_FILES': str.join(' ', map(lambda x: utils.shellEscape(os.path.basename(x)), self.getInFiles())),
			# Runtime
			'DOBREAK': self.nodeTimeout,
			'MY_RUNTIME': self.getCommand(),
			# Seeds
			'SEEDS': str.join(' ', map(str, self.seeds)),
			# Task infos
			'TASK_ID': self.taskID,
			'TASK_USER': self.username,
			'DASHBOARD': ('no', 'yes')[self.dashboard]
		})
		return self.storageInfo


	# Get job dependent environment variables
	def getJobConfig(self, jobNum):
		return {
			'MY_JOBID': jobNum
		}


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
		return map(fileMap, self.sbInputFiles)


	# Get files for output sandbox
	def getOutFiles(self):
		return self.sbOutputFiles


	def getCommand(self):
		raise AbstractError


	def getJobArguments(self, job):
		raise AbstractError


	def getMaxJobs(self):
		return None
