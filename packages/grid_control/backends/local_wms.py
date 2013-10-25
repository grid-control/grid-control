import sys, os, tempfile, shutil, time, random, glob
from grid_control import AbstractObject, AbstractError, ConfigError, Job, utils
from wms import WMS, BasicWMS
from broker import Broker

class LocalWMS(BasicWMS):
	def __init__(self, config, wmsName, submitExec, statusExec, cancelExec):
		config.set('local', 'broker', 'RandomBroker', override = False)
		(self.submitExec, self.statusExec, self.cancelExec) = (submitExec, statusExec, cancelExec)
		BasicWMS.__init__(self, config, wmsName, 'local')

		self.brokerSite = self._createBroker('site broker', 'UserBroker', 'sites', 'sites', self.getNodes)
		self.brokerQueue = self._createBroker('queue broker', 'UserBroker', 'queue', 'queues', self.getQueues)

		self.sandCache = []
		self.sandPath = config.getPath('local', 'sandbox path', os.path.join(config.workDir, 'sandbox'), mustExist = False)
		self.scratchPath = config.getList('local', 'scratch path', ['TMPDIR', '/tmp'], mutable=True)


	def getTimings(self):
		return (20, 5) # Wait 20 seconds between cycles and 5 seconds between steps


	# Check status of jobs and yield (jobNum, wmsID, status, other data)
	def checkJobs(self, ids):
		if not len(ids):
			raise StopIteration

		activity = utils.ActivityLog('checking job status')
		proc = utils.LoggedProcess(self.statusExec, self.getCheckArguments(self._getRawIDs(ids)))

		tmp = {}
		for data in self.parseStatus(proc.iter()):
			wmsId = self._createId(data['id'])
			tmp[wmsId] = (wmsId, self.parseJobState(data['status']), data)

		for wmsId, jobNum in ids:
			if wmsId not in tmp:
				yield (jobNum, wmsId, Job.DONE, {})
			else:
				yield tuple([jobNum] + list(tmp[wmsId]))

		retCode = proc.wait()
		del activity

		if retCode != 0:
			for line in proc.getError().splitlines():
				if not self.unknownID() in line:
					utils.eprint(line)


	def cancelJobs(self, ids):
		if not len(ids):
			raise StopIteration

		activity = utils.ActivityLog('cancelling jobs')
		proc = utils.LoggedProcess(self.cancelExec, self.getCancelArguments(self._getRawIDs(ids)))
		if proc.wait() != 0:
			for line in proc.getError().splitlines():
				if not self.unknownID() in line:
					utils.eprint(line.strip())
		del activity

		activity = utils.ActivityLog('waiting for jobs to finish')
		time.sleep(5)
		for wmsId, jobNum in ids:
			path = self._getSandbox(wmsId)
			if path == None:
				utils.eprint('Sandbox for job %d with wmsId "%s" could not be found' % (jobNum, wmsId))
				continue
			try:
				shutil.rmtree(path)
			except:
				raise RuntimeError('Sandbox for job %d with wmsId "%s" could not be deleted' % (jobNum, wmsId))
			yield (jobNum, wmsId)
		del activity


	def _getSandbox(self, wmsId):
		# Speed up function by caching result of listdir
		def searchSandbox(source):
			for path in map(lambda sbox: os.path.join(self.sandPath, sbox), source):
				if os.path.exists(os.path.join(path, wmsId)):
					return path
		result = searchSandbox(self.sandCache)
		if result:
			return result
		oldCache = self.sandCache[:]
		self.sandCache = filter(lambda x: os.path.isdir(os.path.join(self.sandPath, x)), os.listdir(self.sandPath))
		return searchSandbox(filter(lambda x: x not in oldCache, self.sandCache))


	# Submit job and yield (jobNum, WMS ID, other data)
	def _submitJob(self, jobNum, module):
		activity = utils.ActivityLog('submitting jobs')

		try:
			if not os.path.exists(self.sandPath):
				os.mkdir(self.sandPath)
			sandbox = tempfile.mkdtemp('', '%s.%04d.' % (module.taskID, jobNum), self.sandPath)
		except:
			raise RuntimeError('Unable to create sandbox directory "%s"!' % sandbox)
		sbPrefix = sandbox.replace(self.sandPath, '').lstrip('/')
		self.smSBIn.doTransfer(map(lambda (d, s, t): (d, s, os.path.join(sbPrefix, t)), self._getSandboxFilesIn(module)))

		cfgPath = os.path.join(sandbox, '_jobconfig.sh')
		self._writeJobConfig(cfgPath, jobNum, module, {'GC_SANDBOX': sandbox,
			'GC_SCRATCH': str.join(' ', self.scratchPath)})
		reqs = self.brokerSite.brokerAdd(module.getRequirements(jobNum), WMS.SITES)
		reqs = dict(self.brokerQueue.brokerAdd(reqs, WMS.QUEUES))

		(stdout, stderr) = (os.path.join(sandbox, 'gc.stdout'), os.path.join(sandbox, 'gc.stderr'))
		(taskName, jobName, jobType) = module.getDescription(jobNum)
		proc = utils.LoggedProcess(self.submitExec, '%s "%s" %s' % (
			self.getSubmitArguments(jobNum, jobName, reqs, sandbox, stdout, stderr),
			utils.pathShare('gc-local.sh'), self.getJobArguments(jobNum, sandbox)))
		retCode = proc.wait()
		wmsIdText = proc.getOutput().strip().strip('\n')
		try:
			wmsId = self.parseSubmitOutput(wmsIdText)
		except:
			wmsId = None

		del activity

		if retCode != 0:
			utils.eprint('WARNING: %s failed:' % self.submitExec)
		elif wmsId == None:
			utils.eprint('WARNING: %s did not yield job id:\n%s' % (self.submitExec, wmsIdText))
		if wmsId:
			wmsId = self._createId(wmsId)
			open(os.path.join(sandbox, wmsId), 'w')
		else:
			proc.logError(self.errorLog)
		return (jobNum, utils.QM(wmsId, wmsId, None), {'sandbox': sandbox})


	def _getJobsOutput(self, ids):
		if not len(ids):
			raise StopIteration

		activity = utils.ActivityLog('retrieving job outputs')
		for wmsId, jobNum in ids:
			path = self._getSandbox(wmsId)
			if path == None:
				yield (jobNum, None)
				continue

			# Cleanup sandbox
			outFiles = utils.listMapReduce(lambda pat: glob.glob(os.path.join(path, pat)), self.outputFiles)
			utils.removeFiles(filter(lambda x: x not in outFiles, map(lambda fn: os.path.join(path, fn), os.listdir(path))))

			yield (jobNum, path)
		del activity


	def _getSandboxFiles(self, module, monitor, smList):
		files = BasicWMS._getSandboxFiles(self, module, monitor, smList)
		if self.proxy.getAuthFile():
			files.append(utils.VirtualFile('_proxy.dat', open(self.proxy.getAuthFile(), 'r').read()))
		return files


	def getQueues(self):
		return None

	def getNodes(self):
		return None

	def parseJobState(self, state):
		return self._statusMap[state]

	def getCancelArguments(self, wmsIds):
		return str.join(' ', wmsIds)

	def checkReq(self, reqs, req, test = lambda x: x > 0):
		if req in reqs:
			return test(reqs[req])
		return False

	def getJobArguments(self, jobNum, sandbox):
		raise AbstractError

	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		raise AbstractError

	def parseSubmitOutput(self, data):
		raise AbstractError

	def unknownID(self):
		raise AbstractError

	def parseStatus(self, status):
		raise AbstractError

	def getCheckArguments(self, wmsIds):
		raise AbstractError
