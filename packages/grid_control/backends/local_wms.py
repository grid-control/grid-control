import sys, os, tempfile, shutil, time, random, glob
from grid_control import AbstractObject, ConfigError, Job, utils
from wms import WMS
from local_api import LocalWMSApi

class LocalWMS(WMS):
	def __init__(self, config, module, monitor):
		wmsapi = config.get('local', 'wms', self._guessWMS())
		if wmsapi != self._guessWMS():
			utils.vprint('Default batch system on this host is: %s' % self._guessWMS(), -1, once = True)
		self.api = LocalWMSApi.open(wmsapi, config, self)
		utils.vprint('Using batch system: %s' % self.api.__class__.__name__, -1)
		self.addAttr = {}
		if config.parser.has_section(wmsapi):
			self.addAttr = dict(map(lambda item: (item, config.get(wmsapi, item)), config.parser.options(wmsapi)))

		config.set('local', 'broker', 'RandomBroker', override = False)
		WMS.__init__(self, config, module, monitor, 'local', self.api)

		self.sandPath = config.getPath('local', 'sandbox path', os.path.join(config.workDir, 'sandbox'), check=False)
		self.sandCache = []
		self.scratchPath = config.getPath('local', 'scratch path', '', volatile=True)
		self._source = None
		nameFile = config.getPath('local', 'name source', '', volatile=True)
		if nameFile != '':
			tmp = map(str.strip, open(nameFile, 'r').readlines())
			self._source = filter(lambda x: not (x.startswith('#') or x == ''), tmp)


	def _guessWMS(self):
		wmsCmdList = [ ('PBS', 'pbs-config'), ('OGE', 'qsub'), ('LSF', 'bsub'), ('SLURM', 'job_slurm'), ('PBS', 'sh') ]
		for wms, cmd in wmsCmdList:
			try:
				utils.resolveInstallPath(cmd)
				return wms
			except:
				pass


	def getSandboxFiles(self):
		files = WMS.getSandboxFiles(self)
		if self.proxy.getAuthFile():
			files.append(utils.VirtualFile('_proxy.dat', open(self.proxy.getAuthFile(), 'r').read()))
		return files


	# Wait 5 seconds between cycles and 0 seconds between steps
	def getTimings(self):
		return (20, 5)


	def getJobName(self, jobNum):
		if self._source:
			return self._source[jobNum % len(self._source)]
		return self.module.taskID[:10] + '.' + str(jobNum) #.rjust(4, '0')[:4]


	# Submit job and yield (jobNum, WMS ID, other data)
	def submitJob(self, jobNum):
		activity = utils.ActivityLog('submitting jobs')

		try:
			if not os.path.exists(self.sandPath):
				os.mkdir(self.sandPath)
			sandbox = tempfile.mkdtemp('', '%s.%04d.' % (self.module.taskID, jobNum), self.sandPath)
			for fileName in self.sandboxIn:
				shutil.copy(fileName, sandbox)
		except OSError:
			raise RuntimeError('Sandbox path "%s" is not accessible.' % self.sandPath)
		except IOError:
			raise RuntimeError('Sandbox "%s" could not be prepared.' % sandbox)

		cfgPath = os.path.join(sandbox, '_jobconfig.sh')
		self.writeJobConfig(jobNum, cfgPath, {'GC_SANDBOX': sandbox, 'GC_SCRATCH': self.scratchPath})
		reqs = dict(self.broker.brokerSites(self.module.getRequirements(jobNum)))

		(stdout, stderr) = (os.path.join(sandbox, 'gc.stdout'), os.path.join(sandbox, 'gc.stderr'))
		proc = utils.LoggedProcess(self.api.submitExec, '%s "%s" %s' % (
			self.api.getSubmitArguments(jobNum, reqs, sandbox, stdout, stderr, self.addAttr),
			utils.pathShare('local.sh'), self.api.getJobArguments(jobNum, sandbox)))
		retCode = proc.wait()
		wmsIdText = proc.getOutput().strip().strip('\n')
		try:
			wmsId = self.api.parseSubmitOutput(wmsIdText)
		except:
			wmsId = None

		del activity

		if retCode != 0:
			utils.eprint('WARNING: %s failed:' % self.api.submitExec)
		elif wmsId == None:
			utils.eprint('WARNING: %s did not yield job id:\n%s' % (self.api.submitExec, wmsIdText))
		if wmsId:
			wmsId = '%s.%s' % (wmsId, self.api.__class__.__name__)
			open(os.path.join(sandbox, wmsId), 'w')
		else:
			proc.logError(self.errorLog)
		return (jobNum, utils.QM(wmsId, wmsId, None), {'sandbox': sandbox})


	# Check status of jobs and yield (jobNum, wmsID, status, other data)
	def checkJobs(self, ids):
		if not len(ids):
			raise StopIteration

		shortWMSIds = map(lambda (wmsId, jobNum): wmsId.split('.')[0], ids)
		activity = utils.ActivityLog('checking job status')
		proc = utils.LoggedProcess(self.api.statusExec, self.api.getCheckArguments(shortWMSIds))

		tmp = {}
		for data in self.api.parseStatus(proc.iter()):
			# (job number, status, extra info)
			wmsId = '%s.%s' % (data['id'], self.api.__class__.__name__)
			tmp[wmsId] = (wmsId, self.api.parseJobState(data['status']), data)

		for wmsId, jobNum in ids:
			if wmsId not in tmp:
				yield (jobNum, wmsId, Job.DONE, {})
			else:
				yield tuple([jobNum] + list(tmp[wmsId]))

		retCode = proc.wait()
		del activity

		if retCode != 0:
			for line in proc.getError().splitlines():
				if not self.api.unknownID() in line:
					utils.eprint(line)


	def getSandbox(self, wmsId):
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


	def getJobsOutput(self, ids):
		if not len(ids):
			raise StopIteration

		activity = utils.ActivityLog('retrieving job outputs')
		for wmsId, jobNum in ids:
			path = self.getSandbox(wmsId)
			if path == None:
				yield (jobNum, None)
				continue

			# Cleanup sandbox
			outFiles = utils.listMapReduce(lambda pat: glob.glob(os.path.join(path, pat)), self.sandboxOut)
			utils.removeFiles(filter(lambda x: x not in outFiles, map(lambda fn: os.path.join(path, fn), os.listdir(path))))

			yield (jobNum, path)
		del activity


	def cancelJobs(self, ids):
		if not len(ids):
			raise StopIteration

		activity = utils.ActivityLog('cancelling jobs')
		shortWMSIds = map(lambda (wmsId, jobNum): wmsId.split('.')[0], ids)
		proc = utils.LoggedProcess(self.api.cancelExec, self.api.getCancelArguments(shortWMSIds))
		if proc.wait() != 0:
			for line in proc.getError().splitlines():
				if not self.api.unknownID() in line:
					utils.eprint(line.strip())
		del activity

		activity = utils.ActivityLog('waiting for jobs to finish')
		time.sleep(5)
		for wmsId, jobNum in ids:
			path = self.getSandbox(wmsId)
			if path == None:
				utils.eprint('Sandbox for job %d with wmsId "%s" could not be found' % (jobNum, wmsId))
				continue
			try:
				shutil.rmtree(path)
			except:
				raise RuntimeError('Sandbox for job %d with wmsId "%s" could not be deleted' % (jobNum, wmsId))
			yield (wmsId, jobNum)
		del activity
