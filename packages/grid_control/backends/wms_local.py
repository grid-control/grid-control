# | Copyright 2009-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, glob, time, shutil, tempfile
from grid_control import utils
from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import BackendError, BasicWMS, WMS
from grid_control.job_db import Job
from grid_control.utils.file_objects import VirtualFile
from grid_control.utils.gc_itertools import lchain
from hpfwk import AbstractError, ExceptionCollector
from python_compat import ifilter, imap, ismap, lfilter

class LocalWMS(BasicWMS):
	configSections = BasicWMS.configSections + ['local']

	def __init__(self, config, name, submitExec, statusExec, cancelExec):
		config.set('broker', 'RandomBroker')
		config.setInt('wait idle', 20)
		config.setInt('wait work', 5)
		(self.submitExec, self.statusExec, self.cancelExec) = (submitExec, statusExec, cancelExec)
		BasicWMS.__init__(self, config, name)

		self.brokerSite = config.getPlugin('site broker', 'UserBroker', cls = Broker,
			inherit = True, tags = [self], pargs = ('sites', 'sites', self.getNodes))
		self.brokerQueue = config.getPlugin('queue broker', 'UserBroker', cls = Broker,
			inherit = True, tags = [self], pargs = ('queue', 'queues', self.getQueues))

		self.sandCache = []
		self.sandPath = config.getPath('sandbox path', config.getWorkPath('sandbox'), mustExist = False)
		self.scratchPath = config.getList('scratch path', ['TMPDIR', '/tmp'], onChange = True)
		self.submitOpts = config.get('submit options', '', onChange = None)
		self.memory = config.getInt('memory', -1, onChange = None)
		try:
			if not os.path.exists(self.sandPath):
				os.mkdir(self.sandPath)
		except Exception:
			raise BackendError('Unable to create sandbox base directory "%s"!' % self.sandPath)


	# Check status of jobs and yield (jobNum, wmsID, status, other data)
	def checkJobs(self, ids):
		if not len(ids):
			raise StopIteration

		activity = utils.ActivityLog('checking job status')
		proc = utils.LoggedProcess(self.statusExec, self.getCheckArguments(self._getRawIDs(ids)))

		tmp = {}
		found_error = False
		for data in self.parseStatus(proc.iter()):
			if data.pop(None, None) == 'abort':
				found_error = True
				break
			wmsId = self._createId(data['id'])
			tmp[wmsId] = (wmsId, self.parseJobState(data['status']), data)

		if not found_error:
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
			if path is None:
				self._log.warning('Sandbox for job %d with wmsId "%s" could not be found', jobNum, wmsId)
				continue
			try:
				shutil.rmtree(path)
			except Exception:
				raise BackendError('Sandbox for job %d with wmsId "%s" could not be deleted' % (jobNum, wmsId))
			yield (jobNum, wmsId)
		del activity


	def _getSandbox(self, wmsId):
		# Speed up function by caching result of listdir
		def searchSandbox(source):
			for path in imap(lambda sbox: os.path.join(self.sandPath, sbox), source):
				if os.path.exists(os.path.join(path, wmsId)):
					return path
		result = searchSandbox(self.sandCache)
		if result:
			return result
		oldCache = self.sandCache[:]
		self.sandCache = lfilter(lambda x: os.path.isdir(os.path.join(self.sandPath, x)), os.listdir(self.sandPath))
		return searchSandbox(ifilter(lambda x: x not in oldCache, self.sandCache))


	# Submit job and yield (jobNum, WMS ID, other data)
	def _submitJob(self, jobNum, module):
		activity = utils.ActivityLog('submitting jobs')

		try:
			sandbox = tempfile.mkdtemp('', '%s.%04d.' % (module.taskID, jobNum), self.sandPath)
		except Exception:
			raise BackendError('Unable to create sandbox directory "%s"!' % sandbox)
		sbPrefix = sandbox.replace(self.sandPath, '').lstrip('/')
		def translateTarget(d, s, t):
			return (d, s, os.path.join(sbPrefix, t))
		self.smSBIn.doTransfer(ismap(translateTarget, self._getSandboxFilesIn(module)))

		self._writeJobConfig(os.path.join(sandbox, '_jobconfig.sh'), jobNum, module, {
			'GC_SANDBOX': sandbox, 'GC_SCRATCH_SEARCH': str.join(' ', self.scratchPath)})
		reqs = self.brokerSite.brokerAdd(module.getRequirements(jobNum), WMS.SITES)
		reqs = dict(self.brokerQueue.brokerAdd(reqs, WMS.QUEUES))
		if (self.memory > 0) and (reqs.get(WMS.MEMORY, 0) < self.memory):
			reqs[WMS.MEMORY] = self.memory # local jobs need higher (more realistic) memory requirements

		(stdout, stderr) = (os.path.join(sandbox, 'gc.stdout'), os.path.join(sandbox, 'gc.stderr'))
		jobName = module.getDescription(jobNum).jobName
		proc = utils.LoggedProcess(self.submitExec, '%s %s "%s" %s' % (self.submitOpts,
			self.getSubmitArguments(jobNum, jobName, reqs, sandbox, stdout, stderr),
			utils.pathShare('gc-local.sh'), self.getJobArguments(jobNum, sandbox)))
		retCode = proc.wait()
		wmsIdText = proc.getOutput().strip().strip('\n')
		try:
			wmsId = self.parseSubmitOutput(wmsIdText)
		except Exception:
			wmsId = None

		del activity

		if retCode != 0:
			self._log.warning('%s failed:', self.submitExec)
		elif wmsId is None:
			self._log.warning('%s did not yield job id:\n%s', self.submitExec, wmsIdText)
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
			if path is None:
				yield (jobNum, None)
				continue

			# Cleanup sandbox
			outFiles = lchain(imap(lambda pat: glob.glob(os.path.join(path, pat)), self.outputFiles))
			utils.removeFiles(ifilter(lambda x: x not in outFiles, imap(lambda fn: os.path.join(path, fn), os.listdir(path))))

			yield (jobNum, path)
		del activity


	def _getSandboxFiles(self, module, monitor, smList):
		files = BasicWMS._getSandboxFiles(self, module, monitor, smList)
		for idx, authFile in enumerate(self._token.getAuthFiles()):
			files.append(VirtualFile(('_proxy.dat.%d' % idx).replace('.0', ''), open(authFile, 'r').read()))
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


class Local(WMS):
	configSections = WMS.configSections + ['local']

	def __new__(cls, config, name):
		def createWMS(wms):
			try:
				wmsCls = WMS.getClass(wms)
			except Exception:
				raise BackendError('Unable to load backend class %s' % repr(wms))
			wms_config = config.changeView(viewClass = 'TaggedConfigView', setClasses = [wmsCls])
			return WMS.createInstance(wms, wms_config, name)
		wms = config.get('wms', '')
		if wms:
			return createWMS(wms)
		ec = ExceptionCollector()
		for cmd, wms in [('sacct', 'SLURM'), ('sgepasswd', 'OGE'), ('pbs-config', 'PBS'),
				('qsub', 'OGE'), ('bsub', 'LSF'), ('job_slurm', 'JMS')]:
			try:
				utils.resolveInstallPath(cmd)
			except Exception:
				ec.collect()
				continue
			return createWMS(wms)
		ec.raise_any(BackendError('No valid local backend found!')) # at this point all backends have failed!
