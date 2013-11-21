from python_compat import *
import sys, os, time, copy, tempfile, tarfile
from grid_control import QM, ConfigError, APIError, RethrowError, Job, utils
from wms import WMS, BasicWMS

try:
	from email.utils import parsedate
except ImportError:
	from email.Utils import parsedate

def jdlEscape(value):
	repl = { '\\': r'\\', '\"': r'\"', '\n': r'\n' }
	return '"' + str.join('', map(lambda char: repl.get(char, char), value)) + '"'


class GridWMS(BasicWMS):
	_statusMap = {
		'ready':     Job.READY,
		'submitted': Job.SUBMITTED,
		'waiting':   Job.WAITING,
		'queued':    Job.QUEUED,
		'scheduled': Job.QUEUED,
		'running':   Job.RUNNING,
		'aborted':   Job.ABORTED,
		'cancelled': Job.ABORTED,
		'failed':    Job.ABORTED,
		'done':      Job.DONE,
		'cleared':   Job.ABORTED
	}


	def __init__(self, config, wmsName):
		config.set('grid', 'proxy', 'VomsProxy', override = False)
		BasicWMS.__init__(self, config, wmsName, 'grid')

		self.brokerSite = self._createBroker('site broker', 'UserBroker', 'sites', 'sites', self.getSites)
		self.vo = config.get(self._getSections('backend'), 'vo', self.proxy.getGroup())

		self._submitParams = {}
		self._ce = config.get(self._getSections('backend'), 'ce', '', onChange = None)
		self._configVO = config.getPath(self._getSections('backend'), 'config', '', onChange = None)
		self._warnSBSize = config.getInt(self._getSections('backend'), 'warn sb size', 5 * 1024 * 1024)


	def getSites(self):
		return None


	def storageReq(self, sites):
		fmt = lambda x: 'Member(%s, other.GlueCESEBindGroupSEUniqueID)' % jdlEscape(x)
		if sites:
			return '( %s )' % str.join(' || ', map(fmt, sites))


	def sitesReq(self, sites):
		fmt = lambda x: 'RegExp(%s, other.GlueCEUniqueID)' % jdlEscape(x)
		(blacklist, whitelist) = utils.splitBlackWhiteList(sites)
		sitereqs = map(lambda x: '!' + fmt(x), blacklist)
		if len(whitelist):
			sitereqs.append('(%s)' % str.join(' || ', map(fmt, whitelist)))
		if sitereqs:
			return '( %s )' % str.join(' && ', sitereqs)


	def _formatRequirements(self, reqs):
		result = ['other.GlueHostNetworkAdapterOutboundIP']
		for reqType, arg in reqs:
			if reqType == WMS.SOFTWARE:
				result.append('Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' % jdlEscape(arg))
			elif reqType == WMS.WALLTIME:
				if arg > 0:
					result.append('(other.GlueCEPolicyMaxWallClockTime >= %d)' % int((arg + 59) / 60))
			elif reqType == WMS.CPUTIME:
				if arg > 0:
					result.append('(other.GlueCEPolicyMaxCPUTime >= %d)' % int((arg + 59) / 60))
			elif reqType == WMS.MEMORY:
				if arg > 0:
					result.append('(other.GlueHostMainMemoryRAMSize >= %d)' % arg)
			elif reqType == WMS.STORAGE:
				result.append(self.storageReq(arg))
			elif reqType == WMS.SITES:
				result.append(self.sitesReq(arg))
			elif reqType == WMS.CPUS:
				pass # Handle number of cpus in makeJDL
			else:
				raise APIError('Unknown requirement type %s or argument %r' % (WMS.reqTypes[reqType], arg))
		return str.join(' && ', filter(lambda x: x != None, result))


	def makeJDL(self, jobNum, module):
		cfgPath = os.path.join(self.config.workDir, 'jobs', 'job_%d.var' % jobNum)
		sbIn = map(lambda (d, s, t): s, self._getSandboxFilesIn(module))
		sbOut = map(lambda (d, s, t): t, self._getSandboxFilesOut(module))
		wcList = filter(lambda x: '*' in x, sbOut)
		if len(wcList):
			self._writeJobConfig(cfgPath, jobNum, module, {'GC_WC': str.join(' ', wcList)})
			sandboxOutJDL = filter(lambda x: x not in wcList, sbOut) + ['GC_WC.tar.gz']
		else:
			self._writeJobConfig(cfgPath, jobNum, module)
			sandboxOutJDL = sbOut
		# Warn about too large sandboxes
		sbSizes = map(os.path.getsize, sbIn)
		if sbSizes and (self._warnSBSize > 0) and (sum(sbSizes) > self._warnSBSize):
			if not utils.getUserBool('Sandbox is very large (%d bytes) and can cause issues with the WMS! Do you want to continue?' % sum(sbSizes), False):
				sys.exit(0)
			self._warnSBSize = 0

		reqs = self.brokerSite.brokerAdd(module.getRequirements(jobNum), WMS.SITES)
		formatStrList = lambda strList: '{ %s }' % str.join(', ', map(lambda x: '"%s"' % x, strList))
		contents = {
			'Executable': '"gc-run.sh"',
			'Arguments': '"%d"' % jobNum,
			'StdOutput': '"gc.stdout"',
			'StdError': '"gc.stderr"',
			'InputSandbox': formatStrList(sbIn + [cfgPath]),
			'OutputSandbox': formatStrList(sandboxOutJDL),
			'Requirements': self._formatRequirements(reqs),
			'VirtualOrganisation': '"%s"' % self.vo,
			'Rank': '-other.GlueCEStateEstimatedResponseTime',
			'RetryCount': 2
		}
		cpus = dict(reqs).get(WMS.CPUS, 1)
		if cpus > 1:
			contents['CpuNumber'] = cpus
		return utils.DictFormat(' = ').format(contents, format = '%s%s%s;\n')


	def writeWMSIds(self, ids):
		try:
			fd, jobs = tempfile.mkstemp('.jobids')
			utils.safeWrite(os.fdopen(fd, 'w'), str.join('\n', self._getRawIDs(ids)))
		except:
			raise RethrowError('Could not write wms ids to %s.' % jobs)
		return jobs


	def _parseStatus(self, lines):
		cur = None

		def format(data):
			data = copy.copy(data)
			status = data['status'].lower()
			try:
				if status.find('failed') >=0:
					status = 'failed'
				else:
					status = status.split()[0]
			except:
				pass
			data['status'] = status
			try:
				data['timestamp'] = int(time.mktime(parsedate(data['timestamp'])))
			except:
				pass
			return data

		for line in lines:
			try:
				key, value = line.split(':', 1)
			except:
				continue
			key = key.strip().lower()
			value = value.strip()

			if key.startswith('status info'):
				key = 'id'
			elif key.startswith('current status'):
				key = 'status'
			elif key.startswith('status reason'):
				key = 'reason'
			elif key.startswith('destination'):
				key = 'dest'
			elif key.startswith('reached') or key.startswith('submitted'):
				key = 'timestamp'
			else:
				continue

			if key == 'id':
				if cur != None:
					try:
						yield format(cur)
					except:
						pass
				cur = { 'id': value }
			else:
				cur[key] = value

		if cur != None:
			try:
				yield format(cur)
			except:
				pass


	def _parseStatusX(self, lines):
		adder = lambda a, b: utils.QM('=====' not in b and b != '\n', a + b, a)
		remap = { 'destination': 'dest', 'status reason': 'reason',
			'status info for the job': 'id', 'current status': 'status',
			'submitted': 'timestamp', 'reached': 'timestamp', 'exit code': 'gridexit' }
		for section in utils.accumulate(lines, lambda x, buf: ('='*70) in x, '', opAdd = adder):
			data = utils.DictFormat(':').parse(str.join('', section), keyParser = {None: lambda k: remap.get(k, str)})
			data = utils.filterDict(data, vF = lambda v: v)
			if data:
				try:
					if 'failed' in data['status']:
						data['status'] = 'failed'
					else:
						data['status'] = data['status'].split()[0].lower()
				except:
					pass
				try:
					data['timestamp'] = int(time.mktime(parsedate(data['timestamp'])))
				except:
					pass
				yield data


	def explainError(self, proc, code):
		if 'Keyboard interrupt raised by user' in proc.getError():
			return True
		return False


	# Submit job and yield (jobNum, WMS ID, other data)
	def _submitJob(self, jobNum, module):
		fd, jdl = tempfile.mkstemp('.jdl')
		try:
			data = self.makeJDL(jobNum, module)
			utils.safeWrite(os.fdopen(fd, 'w'), data)
		except:
			utils.removeFiles([jdl])
			raise RethrowError('Could not write jdl data to %s.' % jdl)

		try:
			tmp = utils.filterDict(self._submitParams, vF = lambda v: v)
			params = str.join(' ', map(lambda (x, y): '%s %s' % (x, y), tmp.items()))

			log = tempfile.mktemp('.log')
			activity = utils.ActivityLog('submitting jobs')
			proc = utils.LoggedProcess(self._submitExec, '%s --nomsg --noint --logfile "%s" "%s"' % (params, log, jdl))

			wmsId = None
			for line in filter(lambda x: x.startswith('http'), map(str.strip, proc.iter())):
				wmsId = line
			retCode = proc.wait()
			del activity

			if (retCode != 0) or (wmsId == None):
				if self.explainError(proc, retCode):
					pass
				else:
					proc.logError(self.errorLog, log = log, jdl = jdl)
		finally:
			utils.removeFiles([log, jdl])
		return (jobNum, QM(wmsId, self._createId(wmsId), None), {'jdl': str.join('', data)})


	# Check status of jobs and yield (jobNum, wmsID, status, other data)
	def checkJobs(self, ids):
		if len(ids) == 0:
			raise StopIteration

		jobNumMap = dict(ids)
		jobs = self.writeWMSIds(ids)
		log = tempfile.mktemp('.log')

		activity = utils.ActivityLog('checking job status')
		proc = utils.LoggedProcess(self._statusExec, '--verbosity 1 --noint --logfile "%s" -i "%s"' % (log, jobs))
		for data in self._parseStatus(proc.iter()):
			data['id'] = self._createId(data['id'])
			yield (jobNumMap.get(data['id']), data['id'], self._statusMap[data['status']], data)
		retCode = proc.wait()
		del activity

		if retCode != 0:
			if self.explainError(proc, retCode):
				pass
			else:
				proc.logError(self.errorLog, log = log, jobs = jobs)
		utils.removeFiles([log, jobs])


	# Get output of jobs and yield output dirs
	def _getJobsOutput(self, ids):
		if len(ids) == 0:
			raise StopIteration

		basePath = os.path.join(self._outputPath, 'tmp')
		try:
			if len(ids) == 1:
				# For single jobs create single subdir
				tmpPath = os.path.join(basePath, md5(ids[0][0]).hexdigest())
			else:
				tmpPath = basePath
			utils.ensureDirExists(tmpPath)
		except:
			raise RethrowError('Temporary path "%s" could not be created.' % tmpPath, RuntimeError)

		jobNumMap = dict(ids)
		jobs = self.writeWMSIds(ids)
		log = tempfile.mktemp('.log')

		activity = utils.ActivityLog('retrieving job outputs')
		proc = utils.LoggedProcess(self._outputExec,
			'--noint --logfile "%s" -i "%s" --dir "%s"' % (log, jobs, tmpPath))

		# yield output dirs
		todo = jobNumMap.values()
		currentJobNum = None
		for line in map(str.strip, proc.iter()):
			if line.startswith(tmpPath):
				todo.remove(currentJobNum)
				outputDir = line.strip()
				if os.path.exists(outputDir):
					if 'GC_WC.tar.gz' in os.listdir(outputDir):
						wildcardTar = os.path.join(outputDir, 'GC_WC.tar.gz')
						try:
							tarfile.TarFile.open(wildcardTar, 'r:gz').extractall(outputDir)
							os.unlink(wildcardTar)
						except:
							utils.eprint("Can't unpack output files contained in %s" % wildcardTar)
							pass
				yield (currentJobNum, line.strip())
				currentJobNum = None
			else:
				currentJobNum = jobNumMap.get(self._createId(line), currentJobNum)
		retCode = proc.wait()
		del activity

		if retCode != 0:
			if 'Keyboard interrupt raised by user' in proc.getError():
				utils.removeFiles([log, jobs, basePath])
				raise StopIteration
			else:
				proc.logError(self.errorLog, log = log)
			utils.eprint('Trying to recover from error ...')
			for dirName in os.listdir(basePath):
				yield (None, os.path.join(basePath, dirName))

		# return unretrievable jobs
		for jobNum in todo:
			yield (jobNum, None)

		utils.removeFiles([log, jobs, basePath])


	def cancelJobs(self, allIds):
		if len(allIds) == 0:
			raise StopIteration

		waitFlag = False
		for ids in map(lambda x: allIds[x:x+5], range(0, len(allIds), 5)):
			# Delete jobs in groups of 5 - with 5 seconds between groups
			if waitFlag and utils.wait(5) == False:
				break
			waitFlag = True

			jobNumMap = dict(ids)
			jobs = self.writeWMSIds(ids)
			log = tempfile.mktemp('.log')

			activity = utils.ActivityLog('cancelling jobs')
			proc = utils.LoggedProcess(self._cancelExec, '--noint --logfile "%s" -i "%s"' % (log, jobs))
			retCode = proc.wait()
			del activity

			# select cancelled jobs
			for deletedWMSId in filter(lambda x: x.startswith('- '), proc.iter()):
				deletedWMSId = self._createId(deletedWMSId.strip('- \n'))
				yield (jobNumMap.get(deletedWMSId), deletedWMSId)

			if retCode != 0:
				if self.explainError(proc, retCode):
					pass
				else:
					proc.logError(self.errorLog, log = log)
			utils.removeFiles([log, jobs])
