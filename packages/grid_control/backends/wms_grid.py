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

import os, sys, copy, time, tempfile
from grid_control import utils
from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import BackendError, BasicWMS, WMS
from grid_control.job_db import Job
from grid_control.utils.process_base import LocalProcess
from hpfwk import APIError
from python_compat import ifilter, imap, irange, lfilter, lmap, md5, parsedate, tarfile

def jdlEscape(value):
	repl = { '\\': r'\\', '\"': r'\"', '\n': r'\n' }
	return '"' + str.join('', imap(lambda char: repl.get(char, char), value)) + '"'


class GridWMS(BasicWMS):
	configSections = BasicWMS.configSections + ['grid']

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


	def __init__(self, config, name):
		config.set('access token', 'VomsProxy')
		BasicWMS.__init__(self, config, name)

		self.brokerSite = config.getPlugin('site broker', 'UserBroker',
			cls = Broker, tags = [self], pargs = ('sites', 'sites', self.getSites))
		self.vo = config.get('vo', self._token.getGroup())

		self._submitParams = {}
		self._ce = config.get('ce', '', onChange = None)
		self._configVO = config.getPath('config', '', onChange = None)
		self._warnSBSize = config.getInt('warn sb size', 5 * 1024 * 1024)
		self._jobPath = config.getWorkPath('jobs')

	def getSites(self):
		return None


	def storageReq(self, sites):
		fmt = lambda x: 'Member(%s, other.GlueCESEBindGroupSEUniqueID)' % jdlEscape(x)
		if sites:
			return '( %s )' % str.join(' || ', imap(fmt, sites))


	def sitesReq(self, sites):
		fmt = lambda x: 'RegExp(%s, other.GlueCEUniqueID)' % jdlEscape(x)
		(blacklist, whitelist) = utils.splitBlackWhiteList(sites)
		sitereqs = lmap(lambda x: '!' + fmt(x), blacklist)
		if len(whitelist):
			sitereqs.append('(%s)' % str.join(' || ', imap(fmt, whitelist)))
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
		return str.join(' && ', ifilter(lambda x: x is not None, result))


	def makeJDL(self, jobNum, module):
		cfgPath = os.path.join(self._jobPath, 'job_%d.var' % jobNum)
		sbIn = lmap(lambda d_s_t: d_s_t[1], self._getSandboxFilesIn(module))
		sbOut = lmap(lambda d_s_t: d_s_t[2], self._getSandboxFilesOut(module))
		wcList = lfilter(lambda x: '*' in x, sbOut)
		if len(wcList):
			self._writeJobConfig(cfgPath, jobNum, module, {'GC_WC': str.join(' ', wcList)})
			sandboxOutJDL = lfilter(lambda x: x not in wcList, sbOut) + ['GC_WC.tar.gz']
		else:
			self._writeJobConfig(cfgPath, jobNum, module, {})
			sandboxOutJDL = sbOut
		# Warn about too large sandboxes
		sbSizes = lmap(os.path.getsize, sbIn)
		if sbSizes and (self._warnSBSize > 0) and (sum(sbSizes) > self._warnSBSize):
			if not utils.getUserBool('Sandbox is very large (%d bytes) and can cause issues with the WMS! Do you want to continue?' % sum(sbSizes), False):
				sys.exit(os.EX_OK)
			self._warnSBSize = 0

		reqs = self.brokerSite.brokerAdd(module.getRequirements(jobNum), WMS.SITES)
		formatStrList = lambda strList: '{ %s }' % str.join(', ', imap(lambda x: '"%s"' % x, strList))
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
		except Exception:
			raise BackendError('Could not write wms ids to %s.' % jobs)
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
					status = status.split('(')[0].split()[0]
			except Exception:
				pass
			data['status'] = status
			try:
				data['timestamp'] = int(time.mktime(parsedate(data['timestamp'])))
			except Exception:
				pass
			return data

		for line in lines:
			try:
				key, value = line.split(':', 1)
			except Exception:
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
				if cur is not None:
					try:
						yield format(cur)
					except Exception:
						pass
				cur = { 'id': value }
			else:
				cur[key] = value

		if cur is not None:
			try:
				yield format(cur)
			except Exception:
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
				except Exception:
					pass
				try:
					data['timestamp'] = int(time.mktime(parsedate(data['timestamp'])))
				except Exception:
					pass
				yield data


	def explainError(self, proc, code):
		if 'Keyboard interrupt raised by user' in proc.stderr.read(timeout = 0):
			return True
		return False


	# Submit job and yield (jobNum, WMS ID, other data)
	def _submitJob(self, jobNum, module):
		fd, jdl = tempfile.mkstemp('.jdl')
		try:
			jdlData = self.makeJDL(jobNum, module)
			utils.safeWrite(os.fdopen(fd, 'w'), jdlData)
		except Exception:
			utils.removeFiles([jdl])
			raise BackendError('Could not write jdl data to %s.' % jdl)

		try:
			submitArgs = []
			for key_value in utils.filterDict(self._submitParams, vF = lambda v: v).items():
				submitArgs.extend(key_value)
			submitArgs.append(jdl)

			activity = utils.ActivityLog('submitting jobs')
			proc = LocalProcess(self._submitExec, '--nomsg', '--noint', '--logfile', '/dev/stderr', *submitArgs)

			wmsId = None
			for line in ifilter(lambda x: x.startswith('http'), imap(str.strip, proc.stdout.iter(timeout = 60))):
				wmsId = line
			retCode = proc.status(timeout = 0, terminate = True)

			del activity

			if (retCode != 0) or (wmsId is None):
				if self.explainError(proc, retCode):
					pass
				else:
					self._log.log_process(proc, files = {'jdl': utils.safeRead(jdl)})
		finally:
			utils.removeFiles([jdl])
		return (jobNum, utils.QM(wmsId, self._createId(wmsId), None), {'jdl': str.join('', jdlData)})


	# Check status of jobs and yield (jobNum, wmsID, status, other data)
	def checkJobs(self, ids):
		if len(ids) == 0:
			raise StopIteration

		jobNumMap = dict(ids)
		jobs = self.writeWMSIds(ids)

		activity = utils.ActivityLog('checking job status')
		proc = LocalProcess(self._statusExec, '--verbosity', 1, '--noint', '--logfile', '/dev/stderr', '-i', jobs)
		for data in self._parseStatus(proc.stdout.iter(timeout = 60)):
			data['id'] = self._createId(data['id'])
			yield (jobNumMap.get(data['id']), data['id'], self._statusMap[data['status']], data)
		retCode = proc.status(timeout = 0, terminate = True)
		del activity

		if retCode != 0:
			if self.explainError(proc, retCode):
				pass
			else:
				self._log.log_process(proc, files = {'jobs': utils.safeRead(jobs)})
		utils.removeFiles([jobs])


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
		except Exception:
			raise BackendError('Temporary path "%s" could not be created.' % tmpPath, BackendError)

		jobNumMap = dict(ids)
		jobs = self.writeWMSIds(ids)

		activity = utils.ActivityLog('retrieving job outputs')
		proc = LocalProcess(self._outputExec, '--noint', '--logfile', '/dev/stderr', '-i', jobs, '--dir', tmpPath)

		# yield output dirs
		todo = jobNumMap.values()
		currentJobNum = None
		for line in imap(str.strip, proc.stdout.iter(timeout = 60)):
			if line.startswith(tmpPath):
				todo.remove(currentJobNum)
				outputDir = line.strip()
				if os.path.exists(outputDir):
					if 'GC_WC.tar.gz' in os.listdir(outputDir):
						wildcardTar = os.path.join(outputDir, 'GC_WC.tar.gz')
						try:
							tarfile.TarFile.open(wildcardTar, 'r:gz').extractall(outputDir)
							os.unlink(wildcardTar)
						except Exception:
							utils.eprint("Can't unpack output files contained in %s" % wildcardTar)
				yield (currentJobNum, line.strip())
				currentJobNum = None
			else:
				currentJobNum = jobNumMap.get(self._createId(line), currentJobNum)
		retCode = proc.status(timeout = 0, terminate = True)
		del activity

		if retCode != 0:
			if 'Keyboard interrupt raised by user' in proc.stderr.read(timeout = 0):
				utils.removeFiles([jobs, basePath])
				raise StopIteration
			else:
				self._log.log_process(proc, files = {'jobs': utils.safeRead(jobs)})
			utils.eprint('Trying to recover from error ...')
			for dirName in os.listdir(basePath):
				yield (None, os.path.join(basePath, dirName))

		# return unretrievable jobs
		for jobNum in todo:
			yield (jobNum, None)

		utils.removeFiles([jobs, basePath])


	def cancelJobs(self, allIds):
		if len(allIds) == 0:
			raise StopIteration

		waitFlag = False
		for ids in imap(lambda x: allIds[x:x+5], irange(0, len(allIds), 5)):
			# Delete jobs in groups of 5 - with 5 seconds between groups
			if waitFlag and not utils.wait(5):
				break
			waitFlag = True

			jobNumMap = dict(ids)
			jobs = self.writeWMSIds(ids)

			activity = utils.ActivityLog('cancelling jobs')
			proc = LocalProcess(self._cancelExec, '--noint', '--logfile', '/dev/stderr', '-i', jobs)
			retCode = proc.status(timeout = 60, terminate = True)
			del activity

			# select cancelled jobs
			for deletedWMSId in ifilter(lambda x: x.startswith('- '), proc.stdout.iter()):
				deletedWMSId = self._createId(deletedWMSId.strip('- \n'))
				yield (jobNumMap.get(deletedWMSId), deletedWMSId)

			if retCode != 0:
				if self.explainError(proc, retCode):
					pass
				else:
					self._log.log_process(proc, files = {'jobs': utils.safeRead(jobs)})
			utils.removeFiles([jobs])
