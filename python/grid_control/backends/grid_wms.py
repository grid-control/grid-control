import sys, os, time, copy, tempfile, cStringIO, re, tarfile, gzip
from grid_control import ConfigError, Job, utils
from wms import WMS

try:
	from email.utils import parsedate
except ImportError:
	from email.Utils import parsedate

class GridWMS(WMS):
	_statusMap = {
		'ready':     Job.READY,
		'submitted': Job.SUBMITTED,
		'waiting':   Job.WAITING,
		'queued':    Job.QUEUED,
		'scheduled': Job.QUEUED,
		'running':   Job.RUNNING,
		'aborted':   Job.ABORTED,
		'cancelled': Job.CANCELLED,
		'failed':    Job.FAILED,
		'done':      Job.DONE,
		'cleared':   Job.FAILED
	}


	def __init__(self, config, module, monitor, section):
		WMS.__init__(self, config, module, monitor, 'grid', 'VomsProxy')
		self._sites = config.get('grid', 'sites', '', volatile=True).split()
		self.vo = config.get('grid', 'vo', self.proxy.getVO())

		self._submitParams = {}
		self._ce = config.get(section, 'ce', '', volatile=True)
		self._configVO = config.getPath(section, 'config', '', volatile=True)
		if self._configVO != '' and not os.path.exists(self._configVO):
			raise ConfigError("--config file '%s' does not exist." % self._configVO)


	def _jdlEscape(value):
		repl = { '\\': r'\\', '\"': r'\"', '\n': r'\n' }
		def replace(char):
			try:
				return repl[char]
			except:
				return char
		return '"' + str.join('', map(replace, value)) + '"'
	_jdlEscape = staticmethod(_jdlEscape)


	def storageReq(self, sites):
		def makeMember(member):
			return "Member(%s, other.GlueCESEBindGroupSEUniqueID)" % self._jdlEscape(member)
		if len(sites) == 0:
			return None
		elif len(sites) == 1:
			return makeMember(sites[0])
		else:
			return '(' + str.join(' || ', map(makeMember, sites)) + ')'


	def sitesReq(self, sites):
		sitereqs = []
		fmt = lambda x: "RegExp(%s, other.GlueCEUniqueID)" % self._jdlEscape(x)

		blacklist = filter(lambda x: x.startswith('-'), sites)
		sitereqs.extend(map(lambda x: "!" + fmt(x[1:]), blacklist))

		whitelist = filter(lambda x: not x.startswith('-'), sites)
		if len(whitelist):
			sitereqs.append('(%s)' % str.join(' || ', map(fmt, whitelist)))

		if not len(sitereqs):
			return None
		else:
			return '( ' + str.join(' && ', sitereqs) + ' )'


	def _formatRequirements(self, reqs):
		result = ['other.GlueHostNetworkAdapterOutboundIP']
		for type, arg in reqs:
			if type == self.SOFTWARE:
				result.append('Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' % self._jdlEscape(arg))
			elif type == self.WALLTIME:
				if arg > 0:
					result.append('(other.GlueCEPolicyMaxWallClockTime >= %d)' % int((arg + 59) / 60))
			elif type == self.CPUTIME:
				if arg > 0:
					result.append('(other.GlueCEPolicyMaxCPUTime >= %d)' % int((arg + 59) / 60))
			elif type == self.MEMORY:
				if arg > 0:
					result.append('(other.GlueHostMainMemoryRAMSize >= %d)' % arg)
			elif type == self.STORAGE:
				result.append(self.storageReq(arg))
			elif type == self.SITES:
				result.append(self.sitesReq(arg))
			else:
				raise RuntimeError('unknown requirement type %s or argument %r' % (WMS.reqTypes[type], arg))
		return str.join(' && ', filter(lambda x: x != None, result))


	def getRequirements(self, job):
		reqs = WMS.getRequirements(self, job)
		# add site requirements
		if len(self._sites):
			reqs.append((self.SITES, self._sites))
		return reqs	


	def makeJDL(self, fp, jobNum):
		cfgPath = os.path.join(self.config.workDir, 'jobs', "job_%d.var" % jobNum)
		wcList = filter(lambda x: '*' in x, self.sandboxOut)
		if len(wcList):
			self.writeJobConfig(jobNum, cfgPath, {'GC_WC': str.join(' ', wcList)})
			sandboxOutJDL = filter(lambda x: x not in wcList, self.sandboxOut) + ['GC_WC.tar.gz']
		else:
			self.writeJobConfig(jobNum, cfgPath)
			sandboxOutJDL = self.sandboxOut

		def formatStrList(strList):
			return '{ ' + str.join(', ', map(lambda x: '"%s"' % x, strList)) + ' }'

		contents = {
			'Executable': '"run.sh"',
			'Arguments': '"%d"' % jobNum,
			'StdOutput': '"gc.stdout"',
			'StdError': '"gc.stderr"',
			'InputSandbox': formatStrList(self.sandboxIn + [cfgPath]),
			'OutputSandbox': formatStrList(sandboxOutJDL),
			'Requirements': self._formatRequirements(self.getRequirements(jobNum)),
			'VirtualOrganisation': '"%s"' % self.vo,
			'Rank': '-other.GlueCEStateEstimatedResponseTime',
			'RetryCount': 2
		}
		fp.writelines(utils.DictFormat(' = ').format(contents, format = '%s%s%s;\n'))


	def cleanup(self, list):
		for item in list:
			try:
				if os.path.isdir(item):
					os.rmdir(item)
				else:
					os.unlink(item)
			except:
				pass


	def logError(self, proc, log, jdl = None):
		retCode, stdout, stderr = proc.getAll()
		sys.stderr.write("WARNING: %s failed with code %d\n" %
			(os.path.basename(proc.cmd[0]), retCode))

		now = time.time()
		entry = "%s.%s" % (time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime(now)), ("%.5f" % (now - int(now)))[2:])
		data = { 'retCode': retCode, 'exec': proc.cmd[0], 'args': proc.cmd[1] }
		sys.stderr.writelines(filter(lambda x: (x != '\n') and not x.startswith('----'), stderr))

		tar = tarfile.TarFile.open(os.path.join(self.config.workDir, 'error.tar'), 'a')
		try:
			logcontent = open(log, 'r').readlines()
		except:
			logcontent = []
		
		files = [
			utils.VirtualFile(os.path.join(entry, "log"), logcontent),
			utils.VirtualFile(os.path.join(entry, "info"), utils.DictFormat().format(data)),
			utils.VirtualFile(os.path.join(entry, "stdout"), stdout),
			utils.VirtualFile(os.path.join(entry, "stderr"), stderr)
		]
		if jdl:
			files.append(utils.VirtualFile(os.path.join(entry, "jdl"), jdl))
		for file in files:
			info, handle = file.getTarInfo()
			tar.addfile(info, handle)
			handle.close()
			sys.stderr.write(".")
		tar.close()
		sys.stderr.write("\nAll logfile were moved to %s\n" % os.path.join(self.config.workDir, 'error.tar'))
		return False


	def writeWMSIds(self, ids):
		try:
			fd, jobs = tempfile.mkstemp('.jobids')
			fp = os.fdopen(fd, 'w')
			fp.writelines(str.join('\n', map(lambda (wmsId, jobNum): str(wmsId), ids)))
			fp.close()
		except:
			sys.stderr.write("Could not write wms ids to %s." % jobs)
			raise
		return jobs


	def _parseStatus(self, lines):
		cur = None

		def format(data):
			data = copy.copy(data)
			status = data['status'].lower()
			try:
				if status.find('failed') >=0:
					status='failed'
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
			elif key.startswith('reached') or \
			     key.startswith('submitted'):
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
		buffer = []
		for line in lines:
			bline = line.strip("*\n")
			if bline != '' and ('BOOKKEEPING INFORMATION' not in bline):
				buffer.append(bline)
			if line.startswith("****") and len(buffer):
				remap = { 'destination': 'dest', 'status reason': 'reason',
					'status info for the job': 'id', 'current status': 'status',
					'submitted': 'timestamp', 'reached': 'timestamp', 'exit code': 'gridexit'  }
				data = utils.DictFormat(':').parse(buffer, keyRemap = remap)
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
				buffer = []


	def explainError(self, proc, code):
		if "Keyboard interrupt raised by user" in proc.getError():
			return True
		return False


	# Submit job and yield (jobNum, WMS ID, other data)
	def submitJob(self, jobNum):
		fd, jdl = tempfile.mkstemp('.jdl')
		log = tempfile.mktemp('.log')

		try:
			data = cStringIO.StringIO()
			self.makeJDL(data, jobNum)
			data = data.getvalue()
			fp = os.fdopen(fd, 'w')
			fp.write(data)
			fp.close()
		except:
			sys.stderr.write("Could not write jdl data to %s.\n" % jdl)
			raise

		tmp = filter(lambda (x, y): y != '', self._submitParams.iteritems())
		params = str.join(' ', map(lambda (x, y): "%s %s" % (x, y), tmp))

		activity = utils.ActivityLog('submitting jobs')
		proc = utils.LoggedProcess(self._submitExec, "%s --nomsg --noint --logfile %s %s" %
			(params, utils.shellEscape(log), utils.shellEscape(jdl)))

		wmsId = None
		for line in map(str.strip, proc.iter(self.config.opts)):
			if line.startswith('http'):
				wmsId = line
		retCode = proc.wait()
		del activity

		if (retCode != 0) or (wmsId == None):
			if self.explainError(proc, retCode):
				pass
			else:
				self.logError(proc, log, data)
		self.cleanup([log, jdl])
		return (jobNum, wmsId, {'jdl': data})


	# Check status of jobs and yield (wmsID, status, other data)
	def checkJobs(self, ids):
		if len(ids) == 0:
			raise StopIteration

		idMap = dict(ids)
		jobs = self.writeWMSIds(ids)
		log = tempfile.mktemp('.log')

		activity = utils.ActivityLog("checking job status")
		proc = utils.LoggedProcess(self._statusExec, "--noint --logfile %s -i %s" %
			tuple(map(utils.shellEscape, [log, jobs])))

		for data in self._parseStatus(proc.iter(self.config.opts)):
			data['reason'] = data.get('reason', '')
			yield (idMap[data['id']], data['id'], self._statusMap[data['status']], data)

		retCode = proc.wait()
		del activity

		if retCode != 0:
			if self.explainError(proc, retCode):
				pass
			else:
				self.logError(proc, log)
		self.cleanup([log, jobs])


	# Get output of jobs and yield output dirs
	def getJobsOutput(self, ids):
		if len(ids) == 0:
			raise StopIteration

		basePath = os.path.join(self._outputPath, 'tmp')
		try:
			if len(ids) == 1:
				# For single jobs create single subdir
				tmpPath = os.path.join(basePath, utils.md5(ids[0][0]).hexdigest())
			else:
				tmpPath = basePath
			if not os.path.exists(tmpPath):
				os.makedirs(tmpPath)
		except:
			raise RuntimeError("Temporary path '%s' could not be created." % tmpPath)

		idMap = dict(ids)
		jobs = self.writeWMSIds(ids)
		log = tempfile.mktemp('.log')

		activity = utils.ActivityLog("retrieving job outputs")
		proc = utils.LoggedProcess(self._outputExec, "--noint --logfile %s -i %s --dir %s" %
			tuple(map(utils.shellEscape, [log, jobs, tmpPath])))

		# yield output dirs
		todo = idMap.values()
		currentJobNum = None
		for line in proc.iter(self.config.opts):
			line = line.strip()
			if line.startswith(tmpPath):
				todo.remove(currentJobNum)
				outputDir = line.strip()
				if 'GC_WC.tar.gz' in os.listdir(outputDir):
					wildcardTar = os.path.join(outputDir, 'GC_WC.tar.gz')
					try:
						tarfile.TarFile.open(wildcardTar, 'r:gz').extractall(outputDir)
						os.unlink(wildcardTar)
					except:
						print "Can't unpack output files contained in %s" % wildcardTar
						pass
				yield (currentJobNum, line.strip())
				currentJobNum = None
			else:
				currentJobNum = idMap.get(line, currentJobNum)
		retCode = proc.wait()
		del activity

		if retCode != 0:
			if "Keyboard interrupt raised by user" in proc.getError():
				self.cleanup([log, jobs, basePath])
				raise StopIteration
			else:
				self.logError(proc, log)
			print "Trying to recover from error ..."
			for dir in os.listdir(basePath):
				yield (None, os.path.join(basePath, dir))

		# return unretrievable jobs
		for jobNum in todo:
			yield (jobNum, None)

		self.cleanup([log, jobs, basePath])


	def cancelJobs(self, allIds):
		if len(allIds) == 0:
			raise StopIteration

		waitFlag = False
		for ids in map(lambda x: allIds[x:x+5], range(0, len(allIds), 5)):
			# Delete jobs in groups of 5 - with 5 seconds between groups
			if waitFlag:
				utils.wait(self.config.opts, 5)
			waitFlag = True

			idMap = dict(ids)
			jobs = self.writeWMSIds(ids)
			log = tempfile.mktemp('.log')

			activity = utils.ActivityLog("cancelling jobs")
			proc = utils.LoggedProcess(self._cancelExec, "--noint --logfile %s -i %s" %
				tuple(map(utils.shellEscape, [log, jobs])))
			retCode = proc.wait()
			del activity

			# select cancelled jobs
			for deleted in filter(lambda x: x.startswith('- '), proc.iter(self.config.opts)):
				deleted = deleted.strip('- \n')
				yield (deleted, idMap[deleted])

			if retCode != 0:
				if self.explainError(proc, retCode):
					pass
				else:
					self.logError(proc, log)
			self.cleanup([log, jobs])
