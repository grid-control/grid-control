import sys, os, time, copy, popen2, tempfile, cStringIO, md5, re, tarfile, gzip
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
		'cleared':   Job.SUCCESS
	}


	def __init__(self, config, opts, module, section):
		WMS.__init__(self, config, opts, module, 'grid')
		self._sites = config.get('grid', 'sites', '').split()
		self.vo = config.get('grid', 'vo', module.proxy.getVO())

		self._submitParams = {}
		self._ce = config.get(section, 'ce', '')
		self._configVO = config.getPath(section, 'config', '')
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
		formatstring = "RegExp(%s, other.GlueCEUniqueID)"

		blacklist = filter(lambda x: x.startswith('-'), sites)
		sitereqs.extend(map(lambda x: ("!" + formatstring % self._jdlEscape(x[1:])), blacklist))

		whitelist = filter(lambda x: not x.startswith('-'), sites)
		if len(whitelist):
			sitereqs.append('(%s)' % str.join(' || ', map(lambda x: (formatstring % self._jdlEscape(x)), whitelist)))

		if not len(sitereqs):
			return None
		else:
			return '( ' + str.join(' && ', sitereqs) + ' )'


	def _formatRequirements(self, reqs):
		result = ['other.GlueHostNetworkAdapterOutboundIP']
		for type, arg in reqs:
			if type == self.MEMBER:
				result.append('Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' % self._jdlEscape(arg))
			elif (type == self.WALLTIME) and (arg > 0):
				result.append('(other.GlueCEPolicyMaxWallClockTime >= %d)' % int((arg + 59) / 60))
			elif (type == self.CPUTIME) and (arg > 0):
				result.append('(other.GlueCEPolicyMaxCPUTime >= %d)' % int((arg + 59) / 60))
			elif (type == self.MEMORY) and (arg > 0):
				result.append('(other.GlueHostMainMemoryRAMSize >= %d)' % arg)
			elif type == self.STORAGE:
				result.append(self.storageReq(arg))
			elif type == self.SITES:
				result.append(self.sitesReq(arg))
			else:
				raise RuntimeError('unknown requirement type %d' % type)
		return str.join(' && ', result)


	def getRequirements(self, job):
		reqs = WMS.getRequirements(self, job)
		# add site requirements
		if len(self._sites):
			reqs.append((self.SITES, self._sites))
		return reqs	


	def makeJDL(self, fp, job):
		contents = {
			'Executable': 'grid.sh',
			'Arguments': "%d %s" % (job, self.module.getJobArguments(job)),
			'Environment': utils.DictFormat().format(self.module.getJobConfig(job), format = '%s%s%s'),
			'StdOutput': 'stdout.txt',
			'StdError': 'stderr.txt',
			'InputSandbox': self.sandboxIn,
			'OutputSandbox': self.sandboxOut,
			'_Requirements': self._formatRequirements(self.getRequirements(job)),
			'VirtualOrganisation': self.vo,
			'RetryCount': 2
		}

		# JDL parameter formatter
		def jdlRep((key, delim, value)):
			# _KEY is marker for already formatted text
			if key[0] == '_':
				return (key[1:], delim, value)
			elif type(value) in (int, long):
				return (key, delim, value)
			elif type(value) in (tuple, list):
				recursiveResult = map(lambda x: jdlRep((key, delim, x)), value)
				return (key, delim, '{ ' + str.join(', ', map(lambda (k,d,v): v, recursiveResult)) + ' }')
			else:
				return (key, delim, '"%s"' % value)

		fp.writelines(utils.DictFormat().format(contents, format = '%s %s %s;\n', fkt = jdlRep))


	def cleanup(self, list):
		for item in list:
			try:
				if os.path.isdir(item):
					os.rmdir(item)
				else:
					os.unlink(item)
			except:
				pass


	class LoggedProcess(object):
		def __init__(self, cmd, args):
			self.cmd = (cmd, args)
			self.proc = popen2.Popen3("%s %s" % (cmd, args), True)
			self.stdout = []
			self.stderr = []

		def getError(self):
			self.stderr.extend(self.proc.childerr.readlines())
			return str.join("\n", self.stderr)

		def iter(self, opts):
			while True:
				try:
					line = self.proc.fromchild.readline()
				except:
					opts.abort = True
					break
				if not line:
					break
				self.stdout.append(line)
				yield line

		def wait(self):
			return self.proc.wait()

		def getOutput(self):
			self.stdout.extend(self.proc.fromchild.readline())
			self.stderr.extend(self.proc.childerr.readlines())
			return (self.wait(), self.stdout, self.stderr)


	def logError(self, proc, log):
		retCode, stdout, stderr = proc.getOutput()
		sys.stderr.write("WARNING: %s failed with code %d\n" %
			(os.path.basename(proc.cmd[0]), retCode))

		now = time.time()
		entry = "%s.%s" % (time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime(now)), ("%.5f" % (now - int(now)))[2:])
		data = { 'retCode': retCode, 'exec': proc.cmd[0], 'args': proc.cmd[1] }

		tar = tarfile.TarFile.open(os.path.join(self.opts.workDir, 'error.tar'), 'a')
		try:
			logcontent = open(log, 'r').readlines()
		except:
			logcontent = []
		for file in [
			utils.VirtualFile(os.path.join(entry, "log"), logcontent),
			utils.VirtualFile(os.path.join(entry, "info"), utils.DictFormat().format(data)),
			utils.VirtualFile(os.path.join(entry, "stdout"), stdout),
			utils.VirtualFile(os.path.join(entry, "stderr"), stderr)
		]:
			info, handle = file.getTarInfo()
			tar.addfile(info, handle)
			handle.close()
		tar.close()

		sys.stderr.writelines(filter(lambda x: (x != '\n') and not x.startswith('----'), stderr))
#		sys.stderr.write("Logfile can be found here: %s\n\n" % log)
		return False


	def writeWMSIds(self, wmsIds):
		try:
			fd, jobs = tempfile.mkstemp('.jobids')
			fp = os.fdopen(fd, 'w')
			fp.writelines(str.join('\n', wmsIds))
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
			sys.stderr.write("Could not write jdl data to %s." % jdl.name)
			raise

		tmp = filter(lambda (x,y): y != '', self._submitParams.iteritems())
		params = str.join(' ', map(lambda (x,y): "%s %s" % (x, y), tmp))

		activity = utils.ActivityLog('submitting jobs')
		proc = GridWMS.LoggedProcess(self._submitExec, "%s --nomsg --noint --logfile %s %s" %
			(params, utils.shellEscape(log), utils.shellEscape(jdl)))

		wmsId = None
		for line in map(str.strip, proc.iter(self.opts)):
			if line.startswith('http'):
				wmsId = line
		retCode = proc.wait()
		del activity

		if (retCode != 0) or (wmsId == None):
			if "Keyboard interrupt raised by user" in proc.getError():
				pass
			else:
				self.logError(proc, log)
		self.cleanup([log, jdl])
		return (jobNum, wmsId, {'jdl': data})


	# Check status of jobs and yield (wmsID, status, other data)
	def checkJobs(self, ids):
		if len(ids) == 0:
			raise StopIteration

		jobs = self.writeWMSIds(ids)
		log = tempfile.mktemp('.log')

		activity = utils.ActivityLog("checking job status")
		proc = GridWMS.LoggedProcess(self._statusExec, "--noint --logfile %s -i %s" %
			tuple(map(utils.shellEscape, [log, jobs])))

		for data in self._parseStatusX(proc.iter(self.opts)):
			data['reason'] = data.get('reason', '')
			yield (data['id'], self._statusMap[data['status']], data)

		retCode = proc.wait()
		del activity

		if retCode != 0:
			if "Keyboard interrupt raised by user" in proc.getError():
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
				tmpPath = os.path.join(basePath, md5.md5(ids[0]).hexdigest())
			else:
				tmpPath = basePath
			if not os.path.exists(tmpPath):
				os.makedirs(tmpPath)
		except:
			raise RuntimeError("Temporary path '%s' could not be created." % tmpPath)

		jobs = self.writeWMSIds(ids)
		log = tempfile.mktemp('.log')

		activity = utils.ActivityLog("retrieving job outputs")
		proc = GridWMS.LoggedProcess(self._outputExec, "--noint --logfile %s -i %s --dir %s" %
			tuple(map(utils.shellEscape, [log, jobs, tmpPath])))

		# yield output dirs
		for line in proc.iter(self.opts):
			if line.startswith(tmpPath):
				yield line.strip()

		retCode = proc.wait()
		del activity

		if retCode != 0:
			if "Keyboard interrupt raised by user" in proc.getError():
				self.cleanup([log, jobs, basePath])
				raise StopIteration
			else:
				self.logError(proc, log)
			print "Trying to recover from error ..."
			# TODO: Create fake results for lost jobs...
			# Return leftover (and fake) output directories
			for dir in os.listdir(basePath):
				yield os.path.join(basePath, dir)
		self.cleanup([log, jobs, basePath])


	def cancelJobs(self, ids):
		if len(ids) == 0:
			return True

		log = tempfile.mktemp('.log')
		jobs = self.writeWMSIds(ids)

		activity = utils.ActivityLog("cancelling jobs")
		proc = GridWMS.LoggedProcess(self._cancelExec, "--noint --logfile %s -i %s" %
			tuple(map(utils.shellEscape, [log, jobs])))
		retCode = proc.wait()
		del activity

		# select cancelled jobs
		deleted = map(lambda x: x.strip('- \n'), filter(lambda x: x.startswith('- '), proc.iter(self.opts)))

		if len(deleted) != len(ids):
			sys.stderr.write("Could not delete all jobs!\n")
		if retCode != 0:
			if "Keyboard interrupt raised by user" in proc.getError():
				pass
			else:
				self.logError(proc, log)
		self.cleanup([log, jobs])
		return True
