from __future__ import generators
import sys, os, time, copy, popen2, tempfile, cStringIO, md5, re
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


	def __init__(self, workDir, config, opts, module):
		WMS.__init__(self, workDir, config, opts, module, 'grid')
		self._sites = config.get('grid', 'sites', '').split()
		self.vo = config.get('grid', 'vo', module.proxy.getVO())
		self._submitParams = {}


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
		def appendSiteItem(list, site):
			if site[0] == ':':
				list.append(site[1:])
			else:
				list.append(site)
		blacklist = []
		whitelist = []
		for site in sites:
			if site[0] == '-':
				appendSiteItem(blacklist, site[1:])
			else:
				appendSiteItem(whitelist, site)

		sitereqs = []
		formatstring = "RegExp(%s, other.GlueCEUniqueID)"
		if len(blacklist):
			sitereqs.extend(map(lambda x: ("!" + formatstring % self._jdlEscape(x)), blacklist))
		if len(whitelist):
			sitereqs.append('(' + str.join(' || ', map(lambda x: (formatstring % self._jdlEscape(x)), whitelist)) + ')')
		if not len(sitereqs):
			return None
		else:
			return '( ' + str.join(' && ', sitereqs) + ' )'


	def _formatRequirements(self, reqs):
		result = []
		for type, arg in reqs:
			if type == self.MEMBER:
				result.append('Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' % self._jdlEscape(arg))
			elif (type == self.WALLTIME) and (arg > 0):
				result.append('(other.GlueCEPolicyMaxWallClockTime >= %d)' % int((arg + 59) / 60))
			elif (type == self.CPUTIME) and (arg > 0):
				result.append('(other.GlueCEPolicyMaxCPUTime >= %d)' % int((arg + 59) / 60))
			elif (type == self.MEMORY) and (arg > 0):
				result.append('(other.GlueHostMainMemoryRAMSize >= %d)' % arg)
			elif type == self.OTHER:
				result.append('other.GlueHostNetworkAdapterOutboundIP')
			elif type == self.STORAGE:
				result.append(self.storageReq(arg))
			elif type == self.SITES:
				result.append(self.sitesReq(arg))
			else:
				raise RuntimeError('unknown requirement type %d' % type)
		return str.join(' && ', result)


	def getRequirements(self, job):
		reqs = WMS.getRequirements(self, job)
		# WMS.OTHER => GlueHostNetworkAdapterOutboundIP
		reqs.append((WMS.OTHER, ()))
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


	def printError(self, cmd, retCode, lines, log, cleanup):
		sys.stderr.write("WARNING: %s failed with code %d\n" % (os.path.basename(cmd), retCode))
		sys.stderr.writelines(filter(lambda x: (x != '\n') and not x.startswith('----'), lines))
#		sys.stderr.write("Logfile can be found here: %s\n\n" % log)
		self.cleanup(cleanup)
		return False


	def submitJob(self, jobNum):
		fd, jdl = tempfile.mkstemp('.jdl')
		log = tempfile.mktemp('.log')

		try:
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
			proc = popen2.Popen3("%s %s --nomsg --noint --logfile %s %s" %
				(self._submitExec, params, utils.shellEscape(log), utils.shellEscape(jdl)), True)
			retCode = proc.wait()

			wmsId = None
			for line in map(str.strip, proc.fromchild.readlines()):
				if line.startswith('http'):
					wmsId = line
			del activity

			if (wmsId == None):
				self.printError(self._submitExec, retCode, proc.childerr.readlines(), log, [jdl])
		except:
			self.cleanup([log, jdl])
			raise
		return (jobNum, wmsId, {'jdl': data})


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


	def iterProc(self, stream):
		while True:
			line = stream.readline()
			if not line:
				break
			yield line


	def checkJobs(self, ids):
		if len(ids) == 0:
			raise StopIteration

		jobs = self.writeWMSIds(ids)
		log = tempfile.mktemp('.log')

		activity = utils.ActivityLog("checking job status")
		proc = popen2.Popen3("%s --noint --logfile %s -i %s" %
			(self._statusExec, utils.shellEscape(log), utils.shellEscape(jobs)), True)

		for data in self._parseStatus(self.iterProc(proc.fromchild)):
			data['reason'] = data.get('reason', '')
			yield (data['id'], self._statusMap[data['status']], data)

		retCode = proc.wait()
		del activity

		if retCode != 0:
			self.printError(self._statusExec, retCode, proc.childerr.readlines(), log, [jobs])
			raise StopIteration
		self.cleanup([log, jobs])


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
		toclean = [log, jobs, basePath]

		activity = utils.ActivityLog("retrieving job outputs")
		proc = popen2.Popen3("%s --noint --logfile %s -i %s --dir %s" %
			tuple([self._outputExec] + map(utils.shellEscape, [log, jobs, tmpPath])), True)

		# yield output dirs
		for line in self.iterProc(proc.fromchild):
			if line.startswith(tmpPath):
				yield line.strip()

		retCode = proc.wait()
		del activity

		if retCode != 0:
			stderr = proc.childerr.readlines()
			self.printError(self._outputExec, retCode, stderr, log, [])
			print "Trying to recover from error ..."
			# TODO: Create fake results for lost jobs...
			# Return leftover (and fake) output directories
			for dir in os.listdir(basePath):
				yield os.path.join(basePath, dir)
			toclean.remove(log)
		self.cleanup(toclean)


	def cancelJobs(self, ids):
		if len(ids) == 0:
			return True

		try:
			log = tempfile.mktemp('.log')
			jobs = self.writeWMSIds(ids)

			activity = utils.ActivityLog("cancelling jobs")
			proc = popen2.Popen4("%s --noint --logfile %s -i %s" %
				(self._cancelExec, utils.shellEscape(log), utils.shellEscape(jobs)))
			retCode = proc.wait()
			del activity

			# select cancelled jobs
			lines = proc.fromchild.readlines()
			deleted = map(lambda x: x.strip('- \n'), filter(lambda x: x.startswith('- '), lines))
			deleted.sort()

			if (deleted != ids):
				sys.stderr.write("Could not delete all jobs!\n")
			if (retCode != 0):
				return self.printError(self._cancelExec, retCode, lines, log, [jobs])
		except:
			self.cleanup([log, jobs])
			raise
		self.cleanup([log, jobs])
		return True
