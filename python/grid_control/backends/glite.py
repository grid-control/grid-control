from __future__ import generators
import sys, os, time, copy, popen2, tempfile, cStringIO
from grid_control import ConfigError, Job, utils
from wms import WMS

try:
	from email.utils import parsedate
except ImportError:
	from email.Utils import parsedate

class Glite(WMS):
	_statusMap = {
		'ready':	Job.READY,
		'submitted':	Job.SUBMITTED,
		'waiting':	Job.WAITING,
		'queued':	Job.QUEUED,
		'scheduled':	Job.QUEUED,
		'running':	Job.RUNNING,
		'aborted':	Job.ABORTED,
		'cancelled':	Job.CANCELLED,
		'failed':	Job.FAILED,
		'done':		Job.DONE,
		'cleared':	Job.SUCCESS
	}

	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, 'grid', init)

		self._submitExec = utils.searchPathFind('glite-job-submit')
		self._statusExec = utils.searchPathFind('glite-job-status')
		self._outputExec = utils.searchPathFind('glite-job-output')
		self._cancelExec = utils.searchPathFind('glite-job-cancel')
		
		self._configVO = config.getPath('glite', 'config-vo', '')
		if self._configVO != '' and not os.path.exists(self._configVO):
			raise ConfigError("--config-vo file '%s' does not exist." % self._configVO)

		try:
			self._ce = config.get('glite', 'ce')
		except:
			self._ce = None


	def _jdlEscape(value):
		repl = { '\\': r'\\', '\"': r'\"', '\n': r'\n' }
		def replace(char):
			try:
				return repl[char]
			except:
				return char
		return '"' + str.join('', map(replace, value)) + '"'
	_jdlEscape = staticmethod(_jdlEscape)


	def memberReq(self, member):
		return 'Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' % self._jdlEscape(member)


	def wallTimeReq(self, wallTime):
		#              GlueCEPolicyMaxWallClockTime: The maximum wall clock time in minutes
		return '(other.GlueCEPolicyMaxWallClockTime >= %d)' % int((wallTime + 59) / 60)


	def storageReq(self, sites):
		def makeMember(member):
			return "Member(%s, other.GlueCESEBindGroupSEUniqueID)" % self._jdlEscape(member)
		if not len(sites):
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


	def cpuTimeReq(self, cpuTime):
		if cpuTime == 0:
			return None
		#              GlueCEPolicyMaxCPUTime: The maximum CPU time available to jobs submitted to this queue, in minutes.
		return '(other.GlueCEPolicyMaxCPUTime >= %d)' % int((cpuTime + 59) / 60)


	def memoryReq(self, memory):
		if memory == 0:
			return None
		return '(other.GlueHostMainMemoryRAMSize >= %d)' % memory


	def otherReq(self, dummyArg):
		return 'other.GlueHostNetworkAdapterOutboundIP'


	def makeJDL(self, fp, job):
		reqs = self.module.getRequirements(job)
		# WMS.OTHER => GlueHostNetworkAdapterOutboundIP
		reqs.append((WMS.OTHER, ()))
		contents = {
			'Executable': 'grid.sh',
			'Arguments': "%d %s" % (job, self.module.getJobArguments(job)),
			'Environment': self.module.getEnvironment(job),
			'StdOutput': 'stdout.txt',
			'StdError': 'stderr.txt',
			'InputSandbox': self.sandboxIn,
			'OutputSandbox': self.sandboxOut,
			'_Requirements': self.formatRequirements(reqs),
			'VirtualOrganisation': self.config.get('grid', 'vo'),
			'RetryCount': 2
		}

		# JDL parameter formatter
		def jdlRep(value):
			if type(value) in (int, long):
				return str(value)
			elif type(value) in (tuple, list):
				return '{ ' + str.join(', ', map(jdlRep, value)) + ' }'
			else:
				return self._jdlEscape(value)

		# write key <-> formatted parameter pairs
		for key, value in contents.items():
			if key[0] == '_':
				key = key[1:]
			else:
				value = jdlRep(value)

			if value != '':
				fp.write("%s = %s;\n" % (key, value))


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


	def submitJob(self, id, job):
		try:
			fd, jdl = tempfile.mkstemp('.jdl')
		except AttributeError:	# Python 2.2 has no tempfile.mkstemp
			while True:
				jdl = tempfile.mktemp('.jdl')
				try:
					fd = os.open(jdl, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
				except OSError:
					continue
				break

		log = tempfile.mktemp('.log')

		try:
			data = cStringIO.StringIO()
			self.makeJDL(data, id)
			data = data.getvalue()

			job.set('jdl', data)

			fp = os.fdopen(fd, 'w')
			fp.write(data)
			fp.close()
			# FIXME: error handling

			params = ''

			if self._configVO != '':
				params += ' --config-vo %s' % utils.shellEscape(self._configVO)
					
			if self._ce != None:
				params += ' -r %s' % utils.shellEscape(self._ce)
				
			activity = utils.ActivityLog('submitting jobs')

			proc = popen2.Popen3("%s%s --nomsg --noint --logfile %s %s"
			                     % (self._submitExec, params,
			                        utils.shellEscape(log),
			                        utils.shellEscape(jdl)), True)

			id = None

			for line in proc.fromchild.readlines():
				line = line.strip()
				if line.startswith('http'):
					id = line
			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: %s failed:" % os.path.basename(self._submitExec)
			elif id == None:
				print >> sys.stderr, "WARNING: %s did not yield job id:" % os.path.basename(self._submitExec)

			if id == None and os.path.exists(log):
				sys.stderr.write(open(log, 'r').read())

			# FIXME: glite-job-submit
			return id

		finally:
			try:
				os.unlink(jdl)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass


	def checkJobs(self, ids):
		if not len(ids):
			return []

		try:
			fd, jobs = tempfile.mkstemp('.jobids')
		except AttributeError:	# Python 2.2 has no tempfile.mkstemp
			while True:
				jobs = tempfile.mktemp('.jobids')
				try:
					fd = os.open(jobs, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
				except OSError:
					continue
				break

		log = tempfile.mktemp('.log')

		result = []

		try:
			fp = os.fdopen(fd, 'w')
			for id in ids:
				fp.write("%s\n" % id)
			fp.close()
			# FIXME: error handling

			activity = utils.ActivityLog("checking job status")

			proc = popen2.Popen3("%s --noint --logfile %s -i %s"
			                     % (self._statusExec,
			                        utils.shellEscape(log),
			                        utils.shellEscape(jobs)), True)

			for data in self._parseStatus(proc.fromchild.readlines()):
				id = data['id']
				status = self._statusMap[data['status']]
				result.append((id, status, data))

			retCode = proc.wait()

			del activity

			if retCode != 0:
				ok = False
				if os.path.exists(log):
					if open(log, 'r').read().find("No Errors found") != -1:
						ok = True
					if not ok:
						#FIXME
						print >> sys.stderr, "WARNING: %s failed:" % os.path.basename(self._statusExec)
						sys.stderr.write(open(log, 'r').read())

		finally:
			try:
				os.unlink(jobs)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass

		return result


	def getJobsOutput(self, ids):
		if not len(ids):
			return []

		tmpPath = os.path.join(self._outputPath, 'tmp')
		try:
			if not os.path.exists(tmpPath):
				os.mkdir(tmpPath)
		except IOError:
			raise RuntimeError("Temporary path '%s' could not be created." % tmpPath)

		try:
			fd, jobs = tempfile.mkstemp('.jobids')
		except AttributeError:	# Python 2.2 has no tempfile.mkstemp
			while True:
				jobs = tempfile.mktemp('.jobids')
				try:
					fd = os.open(jobs, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
				except OSError:
					continue
				break

		log = tempfile.mktemp('.log')

		result = []

		try:
			fp = os.fdopen(fd, 'w')
			for id in ids:
				fp.write("%s\n" % id)
			fp.close()

			# FIXME: error handling

			activity = utils.ActivityLog("retrieving job outputs")

			proc = popen2.Popen3("%s --noint --logfile %s -i %s --dir %s"
			                     % (self._outputExec,
			                        utils.shellEscape(log),
			                        utils.shellEscape(jobs),
			                        utils.shellEscape(tmpPath)),
			                        True)

			for data in proc.fromchild.readlines():
				# FIXME: moep
				pass

			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: %s failed:" % os.path.basename(self._outputExec)
				if os.path.exists(log):
					sys.stderr.write(open(log, 'r').read())

			for file in os.listdir(tmpPath):
				path = os.path.join(tmpPath, file)
				if os.path.isdir(path):
					result.append(path)

		finally:
			try:
				os.unlink(jobs)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass
			try:
				os.rmdir(tmpPath)
			except:
				pass

		return result


	def cancelJobs(self, ids):
		if not len(ids):
			return True

		try:
			fd, jobs = tempfile.mkstemp('.jobids')
		except AttributeError:	# Python 2.2 has no tempfile.mkstemp
			while True:
				jobs = tempfile.mktemp('.jobids')
				try:
					fd = os.open(jobs, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
				except OSError:
					continue
				break

		log = tempfile.mktemp('.log')

		result = []

		try:
			fp = os.fdopen(fd, 'w')
			for id in ids:
				fp.write("%s\n" % id)
			fp.close()
			# FIXME: error handling

			activity = utils.ActivityLog("cancelling jobs")

			proc = popen2.Popen3("%s --noint --logfile %s -i %s"
			                     % (self._cancelExec,
			                        utils.shellEscape(log),
			                        utils.shellEscape(jobs)), True)

			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: %s failed:" % os.path.basename(self._cancelExec)
				if os.path.exists(log):
					sys.stderr.write(open(log, 'r').read())
				return False

		finally:
			try:
				os.unlink(jobs)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass

		return True
