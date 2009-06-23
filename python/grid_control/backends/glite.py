from __future__ import generators
import sys, os, time, copy, popen2, tempfile, cStringIO
from grid_control import ConfigError, Job, utils
from grid_wms import GridWMS

try:
	from email.utils import parsedate
except ImportError:
	from email.Utils import parsedate

class Glite(GridWMS):
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
		utils.deprecated("Please use the GliteWMS backend for grid jobs!")
		GridWMS.__init__(self, workDir, config, opts, module)

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

			wmsId = None

			for line in proc.fromchild.readlines():
				line = line.strip()
				if line.startswith('http'):
					wmsId = line
			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: %s failed:" % os.path.basename(self._submitExec)
			elif wmsId == None:
				print >> sys.stderr, "WARNING: %s did not yield job id:" % os.path.basename(self._submitExec)

			if wmsId == None and os.path.exists(log):
				sys.stderr.write(open(log, 'r').read())

			# FIXME: glite-job-submit
			return (jobNum, wmsId, {'jdl': data})

		finally:
			self.cleanup([log, jdl])


	def checkJobs(self, ids):
		if not len(ids):
			return []
			#raise StopIteration

		result = []

		try:
			log = tempfile.mktemp('.log')
			jobs = self.writeWMSIds(ids)
			# FIXME: error handling

			activity = utils.ActivityLog("checking job status")

			proc = popen2.Popen3("%s --noint --logfile %s -i %s"
			                     % (self._statusExec,
			                        utils.shellEscape(log),
			                        utils.shellEscape(jobs)), True)

			for data in self._parseStatus(proc.fromchild.readlines()):
				id = data['id']
				data['reason'] = data.get('reason', '')
				status = self._statusMap[data['status']]
				result.append((id, status, data))
				#yield (id, status, data)

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
			self.cleanup([log, jobs])
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

		result = []

		try:
			jobs = self.writeWMSIds(ids)
			log = tempfile.mktemp('.log')

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
			self.cleanup([log, jobs, tmpPath])
		return result


	def cancelJobs(self, ids):
		if not len(ids):
			return True

		result = []

		try:
			jobs = self.writeWMSIds(ids)
			log = tempfile.mktemp('.log')
			# FIXME: error handling

			activity = utils.ActivityLog("cancelling jobs")

			proc = popen2.Popen3("%s --noint --logfile %s -i %s" % \
				(self._cancelExec, utils.shellEscape(log), utils.shellEscape(jobs)), True)

			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: %s failed:" % os.path.basename(self._cancelExec)
				if os.path.exists(log):
					sys.stderr.write(open(log, 'r').read())
				return False

		finally:
			self.cleanup([log, jobs])
		return True
