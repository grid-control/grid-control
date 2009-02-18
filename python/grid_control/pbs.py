from __future__ import generators
import sys, os, popen2, tempfile, shutil
from grid_control import ConfigError, WMS, Job, utils

class PBS(WMS):
	_statusMap = {
		'H':	Job.SUBMITTED,
		'S':	Job.SUBMITTED,
		'W':	Job.WAITING,
		'Q':	Job.QUEUED,
		'R':	Job.RUNNING,
		'C':	Job.DONE,
		'E':	Job.DONE,
		'T':	Job.DONE,
		'fail':	Job.FAILED,
		'success':	Job.SUCCESS
	}

	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, 'local', init)

		self._submitExec = utils.searchPathFind('qsub')
		self._statusExec = utils.searchPathFind('qstat')

		self._queue = config.get('pbs', 'queue', '')
		self._group = config.get('pbs', 'group', '')
		self._sandPath = config.getPath('local', 'sandbox path', os.path.join(self.workDir, 'sandbox'))


	def getJobName(self, taskId, jobId):
		return taskID[:10] + "." + str(jobId) #.rjust(4, "0")[:4]


	def submitJob(self, id, job):
		params = ''
		if len(self._queue):
			params += ' -q %s' % self._queue
		if len(self._group):
			params += ' -W group_list=%s' % self._group
		# TODO: fancy job name function
		jobname = self.getJobName(self.module.taskID, id)

		activity = utils.ActivityLog('submitting jobs')

		try:
			sandbox = tempfile.mkdtemp("." + str(id), self.module.taskID + ".", self._sandPath)
			for file in self.sandboxIn:
				shutil.copy(file, sandbox)
			job.set('sandbox', sandbox)
		except IOError:
			raise RuntimeError("Sandbox '%s' could not be prepared." % sandbox)

		env_vars = {
			'ARGS': utils.shellEscape("%d %s" % (id, self.module.getJobArguments(job))),
			'SANDBOX': sandbox
		}
		params += ' -v ' + str.join(",", map(lambda (x,y): x + "=" + y, env_vars.items()))

		proc = popen2.Popen3("%s %s -N %s -o %s -e %s %s" % (
			self._submitExec, params, jobname,
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')),
			utils.shellEscape(utils.atRoot('share', 'local.sh'))), True)

		wmsId = proc.fromchild.read().strip()
		open(os.path.join(sandbox, wmsId), "w")
		retCode = proc.wait()

		del activity

		if retCode != 0:
			print >> sys.stderr, "WARNING: qsub failed:"
		elif wmsId == None:
			print >> sys.stderr, "WARNING: qsub did not yield job id:"

		if wmsId == '':
			sys.stderr.write(proc.childerr)

		return wmsId


	def _parseStatus(self, status):
		current_job = None
		key = None
		value = ""
		result = []
		jobinfo = {}
		status.append("Job Id:")

		for line in status:
			if "Job Id:" in line:
				if current_job != None:
					jobinfo['id'] = current_job
					if jobinfo.has_key('exec_host'):
						jobinfo['dest'] = jobinfo.get('exec_host') + "." + jobinfo.get('server', '')
					else:
						jobinfo['dest'] = 'N/A'
					jobinfo['status'] = jobinfo.get('job_state')
					result.append(jobinfo)
					jobinfo = {}
				current_job = line.split(":")[1].strip()

			# lines beginning with tab are part of the previous value
			if line[0] == '\t':
				value += line.strip()
			else:
				# parse key=value pairs
				if key != None:
					jobinfo[key] = value
				tmp = line.split('=', 1)
				if len(tmp) == 2:
					key = tmp[0].strip()
					value = tmp[1].strip()
				else:
					key = None
		return result


	def checkJobs(self, wmsIds):
		if not len(wmsIds):
			return []

		activity = utils.ActivityLog("checking job status")
		proc = popen2.Popen3("%s -f %s" % (self._statusExec, str.join(" ", wmsIds)), True)

		tmp = {}
		for data in self._parseStatus(proc.fromchild.readlines()):
			# (job number, status, extra info)
			tmp[data['id']] = (data['id'], self._statusMap[data['status']], data)

		result = []
		for wmsId in wmsIds:
			if not tmp.has_key(wmsId):
				result.append((wmsId, Job.DONE, {}))
			else:
				result.append(tmp[wmsId])

		retCode = proc.wait()
		del activity

		if retCode != 0:
			for line in proc.childerr.readlines():
				if not "Unknown Job Id" in line:
					sys.stderr.write(line)

		return result


	def getJobsOutput(self, wmsIds):
		if not len(wmsIds):
			return []

		result = []
		activity = utils.ActivityLog("retrieving job outputs")

		for jobdir in os.listdir(self._sandPath):
			path = os.path.join(self._sandPath, jobdir)
			if os.path.isdir(path):
				for wmsId in wmsIds:
					sandboxfiles = os.listdir(path)
					if wmsId in sandboxfiles:
						# Cleanup sandbox
						for file in sandboxfiles:
							if not file in self.sandboxOut:
								try:
									os.unlink(os.path.join(path, file))
								except:
									pass
						result.append(path)

		del activity
		return result
