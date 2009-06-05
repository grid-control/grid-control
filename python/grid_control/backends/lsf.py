import sys, os, popen2, tempfile, shutil
from grid_control import ConfigError, Job, utils
from local_wms import LocalWMS

class LSF(LocalWMS):
	_statusMap = {
		'PEND':	Job.QUEUED,
		'RUN':	Job.RUNNING,
		'aborted':	Job.ABORTED,
		'cancelled':	Job.CANCELLED,
		'EXIT':	Job.FAILED,
		'DONE':		Job.DONE,
		'cleared':	Job.SUCCESS
	}

	def __init__(self, config, module, init):
		LocalWMS.__init__(self, config, module, init)

		self.submitExec = utils.searchPathFind('bsub')
		self.statusExec = utils.searchPathFind('bjobs')
		self.cancelExec = utils.searchPathFind('bkill')

		self._queue = config.get('lsf', 'queue', '')


	def getArguments(self, jobNum, sandbox):
		return sandbox


	def getSubmitArguments(self, jobNum, sandbox):
		# Job name
		params = ' -J %s' % self.getJobName(self.module.taskID, jobNum)
		# Job queue
		if len(self._queue):
			params += ' -q %s' % self._queue
		# Job time
		if len(self._queue):
			params += ' -c %d' % 20000
		# IO paths
		params += ' -o %s -e %s' % (
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))
		return params


	def parseSubmitOutput(self, data):
		return data.strip()


	def parseStatus(self, status):
		raise RuntimeError('parseStatus not yet implemented!')
		result = []
		for section in status.replace("\n\t", "").split("\n\n"):
			lines = section.split('\n')
			try:
				jobinfo = DictFormat(' = ').parse(lines[1:])
				jobinfo['id'] = lines[0].split(":")[1].strip()
			except:
				continue
			if jobinfo.has_key('exec_host'):
				jobinfo['dest'] = jobinfo.get('exec_host') + "." + jobinfo.get('server', '')
			else:
				jobinfo['dest'] = 'N/A'
			jobinfo['status'] = jobinfo.get('job_state')
			result.append(jobinfo)
		return result


	def getCheckArgument(self, wmsIds):
		return " -a %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)
