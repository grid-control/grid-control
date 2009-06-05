import sys, os, popen2, tempfile, shutil
from grid_control import ConfigError, Job, utils
from local_wms import LocalWMS

class SGE(LocalWMS):
	_statusMap = {
		'H': Job.SUBMITTED, 'S': Job.SUBMITTED,
		'W': Job.WAITING,   'Q': Job.QUEUED,
		'R': Job.RUNNING,   'C': Job.DONE,
		'E': Job.DONE,      'T': Job.DONE,
		'fail':	Job.FAILED, 'success': Job.SUCCESS
	}

	def __init__(self, config, module, init):
		LocalWMS.__init__(self, config, module, init)

		self.submitExec = utils.searchPathFind('qsub')
		self.statusExec = utils.searchPathFind('qstat')
		self.cancelExec = utils.searchPathFind('qdel')

		self._queue = config.get('pbs', 'queue', '')

	def unknownID(self):
		return "Unknown Job Id"

	def getArguments(self, jobNum, sandbox):
		return ""


	def getSubmitArguments(self, jobNum, sandbox):
		# Job name
		params = ' -N %s' % self.getJobName(self.module.taskID, jobNum)
		# Job queue
		if len(self._queue):
			params += ' -q %s' % self._queue
		# Sandbox
		params += ' -v SANDBOX=%s' % sandbox
		# IO paths
		params += ' -o %s -e %s' % (
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))
		return params


	def parseSubmitOutput(self, data):
		# Your job 424992 ("test.sh") has been submitted
		return data.split()[2]


	def parseStatus(self, status):
		result = []
		for section in str.join('\n', status).replace("\n\t", "").split("\n\n"):
			try:
				lines = section.split('\n')
				jobinfo = DictFormat(':').parse(lines[1:])
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
		return " -j %s" % str.join(",", wmsIds)


	def getCancelArgument(self, wmsIds):
		return str.join(",", wmsIds)
