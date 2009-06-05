import sys, os, popen2, tempfile, shutil
from grid_control import ConfigError, Job, utils
from local_wms import LocalWMS

class LSF(LocalWMS):
	_statusMap = {
		'PEND':	 Job.QUEUED,  'PSUSP': Job.WAITING,
		'USUSP': Job.WAITING, 'SSUSP': Job.WAITING,
		'RUN':   Job.RUNNING, 'DONE':  Job.DONE,
		'WAIT':  Job.WAITING, 'EXIT':  Job.FAILED,
		# Better options?
		'UNKWN':  Job.FAILED, 'ZOMBI':  Job.FAILED,
	}

	def __init__(self, config, module, init):
		LocalWMS.__init__(self, config, module, init)

		self.submitExec = utils.searchPathFind('bsub')
		self.statusExec = utils.searchPathFind('bjobs')
		self.cancelExec = utils.searchPathFind('bkill')

		self._queue = config.get('lsf', 'queue', '')

	def unknownID(self):
		return "is not found"

	def getArguments(self, jobNum, sandbox):
		return sandbox


	def getSubmitArguments(self, jobNum, sandbox):
		# Job name
		params = ' -J %s' % self.getJobName(self.module.taskID, jobNum)
		# Job queue
		if len(self._queue):
			params += ' -q %s' % self._queue
		# Job time
		reqs = dict(self.getRequirements())
		if reqs.has_key(WMS.WALLTIME):
			params += ' -c %d' % ((reqs[WMS.WALLTIME] + 59) / 60)
		# IO paths
		params += ' -o %s -e %s' % (
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))
		return params


	def parseSubmitOutput(self, data):
		#Job <34020017> is submitted to queue <1nh>.
		return data.split()[1].strip("<>")


	def parseStatus(self, status):
		result = []
		for jobline in status[1:]:
			try:
				tmp = jobline.split()
				jobinfo = {
					'id': tmp[0],
					'user': tmp[1],
					'status': tmp[2],
					'queue': tmp[3],
					'from': tmp[4],
					'dest_host': tmp[5],
					'job_name': tmp[6],
					'submit_time': str.join(" ", tmp[7:10]),
				}
				if jobinfo['dest_host'] == "-":
					jobinfo['dest'] = 'N/A'
				else:
					jobinfo['dest'] = "%s/%s" % (jobinfo['dest_host'], jobinfo['queue'])
				result.append(jobinfo)
			except:
				continue
		return result


	def getCheckArgument(self, wmsIds):
		return " -aw %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)
