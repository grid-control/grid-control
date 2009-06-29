import sys, os
from grid_control import ConfigError, Job, utils
from wms import WMS
from local_wms import LocalWMSApi

class LSF(LocalWMSApi):
	_statusMap = {
		'PEND':	 Job.QUEUED,  'PSUSP': Job.WAITING,
		'USUSP': Job.WAITING, 'SSUSP': Job.WAITING,
		'RUN':   Job.RUNNING, 'DONE':  Job.DONE,
		'WAIT':  Job.WAITING, 'EXIT':  Job.FAILED,
		# Better options?
		'UNKWN':  Job.FAILED, 'ZOMBI':  Job.FAILED,
	}

	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)

		self.submitExec = utils.searchPathFind('bsub')
		self.statusExec = utils.searchPathFind('bjobs')
		self.cancelExec = utils.searchPathFind('bkill')

		self._queue = config.get('local', 'queue', '')

	def unknownID(self):
		return "is not found"

	def getArguments(self, jobNum, sandbox):
		return sandbox


	def getSubmitArguments(self, jobNum, sandbox):
		# Job name
		params = ' -J %s' % self.wms.getJobName(jobNum)
		# Job queue
		if len(self._queue):
			params += ' -q %s' % self._queue
		# Job time
		reqs = dict(self.wms.getRequirements(jobNum))
		if reqs.has_key(WMS.WALLTIME):
			params += ' -c %d' % ((reqs[WMS.WALLTIME] + 59) / 60)
		# IO paths
		params += ' -o %s -e %s' % (
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))
		return params


	def parseSubmitOutput(self, data):
		#Job <34020017> is submitted to queue <1nh>.
		return "%s.lsf" % str(data.split()[1].strip("<>"))


	def parseStatus(self, status):
		result = []
		for jobline in status.split('\n')[1:]:
			if jobline == '':
				continue
			try:
				tmp = jobline.split()
				jobinfo = {
					'id': "%s.lsf" % tmp[0],
					'user': tmp[1],
					'status': tmp[2],
					'queue': tmp[3],
					'from': tmp[4],
					'dest_host': tmp[5],
					'job_name': tmp[6],
					'submit_time': str.join(" ", tmp[7:10]),
					'dest': 'N/A',
				}
				if jobinfo['dest_host'] != "-":
					jobinfo['dest'] = "%s/%s" % (jobinfo['dest_host'], jobinfo['queue'])
				result.append(jobinfo)
			except:
				print "Error reading job info\n", jobline
				raise
		return result


	def getCheckArgument(self, wmsIds):
		return "-aw %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)
