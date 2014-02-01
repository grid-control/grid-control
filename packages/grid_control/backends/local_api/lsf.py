import sys, os, itertools
from python_compat import next
from grid_control import ConfigError, RethrowError, Job, utils
from grid_control.backends import WMS, LocalWMS

class LSF(LocalWMS):
	_statusMap = {
		'PEND':  Job.QUEUED,  'PSUSP': Job.WAITING,
		'USUSP': Job.WAITING, 'SSUSP': Job.WAITING,
		'RUN':   Job.RUNNING, 'DONE':  Job.DONE,
		'WAIT':  Job.WAITING, 'EXIT':  Job.FAILED,
		# Better options?
		'UNKWN': Job.FAILED,  'ZOMBI': Job.FAILED,
	}

	def __init__(self, config, wmsName = None):
		LocalWMS.__init__(self, config, wmsName,
			submitExec = utils.resolveInstallPath('bsub'),
			statusExec = utils.resolveInstallPath('bjobs'),
			cancelExec = utils.resolveInstallPath('bkill'))


	def unknownID(self):
		return 'is not found'


	def getJobArguments(self, jobNum, sandbox):
		return repr(sandbox)


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		# Job name
		params = ' -J %s' % jobName
		# Job requirements
		if WMS.QUEUES in reqs:
			params += ' -q %s' % str.join(',', reqs[WMS.QUEUES])
		if WMS.WALLTIME in reqs:
			params += ' -W %d' % ((reqs[WMS.WALLTIME] + 59) / 60)
		if WMS.CPUTIME in reqs:
			params += ' -c %d' % ((reqs[WMS.CPUTIME] + 59) / 60)
		# IO paths
		params += ' -o "%s" -e "%s"' % (stdout, stderr)
		return params


	def parseSubmitOutput(self, data):
		# Job <34020017> is submitted to queue <1nh>.
		return data.split()[1].strip('<>').strip()


	def parseStatus(self, status):
		next(status)
		tmpHead = ['id', 'user', 'status', 'queue', 'from', 'dest_host', 'job_name']
		for jobline in itertools.ifilter(lambda x: x != '', status):
			try:
				tmp = jobline.split()
				jobinfo = dict(zip(tmpHead, tmp[:7]))
				jobinfo['submit_time'] = str.join(' ', tmp[7:10])
				jobinfo['dest'] = 'N/A'
				if jobinfo['dest_host'] != '-':
					jobinfo['dest'] = '%s/%s' % (jobinfo['dest_host'], jobinfo['queue'])
				yield jobinfo
			except:
				raise RethrowError('Error reading job info:\n%s' % jobline)


	def getCheckArguments(self, wmsIds):
		return '-aw %s' % str.join(' ', wmsIds)


	def getCancelArguments(self, wmsIds):
		return str.join(' ', wmsIds)
