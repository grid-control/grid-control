import sys, os, shutil
from grid_control import ConfigError, RethrowError, Job, utils
from grid_control.backends.wms import WMS
from api import LocalWMSApi

class JMS(LocalWMSApi):
	_statusMap = { 's': Job.QUEUED, 'r': Job.RUNNING, 'CG': Job.DONE, 'w': Job.WAITING }

	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)
		self.submitExec = utils.resolveInstallPath('job_submit')
		self.statusExec = utils.resolveInstallPath('job_queue')
		self.cancelExec = utils.resolveInstallPath('job_cancel')


	def unknownID(self):
		return 'not in queue !'


	def getJobArguments(self, jobNum, sandbox):
		return repr(sandbox)


	def getSubmitArguments(self, jobNum, sandbox, stdout, stderr, addAttr):
		# Job name
		params = ' -J "%s"' % self.wms.getJobName(jobNum)
		# Job requirements
		reqs = dict(self.wms.getRequirements(jobNum))
		if WMS.SITES in reqs:
			params += ' -c %s' % reqs[WMS.SITES][0]
		if self.checkReq(reqs, WMS.WALLTIME):
			params += ' -T %d' % ((reqs[WMS.WALLTIME] + 59) / 60)
		if self.checkReq(reqs, WMS.CPUTIME):
			params += ' -t %d' % ((reqs[WMS.CPUTIME] + 59) / 60)
		if self.checkReq(reqs, WMS.MEMORY):
			params += ' -m %d' % reqs[WMS.MEMORY]
		# processes and IO paths
		params += ' -p 1 -o "%s" -e "%s"' % (stdout, stderr)
		return params


	def parseSubmitOutput(self, data):
		# job_submit: Job 121195 has been submitted.
		return data.split()[2].strip()


	def parseStatus(self, status):
		tmpHead = ['id', 'user', 'group', 'job_name', 'queue', 'partition',
			'nodes', 'cpu_time', 'wall_time', 'memory', 'queue_time', 'status']
		for jobline in str.join('', list(status)).split('\n')[2:]:
			if jobline == '':
				continue
			try:
				tmp = map(lambda x: x.strip('\x1b(B'), jobline.replace('\x1b[m', '').split())
				jobinfo = dict(zip(tmpHead, tmp[:12]))
				jobinfo['dest'] = 'N/A'
				if len(tmp) > 12:
					jobinfo['start_time'] = tmp[12]
				if len(tmp) > 13:
					jobinfo['kill_time'] = tmp[13]
				if len(tmp) > 14:
					jobinfo['dest_hosts'] = tmp[14]
					jobinfo['dest'] = '%s.localhost/%s' % (jobinfo['dest_hosts'], jobinfo['queue'])
				yield jobinfo
			except:
				raise RethrowError('Error reading job info:\n%s' % jobline)


	def getCheckArguments(self, wmsIds):
		return '-l %s' % str.join(' ', wmsIds)


	def getCancelArguments(self, wmsIds):
		return str.join(' ', wmsIds)


class SLURM(JMS):
	pass
