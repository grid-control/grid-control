import sys, os, tempfile, shutil
from grid_control import ConfigError, Job, utils
from wms import WMS
from local_wms import LocalWMSApi

class SLURM(LocalWMSApi):
	_statusMap = { 's': Job.QUEUED, 'r': Job.RUNNING, 'CG': Job.DONE }

	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)

		self.submitExec = utils.searchPathFind('job_submit')
		self.statusExec = utils.searchPathFind('job_queue')
		self.cancelExec = utils.searchPathFind('job_cancel')


	def unknownID(self):
		return "not in queue !"


	def getArguments(self, jobNum, sandbox):
		return sandbox


	def getSubmitArguments(self, jobNum, sandbox, stdout, stderr):
		# Job name
		params = ' -J %s' % self.wms.getJobName(jobNum)
		# Job requirements
		reqs = dict(self.wms.getRequirements(jobNum))
		if WMS.SITES in reqs:
			params += ' -c %s' % reqs[WMS.SITES]
		if WMS.WALLTIME in reqs:
			params += ' -T %d' % ((reqs[WMS.WALLTIME] + 59) / 60)
		if WMS.CPUTIME in reqs:
			params += ' -t %d' % ((reqs[WMS.CPUTIME] + 59) / 60)
		if WMS.MEMORY in reqs:
			params += ' -m %d' % reqs[WMS.MEMORY]
		# processes and IO paths
		params += ' -p 1 -o %s -e %s' % (stdout, stderr)
		return params


	def parseSubmitOutput(self, data):
		# job_submit: Job 121195 has been submitted.
		return "%s.jms" % data.split()[2]


	def parseStatus(self, status):
		for jobline in str.join('', list(status)).split('\n')[2:]:
			if jobline == '':
				continue
			try:
				tmp = jobline.split()
				jobinfo = {
					'id': "%s.jms" % tmp[0].strip('\x1b(B\x1b[m'),
					'user': tmp[1],
					'group': tmp[2],
					'job_name': tmp[3],
					'queue': tmp[4],
					'partition': tmp[5],
					'nodes': tmp[6],
					'cpu_time': tmp[7],
					'wall_time': tmp[8],
					'memory': tmp[9],
					'queue_time': tmp[10],
					'status': tmp[11],
					'dest': 'N/A',
				}
				if len(tmp) > 12:
					jobinfo['start_time'] = tmp[12]
				if len(tmp) > 13:
					jobinfo['kill_time'] = tmp[13]
				if len(tmp) > 14:
					jobinfo['dest_hosts'] = tmp[14]
					jobinfo['dest'] = "%s.localhost/%s" % (jobinfo['dest_hosts'], jobinfo['queue'])
				yield jobinfo
			except:
				print "Error reading job info\n", jobline
				raise


	def getCheckArgument(self, wmsIds):
		return "-l %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)
