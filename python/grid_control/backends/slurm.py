import sys, os, popen2, tempfile, shutil
from grid_control import ConfigError, Job, utils
from wms import WMS
from local_wms import LocalWMS

class SLURM(LocalWMS):
	_statusMap = { 's': Job.QUEUED, 'r': Job.RUNNING, 'CG': Job.DONE }

	def __init__(self, workDir, config, module, init):
		LocalWMS.__init__(self, workDir, config, module, init)

		self.submitExec = utils.searchPathFind('job_submit')
		self.statusExec = utils.searchPathFind('job_queue')
		self.cancelExec = utils.searchPathFind('job_cancel')

		self._queue = config.get('local', 'queue', '')

	def unknownID(self):
		return "not in queue !"

	def getArguments(self, jobNum, sandbox):
		return sandbox


	def getSubmitArguments(self, jobNum, sandbox):
		# Job name
		params = ' -J %s' % self.getJobName(self.module.taskID, jobNum)
		# Job queue
		if len(self._queue):
			params += ' -c %s' % self._queue
		# Job requirements
		reqs = dict(self.getRequirements(jobNum))
		if reqs.has_key(WMS.WALLTIME):
			params += ' -T %d' % ((reqs[WMS.WALLTIME] + 59) / 60)
		if reqs.has_key(WMS.CPUTIME):
			params += ' -t %d' % ((reqs[WMS.CPUTIME] + 59) / 60)
		if reqs.has_key(WMS.MEMORY):
			params += ' -m %d' % reqs[WMS.MEMORY]
		# processes
		params += ' -p 1'
		# IO paths
		params += ' -o %s -e %s' % (
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))
		return params


	def parseSubmitOutput(self, data):
		# job_submit: Job 121195 has been submitted.
		return "%s.jms" % data.split()[2]


	def parseStatus(self, status):
		result = []
		for jobline in status.split('\n')[2:]:
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
					jobinfo['kill_time'] = tmp[13]
					jobinfo['dest_hosts'] = tmp[14]
					jobinfo['dest'] = "%s.localhost/%s" % (jobinfo['dest_hosts'], jobinfo['queue'])
				result.append(jobinfo)
			except:
				print "Error reading job info\n", jobline
				raise
		return result


	def getCheckArgument(self, wmsIds):
		return "-l %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)
