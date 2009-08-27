import sys, os
from grid_control import ConfigError, Job, utils
from wms import WMS
from local_wms import LocalWMSApi

class PBS(LocalWMSApi):
	_statusMap = {
		'H': Job.SUBMITTED, 'S': Job.SUBMITTED,
		'W': Job.WAITING,   'Q': Job.QUEUED,
		'R': Job.RUNNING,   'C': Job.DONE,
		'E': Job.DONE,      'T': Job.DONE,
		'fail':	Job.FAILED, 'success': Job.SUCCESS
	}

	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)

		self.submitExec = utils.searchPathFind('qsub')
		self.statusExec = utils.searchPathFind('qstat')
		self.cancelExec = utils.searchPathFind('qdel')

		self._group = config.get('local', 'group', '', volatile=True)

	def unknownID(self):
		return "Unknown Job Id"

	def getArguments(self, jobNum, sandbox):
		return ""


	def getSubmitArguments(self, jobNum, sandbox):
		# Job name
		params = ' -N %s' % self.wms.getJobName(jobNum)
		# Job requirements
		reqs = dict(self.wms.getRequirements(jobNum))
		if reqs.has_key(WMS.SITES):
			params += ' -q %s' % reqs[WMS.SITES]
		# Job group
		if len(self._group):
			params += ' -W group_list=%s' % self._group
		# Sandbox
		params += ' -v SANDBOX=%s' % sandbox
		# IO paths
		params += ' -o %s -e %s' % (
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))
		return params


	def parseSubmitOutput(self, data):
		# 1667161.ekpplusctl.ekpplus.cluster
		return data.strip()


	def parseStatus(self, status):
		for section in status.replace("\n\t", "").split("\n\n"):
			if section == '':
				continue
			try:
				lines = section.split('\n')
				jobinfo = utils.DictFormat(' = ').parse(lines[1:])
				jobinfo['id'] = lines[0].split(":")[1].strip()
				jobinfo['status'] = jobinfo.get('job_state')
				jobinfo['dest'] = 'N/A'
				if jobinfo.has_key('exec_host'):
					jobinfo['dest'] = "%s/%s" % (
							jobinfo.get('exec_host').split('/')[0] + "." + jobinfo.get('server', ''),
							jobinfo.get('queue')
						)
			except:
				print "Error reading job info\n", section
				raise
			yield jobinfo


	def getCheckArgument(self, wmsIds):
		return "-f %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)

	
	def getQueues(self):
		defined = lambda (k, v): v != '--'
		toint = lambda e: int(e)

		keys = (WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME)
		func = (toint, utils.parseTime, utils.parseTime)
		parser = dict(zip(keys, func))

		queues = {}
		output = os.popen('qstat -q').readlines()[5:-2]
		for line in output:
			fields = map(str.strip, line.split()[:4])
			queues[fields[0]] = dict(
				map(lambda (key, value): (key, parser[key](value)),
					filter(defined, zip(keys, fields[1:]))))
		return queues
