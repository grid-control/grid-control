import sys, os
from grid_control import ConfigError, RethrowError, Job, utils
from grid_control.backends.wms import WMS
from api import LocalWMSApi

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
		self.nodesExec = utils.searchPathFind('pbsnodes')

		self._group = config.get('local', 'group', '', volatile=True)
		self._server = config.get('local', 'server', '', volatile=True)

	def unknownID(self):
		return "Unknown Job Id"

	def getArguments(self, jobNum, sandbox):
		return ""


	def getSubmitArguments(self, jobNum, sandbox, stdout, stderr, addAttr):
		# Job name
		params = ' -N %s' % self.wms.getJobName(jobNum)
		# Job requirements
		reqs = dict(self.wms.getRequirements(jobNum))
		if WMS.SITES in reqs:
			(queue, nodes) = reqs[WMS.SITES]
			if queue:
				params += ' -q %s' % queue
			if nodes:
				params += ' -l host=%s' % str.join("+", nodes)
		if self.checkReq(reqs, WMS.MEMORY):
			params += ' -l pvmem=%smb' % (reqs[WMS.MEMORY])
		# Job group
		if len(self._group):
			params += ' -W group_list=%s' % self._group
		# Sandbox, IO paths
		params += ' -v GC_SANDBOX=%s -o %s -e %s' % (sandbox, stdout, stderr)
		return params + str.join(' ', map(lambda kv: ' -l %s=%s' % kv, addAttr.items()))


	def parseSubmitOutput(self, data):
		# 1667161.ekpplusctl.ekpplus.cluster
		return data.split('.')[0].strip()


	def parseStatus(self, status):
		for section in utils.accumulate(status, '\n'):
			if section == '':
				continue
			try:
				lines = section.replace("\n\t", "").split('\n')
				jobinfo = utils.DictFormat(' = ').parse(lines[1:])
				jobinfo['id'] = lines[0].split(":")[1].strip()
				jobinfo['status'] = jobinfo.get('job_state')
				jobinfo['dest'] = 'N/A'
				if 'exec_host' in jobinfo:
					jobinfo['dest'] = "%s/%s" % (
						jobinfo.get('exec_host').split('/')[0] + "." + jobinfo.get('server', ''),
						jobinfo.get('queue')
					)
			except:
				raise RethrowError("Error reading job info:\n%s\n" % section)
			yield jobinfo


	def getCheckArgument(self, wmsIds):
		if self._server != '':
			wmsIds = map(lambda x: "%s.%s" % (x, self._server), wmsIds)
		return "-f %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		if self._server != '':
			wmsIds = map(lambda x: "%s.%s" % (x, self._server), wmsIds)
		return str.join(" ", wmsIds)


	def getQueues(self):
		defined = lambda (key, value): value != '--'
		toint = lambda value: int(value)

		keys = (WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME)
		func = (toint, utils.parseTime, utils.parseTime)
		parser = dict(zip(keys, func))

		queues = {}
		output = utils.LoggedProcess(self.statusExec, '-q').getAll()[1][5:-2]
		for line in output:
			fields = map(str.strip, line.split()[:4])
			queues[fields[0]] = dict(
				map(lambda (key, value): (key, parser[key](value)),
					filter(defined, zip(keys, fields[1:]))))
		return queues


	def getNodes(self):
		result = []
		for line in utils.LoggedProcess(self.nodesExec).iter():
			if not line.startswith(" ") and len(line) > 1:
				result.append(line.strip())
		if len(result) > 0:
			return result
		return None
