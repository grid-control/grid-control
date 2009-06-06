from __future__ import generators
import sys, os, popen2, tempfile, shutil
from grid_control import ConfigError, Job, utils
from local_wms import LocalWMS

class Slurm(LocalWMS):
	_statusMap = {
		'H':	Job.SUBMITTED,
		'S':	Job.SUBMITTED,
		'W':	Job.WAITING,
		'Q':	Job.QUEUED,
		'R':	Job.RUNNING,
		'C':	Job.DONE,
		'E':	Job.DONE,
		'T':	Job.DONE,
		'fail':	Job.FAILED,
		'success':	Job.SUCCESS
	}

	def __init__(self, config, module, init):
		LocalWMS.__init__(self, config, module, init)

		self.submitExec = utils.searchPathFind('job_submit')
		self.statusExec = utils.searchPathFind('job_queue')
		self.cancelExec = utils.searchPathFind('job_cancel')

		self._queue = config.get('local', 'queue', '')
		self._group = config.get('local', 'group', '')


	def getSubmitArguments(self, id, env_vars, sandbox):
		# Job name
		params = ' -N %s' % self.getJobName(self.module.taskID, id)
		# Job queue
		if len(self._queue):
			params += ' -q %s' % self._queue
		# Job group
		if len(self._group):
			params += ' -W group_list=%s' % self._group
		# Job env
		params += ' -v ' + str.join(",", map(lambda (x,y): x + "=" + y, env_vars.items()))
		# IO paths
		params += ' -o %s -e %s' % (
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))
		return params


	def parseSubmitOutput(self, data):
		return data.strip()


	def parseStatus(self, status):
		current_job = None
		key = None
		value = ""
		result = []
		jobinfo = {}
		status.append("Job Id:")

		for line in status:
			if "Job Id:" in line:
				if current_job != None:
					jobinfo['id'] = current_job
					if jobinfo.has_key('exec_host'):
						jobinfo['dest'] = jobinfo.get('exec_host') + "." + jobinfo.get('server', '')
					else:
						jobinfo['dest'] = 'N/A'
					jobinfo['status'] = jobinfo.get('job_state')
					result.append(jobinfo)
					jobinfo = {}
				current_job = line.split(":")[1].strip()

			# lines beginning with tab are part of the previous value
			if line[0] == '\t':
				value += line.strip()
			else:
				# parse key=value pairs
				if key != None:
					jobinfo[key] = value
				tmp = line.split('=', 1)
				if len(tmp) == 2:
					key = tmp[0].strip()
					value = tmp[1].strip()
				else:
					key = None
		return result


	def getCheckArgument(self, wmsIds):
		return " -f %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)
