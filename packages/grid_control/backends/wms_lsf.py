# | Copyright 2008-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

from grid_control import utils
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from python_compat import izip, next

class LSF(LocalWMS):
	configSections = LocalWMS.configSections + ['LSF']
	_statusMap = {
		'PEND':  Job.QUEUED,  'PSUSP': Job.WAITING,
		'USUSP': Job.WAITING, 'SSUSP': Job.WAITING,
		'RUN':   Job.RUNNING, 'DONE':  Job.DONE,
		'WAIT':  Job.WAITING, 'EXIT':  Job.FAILED,
		# Better options?
		'UNKWN': Job.FAILED,  'ZOMBI': Job.FAILED,
	}

	def __init__(self, config, name):
		LocalWMS.__init__(self, config, name,
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
		for jobline in status:
			if jobline != '':
				try:
					tmp = jobline.split()
					jobinfo = dict(izip(tmpHead, tmp[:7]))
					jobinfo['submit_time'] = str.join(' ', tmp[7:10])
					jobinfo['dest'] = 'N/A'
					if jobinfo['dest_host'] != '-':
						jobinfo['dest'] = '%s/%s' % (jobinfo['dest_host'], jobinfo['queue'])
					yield jobinfo
				except Exception:
					raise BackendError('Error reading job info:\n%s' % jobline)


	def getCheckArguments(self, wmsIds):
		return '-aw %s' % str.join(' ', wmsIds)


	def getCancelArguments(self, wmsIds):
		return str.join(' ', wmsIds)
