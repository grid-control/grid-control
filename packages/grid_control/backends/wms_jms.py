# | Copyright 2009-2016 Karlsruhe Institute of Technology
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
from grid_control.backends.aspect_cancel import CancelJobsWithProcessBlind
from grid_control.backends.aspect_status import CheckInfo, CheckJobsMissingState, CheckJobsWithProcess
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments
from grid_control.backends.wms import WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from python_compat import identity, ifilter, izip, lmap, next

class JMS_CheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		CheckJobsWithProcess.__init__(self, config,
			ProcessCreatorAppendArguments(config, 'job_queue', ['-l']),
			{'s': Job.QUEUED, 'r': Job.RUNNING, 'CG': Job.DONE, 'w': Job.WAITING})

	def _parse(self, proc):
		tmpHead = [CheckInfo.WMSID, 'user', 'group', 'job_name', CheckInfo.QUEUE, 'partition',
			'nodes', 'cpu_time', 'wall_time', 'memory', 'queue_time', CheckInfo.RAW_STATUS]
		status_iter = ifilter(identity, proc.stdout.iter(self._timeout))
		next(status_iter)
		next(status_iter)
		for line in status_iter:
			tmp = lmap(lambda x: x.strip(), line.replace('\x1b(B', '').replace('\x1b[m', '').split())
			job_info = dict(izip(tmpHead, tmp[:12]))
			if len(tmp) > 12:
				job_info['start_time'] = tmp[12]
			if len(tmp) > 13:
				job_info['kill_time'] = tmp[13]
			if len(tmp) > 14:
				job_info[CheckInfo.WN] = tmp[14]
			yield job_info

	def _handleError(self, proc):
		self._filter_proc_log(proc, self._errormsg, blacklist = ['not in queue', 'tput: No value for $TERM'])


class JMS(LocalWMS):
	configSections = LocalWMS.configSections + ['JMS']

	def __init__(self, config, name):
		LocalWMS.__init__(self, config, name,
			submitExec = utils.resolveInstallPath('job_submit'),
			checkExecutor = CheckJobsMissingState(config, JMS_CheckJobs(config)),
			cancelExecutor = CancelJobsWithProcessBlind(config, 'job_cancel', unknownID = 'not in queue !'))


	def getJobArguments(self, jobNum, sandbox):
		return repr(sandbox)


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		# Job name
		params = ' -J "%s"' % jobName
		# Job requirements
		if WMS.QUEUES in reqs:
			params += ' -c %s' % reqs[WMS.QUEUES][0]
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
