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
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from python_compat import ifilter, imap, izip, lmap, next

class Host_CheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		CheckJobsWithProcess.__init__(self, config,
			ProcessCreatorAppendArguments(config, 'ps', ['wwup']))

	def _parse_status(self, value, default):
		if 'Z' in value:
			return Job.UNKNOWN
		return Job.RUNNING

	def _parse(self, proc):
		status_iter = proc.stdout.iter(self._timeout)
		head = lmap(lambda x: x.strip('%').lower(), next(status_iter, '').split())
		for entry in imap(str.strip, status_iter):
			job_info = dict(izip(head, ifilter(lambda x: x != '', entry.split(None, len(head) - 1))))
			job_info[CheckInfo.WMSID] = job_info.pop('pid')
			job_info[CheckInfo.RAW_STATUS] = job_info.pop('stat')
			job_info.update({CheckInfo.QUEUE: 'localqueue', CheckInfo.WN: 'localhost'})
			yield job_info

	def _handleError(self, proc):
		self._filter_proc_log(proc, self._errormsg, blacklist = ['Unknown Job Id'], log_empty = False)


class Host_CancelJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config, 'kill', ['-9'], unknownID = 'No such process')

	def _handleError(self, proc):
		self._filter_proc_log(proc, self._errormsg, blacklist = self._blacklist, log_empty = False)


class Host(LocalWMS):
	alias = ['Localhost']
	configSections = LocalWMS.configSections + ['Localhost', 'Host']

	def __init__(self, config, name):
		LocalWMS.__init__(self, config, name,
			submitExec = utils.pathShare('gc-host.sh'),
			checkExecutor = CheckJobsMissingState(config, Host_CheckJobs(config)),
			cancelExecutor = Host_CancelJobs(config))


	def getJobArguments(self, jobNum, sandbox):
		return ''


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		return '%d "%s" "%s" "%s"' % (jobNum, sandbox, stdout, stderr)


	def parseSubmitOutput(self, data):
		return data.strip()
