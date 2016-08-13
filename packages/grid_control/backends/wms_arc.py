# | Copyright 2016 Karlsruhe Institute of Technology
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

from grid_control.backends.aspect_cancel import CancelJobsWithProcessBlind
from grid_control.backends.aspect_status import CheckInfo, CheckJobsWithProcess
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments
from grid_control.job_db import Job
from python_compat import imap

class ARC_CheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		CheckJobsWithProcess.__init__(self, config,
			ProcessCreatorAppendArguments(config, 'arcstat', ['-all']), status_map = {
			'accepted': Job.QUEUED,
			'deleted': Job.ABORTED,
			'failed': Job.ABORTED,
			'finished': Job.DONE,
			'finishing': Job.RUNNING,
			'hold': Job.WAITING,
			'killed': Job.CANCELLED,
			'other': Job.WAITING,
			'preparing': Job.READY,
			'queuing': Job.QUEUED,
			'running': Job.RUNNING,
			'submitting': Job.SUBMITTED,
		})

	def _parse(self, proc):
		job_info = {}
		for line in proc.stdout.iter(self._timeout):
			try:
				(key, value) = imap(str.strip, line.split(':', 1))
			except Exception:
				continue
			key = key.lower()
			if key == 'job':
				yield job_info
				job_info = {CheckInfo.WMSID: value}
			elif key == 'state':
				job_info[CheckInfo.RAW_STATUS] = value.split(' ')[0].lower()
				job_info['state'] = value
			elif value:
				job_info[key] = value
		yield job_info


class ARC_CancelJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config, 'arckill')
