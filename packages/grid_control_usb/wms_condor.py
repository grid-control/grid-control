# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

from grid_control.backends.aspect_cancel import CancelJobsWithProcess
from grid_control.backends.aspect_status import CheckInfo, CheckJobsWithProcess, CheckStatus
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments
from grid_control.job_db import Job
from hpfwk import clear_current_exception
from python_compat import imap


class CondorCancelJobs(CancelJobsWithProcess):
	def __init__(self, config):
		CancelJobsWithProcess.__init__(self, config, ProcessCreatorAppendArguments(config, 'condor_rm'))

	def _parse(self, wms_id_list, proc):  # yield list of wms_id_list
		for line in proc.stdout.iter(self._timeout):
			if 'marked for removal' in line:
				yield (line.split()[1],)


class CondorCheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		CheckJobsWithProcess.__init__(self, config,
			ProcessCreatorAppendArguments(config, 'condor_q', ['-long']), status_map={
				Job.ABORTED: [3],        # removed
				Job.DONE: [4],           # completed
				Job.FAILED: [6],         # submit error
				Job.READY: [1],          # idle (waiting for a machine to execute on)
				Job.RUNNING: [2],
				Job.WAITING: [0, 5, 7],  # unexpanded (never been run); DISABLED (on hold); suspended
			})

	def _handle_error(self, proc):
		if proc.status(timeout=0) and ('Failed to fetch ads' in proc.stderr.read_log()):
			self._status = CheckStatus.ERROR
		CheckJobsWithProcess._handle_error(self, proc)

	def _parse(self, proc):
		job_info = {}
		for line in proc.stdout.iter(self._timeout):
			if not line.strip():
				yield job_info
				job_info = {}
			try:
				(key, value) = imap(str.strip, line.split(' = ', 1))
			except Exception:
				clear_current_exception()
				continue
			if key == 'JobStatus':
				job_info[CheckInfo.RAW_STATUS] = int(value)
			elif key == 'GlobalJobId':
				job_info[CheckInfo.WMSID] = value.split('#')[1]
				job_info[key] = value.strip('"')
			elif key == 'RemoteHost':
				job_info[CheckInfo.WN] = value.strip('"')
			elif 'date' in key.lower():
				job_info[key] = value
		yield job_info
