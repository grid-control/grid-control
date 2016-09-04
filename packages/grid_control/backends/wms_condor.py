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

from grid_control.backends.aspect_cancel import CancelJobsWithProcess
from grid_control.backends.aspect_status import CheckInfo, CheckJobsWithProcess, CheckStatus
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments
from grid_control.job_db import Job
from python_compat import imap

class Condor_CheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		CheckJobsWithProcess.__init__(self, config,
			ProcessCreatorAppendArguments(config, 'condor_q', ['-long']), status_map = {
			0: Job.WAITING,   # unexpanded (never been run)
			1: Job.READY,     # idle (waiting for a machine to execute on)
			2: Job.RUNNING,   # running
			3: Job.ABORTED,   # removed
			4: Job.DONE,      # completed
			5: Job.WAITING,   # DISABLED; on hold
			6: Job.FAILED,    # submit error
			7: Job.WAITING,   # suspended
		})

	def _parse(self, proc):
		job_info = {}
		for line in proc.stdout.iter(self._timeout):
			if not line.strip():
				yield job_info
				job_info = {}
			try:
				(key, value) = imap(str.strip, line.split(' = ', 1))
			except Exception:
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

	def _handleError(self, proc):
		if proc.status(timeout = 0) and ('Failed to fetch ads' in proc.stderr.read_log()):
			self._status = CheckStatus.ERROR
		CheckJobsWithProcess._handleError(self, proc)


class Condor_CancelJobs(CancelJobsWithProcess):
	def __init__(self, config):
		CancelJobsWithProcess.__init__(self, config, ProcessCreatorAppendArguments(config, 'condor_rm'))

	def _parse(self, wmsIDs, proc): # yield list of wmsIDs
		for line in proc.stdout.iter(self._timeout):
			if 'marked for removal' in line:
				yield (line.split()[1],)
