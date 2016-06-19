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
from grid_control.backends.backend_tools import CheckInfo, CheckJobsViaArguments
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from python_compat import identity, ifilter

class SLURM_CheckJobs(CheckJobsViaArguments):
	def __init__(self, config):
		CheckJobsViaArguments.__init__(self, config)
		self._check_exec = utils.resolveInstallPath('sacct')
		self._status_map = {
			'PENDING': Job.WAITING,    # idle (waiting for a machine to execute on)
			'RUNNING': Job.RUNNING,    # running
			'COMPLETED': Job.DONE,     # running
			'COMPLETING': Job.DONE,    # running
			'CANCELLED+': Job.ABORTED, # removed
			'NODE_FAIL': Job.ABORTED,  # removed
			'CANCELLED': Job.ABORTED,  # removed
			'FAILED': Job.FAILED,      # submit error
		}

	def _arguments(self, wmsIDs):
		return [self._check_exec, '-n', '-o', 'jobid,partition,state,exitcode', '-j', str.join(',', wmsIDs)]

	def _parse(self, proc):
		for line in ifilter(identity, proc.stdout.iter(self._timeout)):
			if 'error' in line.lower():
				raise BackendError('Unable to parse status line %s' % repr(line))
			tmp = line.split()
			try:
				wmsID = str(int(tmp[0]))
			except Exception:
				continue
			yield {CheckInfo.WMSID: wmsID, CheckInfo.RAW_STATUS: tmp[2], CheckInfo.QUEUE: tmp[1]}


class SLURM(LocalWMS):
	configSections = LocalWMS.configSections + ['SLURM']

	def __init__(self, config, wmsName = None):
		LocalWMS.__init__(self, config, wmsName,
			submitExec = utils.resolveInstallPath('sbatch'),
			cancelExec = utils.resolveInstallPath('scancel'),
			checkExecutor = SLURM_CheckJobs(config))


	def unknownID(self):
		return 'not in queue !'


	def getJobArguments(self, jobNum, sandbox):
		return repr(sandbox)


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		# Job name
		params = ' -J "%s"' % jobName
		# processes and IO paths
		params += ' -o "%s" -e "%s"' % (stdout, stderr)
		if WMS.QUEUES in reqs:
			params += ' -p %s' % reqs[WMS.QUEUES][0]
		return params


	def parseSubmitOutput(self, data):
		# job_submit: Job 121195 has been submitted.
		return int(data.split()[3].strip())


	def getCancelArguments(self, wmsIds):
		return str.join(' ', wmsIds)
