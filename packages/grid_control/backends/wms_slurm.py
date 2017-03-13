# | Copyright 2009-2017 Karlsruhe Institute of Technology
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
from grid_control.backends.aspect_status import CheckInfo, CheckJobsMissingState, CheckJobsWithProcess  # pylint:disable=line-too-long
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from grid_control.utils import resolve_install_path
from hpfwk import clear_current_exception
from python_compat import identity, ifilter


class SLURMCheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		proc_factory = ProcessCreatorAppendArguments(config,
			'sacct', ['-n', '-o', 'jobid,partition,state,exitcode', '-j'],
			lambda wms_id_list: [str.join(',', wms_id_list)])
		CheckJobsWithProcess.__init__(self, config, proc_factory, status_map={
			Job.ABORTED: ['CANCELLED+', 'NODE_FAIL', 'CANCELLED', 'FAILED'],
			Job.DONE: ['COMPLETED', 'COMPLETING'],
			Job.RUNNING: ['RUNNING'],
			Job.WAITING: ['PENDING'],
		})

	def _parse(self, proc):
		for line in ifilter(identity, proc.stdout.iter(self._timeout)):
			if 'error' in line.lower():
				raise BackendError('Unable to parse status line %s' % repr(line))
			tmp = line.split()
			try:
				wms_id = str(int(tmp[0]))
			except Exception:
				clear_current_exception()
				continue
			yield {CheckInfo.WMSID: wms_id, CheckInfo.RAW_STATUS: tmp[2], CheckInfo.QUEUE: tmp[1]}


class SLURM(LocalWMS):
	config_section_list = LocalWMS.config_section_list + ['SLURM']

	def __init__(self, config, name):
		LocalWMS.__init__(self, config, name,
			submit_exec=resolve_install_path('sbatch'),
			check_executor=CheckJobsMissingState(config, SLURMCheckJobs(config)),
			cancel_executor=CancelJobsWithProcessBlind(config, 'scancel', unknown_id='not in queue !'))

	def parse_submit_output(self, data):
		# job_submit: Job 121195 has been submitted.
		return int(data.split()[3].strip())

	def _get_job_arguments(self, jobnum, sandbox):
		return repr(sandbox)

	def _get_submit_arguments(self, jobnum, job_name, reqs, sandbox, stdout, stderr):
		# Job name
		params = ' -J "%s"' % job_name
		# processes and IO paths
		params += ' -o "%s" -e "%s"' % (stdout, stderr)
		if WMS.QUEUES in reqs:
			params += ' -p %s' % reqs[WMS.QUEUES][0]
		return params
