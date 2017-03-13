# | Copyright 2008-2017 Karlsruhe Institute of Technology
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
from python_compat import identity, ifilter, izip, next


class LSFCancelJobs(CancelJobsWithProcessBlind):
	def __init__(self, config):
		CancelJobsWithProcessBlind.__init__(self, config, 'bkill', unknown_id='is not found')


class LSFCheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		CheckJobsWithProcess.__init__(self, config,
			ProcessCreatorAppendArguments(config, 'bjobs', ['-aw']), status_map={
				Job.DONE: ['DONE', 'EXIT', 'UNKWN', 'ZOMBI'],
				Job.QUEUED: ['PEND'],
				Job.RUNNING: ['RUN'],
				Job.WAITING: ['PSUSP', 'USUSP', 'SSUSP', 'WAIT'],
			})

	def _handle_error(self, proc):
		self._filter_proc_log(proc, self._errormsg, blacklist=['is not found'])

	def _parse(self, proc):
		status_iter = proc.stdout.iter(timeout=self._timeout)
		next(status_iter)
		tmp_head = [CheckInfo.WMSID, 'user', CheckInfo.RAW_STATUS,
			CheckInfo.QUEUE, 'from', CheckInfo.WN, 'job_name']
		for line in ifilter(identity, status_iter):
			try:
				tmp = line.split()
				job_info = dict(izip(tmp_head, tmp[:7]))
				job_info['submit_time'] = str.join(' ', tmp[7:10])
				yield job_info
			except Exception:
				raise BackendError('Error reading job info:\n%s' % line)


class LSF(LocalWMS):
	config_section_list = LocalWMS.config_section_list + ['LSF']

	def __init__(self, config, name):
		LocalWMS.__init__(self, config, name,
			submit_exec=resolve_install_path('bsub'),
			cancel_executor=LSFCancelJobs(config),
			check_executor=CheckJobsMissingState(config, LSFCheckJobs(config)))

	def parse_submit_output(self, data):
		# Job <34020017> is submitted to queue <1nh>.
		return data.split()[1].strip('<>').strip()

	def _get_job_arguments(self, jobnum, sandbox):
		return repr(sandbox)

	def _get_submit_arguments(self, jobnum, job_name, reqs, sandbox, stdout, stderr):
		# Job name
		params = ' -J %s' % job_name
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
