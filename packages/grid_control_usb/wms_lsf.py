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
from grid_control.backends.aspect_submit import LocalSubmitWithProcess
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments
from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import BackendError, WMS
from grid_control.job_db import Job
from grid_control.utils import resolve_install_path
from grid_control_usb.wms_local import LocalWMS
from python_compat import identity, iidfilter, izip, next


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

	def _handle_error(self, log, proc):
		self._filter_proc_log(log, proc, self._errormsg, blacklist=['is not found'])

	def _parse(self, proc):
		status_iter = proc.stdout.iter(timeout=self._timeout)
		next(status_iter)
		tmp_head = [CheckInfo.WMSID, 'user', CheckInfo.RAW_STATUS,
			CheckInfo.QUEUE, 'from', CheckInfo.WN, 'job_name']
		for line in iidfilter(status_iter):
			try:
				tmp = line.split()
				job_info = dict(izip(tmp_head, tmp[:7]))
				job_info['submit_time'] = str.join(' ', tmp[7:10])
				yield job_info
			except Exception:
				raise BackendError('Error reading job info:\n%s' % line)


class LSFSubmit(LocalSubmitWithProcess):
	def __init__(self, config, submit_exec):
		LocalSubmitWithProcess.__init__(self, config, submit_exec, {
			WMS.QUEUES: ('-q', lambda queue_list: str.join(',', queue_list), identity),
			WMS.WALLTIME: ('-W', lambda walltime: int((walltime + 59) / 60), identity),
			WMS.CPUTIME: ('-c', lambda cputime: int((cputime + 59) / 60), identity),
		})

	def _get_submit_arguments(self, job_desc, exec_fn, req_list, stdout_fn, stderr_fn):
		# Job name and IO paths
		arg_list = ['-J', job_desc.job_name, '-o', stdout_fn, '-e', stderr_fn]
		return arg_list + self._get_req_arg_list(req_list) + [exec_fn]

	def _parse_submit_output(self, wms_id_str):
		# Job <34020017> is submitted to queue <1nh>.
		return wms_id_str.split()[1].strip('<>').strip()


class LSF(LocalWMS):
	alias_list = ['']
	config_section_list = LocalWMS.config_section_list + ['LSF']

	def __init__(self, config, name):
		queue_broker = config.get_composited_plugin('queue broker', pargs=('queue',), cls=Broker,
			bind_kwargs={'inherit': True, 'tags': [self]}, pkwargs={'req_type': WMS.QUEUES},
			default='FilterBroker RandomBroker', default_compositor='MultiBroker')
		LocalWMS.__init__(self, config, name, broker_list=[queue_broker],
			local_submit_executor=LSFSubmit(config, 'bsub'),
			cancel_executor=LSFCancelJobs(config),
			check_executor=CheckJobsMissingState(config, LSFCheckJobs(config)))
