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
from grid_control.backends.aspect_submit import LocalSubmitWithProcess
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments
from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import WMS
from grid_control.job_db import Job
from grid_control.utils import resolve_install_path
from grid_control_usb.wms_local import LocalWMS
from python_compat import identity, iidfilter, itemgetter, izip, lmap, next


class JMSCheckJobs(CheckJobsWithProcess):
	def __init__(self, config):
		CheckJobsWithProcess.__init__(self, config,
			ProcessCreatorAppendArguments(config, 'job_queue', ['-l']),
			{Job.QUEUED: ['s'], Job.RUNNING: ['r'], Job.DONE: ['CG'], Job.WAITING: ['w']})

	def _handle_error(self, log, proc):
		self._filter_proc_log(log, proc, self._errormsg,
			blacklist=['not in queue', 'tput: No value for $TERM'])

	def _parse(self, proc):
		tmp_head = [CheckInfo.WMSID, 'user', 'group', 'job_name', CheckInfo.QUEUE, 'partition',
			'nodes', 'cpu_time', 'wall_time', 'memory', 'queue_time', CheckInfo.RAW_STATUS]
		status_iter = iidfilter(proc.stdout.iter(self._timeout))
		next(status_iter)
		next(status_iter)
		for line in status_iter:
			tmp = lmap(lambda x: x.strip(), line.replace('\x1b(B', '').replace('\x1b[m', '').split())
			job_info = dict(izip(tmp_head, tmp[:12]))
			if len(tmp) > 12:
				job_info['start_time'] = tmp[12]
			if len(tmp) > 13:
				job_info['kill_time'] = tmp[13]
			if len(tmp) > 14:
				job_info[CheckInfo.WN] = tmp[14]
			yield job_info


class JMSSubmit(LocalSubmitWithProcess):
	def __init__(self, config, submit_exec):
		LocalSubmitWithProcess.__init__(self, config, submit_exec, {
			WMS.QUEUES: ('-c', itemgetter(0), identity),
			WMS.WALLTIME: ('-T', lambda walltime: int((walltime + 59) / 60), identity),
			WMS.CPUTIME: ('-t', lambda cputime: int((cputime + 59) / 60), identity),
			WMS.MEMORY: ('-m', identity, identity),
		})

	def _get_submit_arguments(self, job_desc, exec_fn, req_list, stdout_fn, stderr_fn):
		# Job name, processes and IO paths
		arg_list = ['-J', job_desc.job_name, '-p', 1, '-o', stdout_fn, '-e', stderr_fn]
		return arg_list + self._get_req_arg_list(req_list) + [exec_fn]

	def _parse_submit_output(self, wms_id_str):
		# job_submit: Job 121195 has been submitted.
		return wms_id_str.split()[2].strip()


class JMS(LocalWMS):
	alias_list = ['']
	config_section_list = LocalWMS.config_section_list + ['JMS']

	def __init__(self, config, name):
		queue_broker = config.get_composited_plugin('queue broker', pargs=('queue',), cls=Broker,
			bind_kwargs={'inherit': True, 'tags': [self]}, pkwargs={'req_type': WMS.QUEUES},
			default='FilterBroker RandomBroker LimitBroker', default_compositor='MultiBroker')
		LocalWMS.__init__(self, config, name, broker_list=[queue_broker],
			local_submit_executor=JMSSubmit(config, 'job_submit'),
			check_executor=CheckJobsMissingState(config, JMSCheckJobs(config)),
			cancel_executor=CancelJobsWithProcessBlind(config, 'job_cancel', unknown_id='not in queue !'))
