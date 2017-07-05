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
from grid_control.backends.wms import BackendError, WMS
from grid_control.job_db import Job
from hpfwk import clear_current_exception
from python_compat import identity, iidfilter, itemgetter


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
		for line in iidfilter(proc.stdout.iter(self._timeout)):
			if 'error' in line.lower():
				raise BackendError('Unable to parse status line %s' % repr(line))
			tmp = line.split()
			try:
				wms_id = str(int(tmp[0]))
			except Exception:
				clear_current_exception()
				continue
			yield {CheckInfo.WMSID: wms_id, CheckInfo.RAW_STATUS: tmp[2], CheckInfo.QUEUE: tmp[1]}


class SLURMSubmit(LocalSubmitWithProcess):
	def __init__(self, config, submit_exec):
		LocalSubmitWithProcess.__init__(self, config, submit_exec, {
			WMS.QUEUES: ('-p', itemgetter(0), identity),  # Queue requirement
		})

	def _get_submit_arguments(self, job_desc, exec_fn, req_list, stdout_fn, stderr_fn):
		# Job name and IO paths
		result = ['-J', job_desc.job_name, '-o', stdout_fn, '-e', stderr_fn]
		return result + self._get_req_arg_list(req_list) + [exec_fn]

	def _parse_submit_output(self, wms_id_str):
		# job_submit: Job 121195 has been submitted.
		return wms_id_str.split('Job ')[1].split()[0].strip()


class SLURM(LocalWMS):
	alias_list = ['']
	config_section_list = LocalWMS.config_section_list + ['SLURM']

	def __init__(self, config, name):
		queue_broker = config.get_composited_plugin('queue broker', pargs=('queue',), cls=Broker,
			bind_kwargs={'inherit': True, 'tags': [self]}, pkwargs={'req_type': WMS.QUEUES},
			default='FilterBroker RandomBroker LimitBroker', default_compositor='MultiBroker')
		LocalWMS.__init__(self, config, name, broker_list=[queue_broker],
			local_submit_executor=SLURMSubmit(config, 'sbatch'),
			check_executor=CheckJobsMissingState(config, SLURMCheckJobs(config)),
			cancel_executor=CancelJobsWithProcessBlind(config, 'scancel', unknown_id='not in queue !'))
