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
from grid_control.backends.backend_tools import BackendDiscovery, ProcessCreatorAppendArguments
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_pbsge import PBSGECommon
from grid_control.job_db import Job
from grid_control.utils import DictFormat, accumulate, resolve_install_path
from grid_control.utils.parsing import parse_time
from grid_control.utils.process_base import LocalProcess
from python_compat import identity, ifilter, izip, lmap


class PBSDiscoverNodes(BackendDiscovery):
	def __init__(self, config):
		BackendDiscovery.__init__(self, config)
		self._timeout = config.get_time('discovery timeout', 30, on_change=None)
		self._exec = resolve_install_path('pbsnodes')

	def discover(self):
		proc = LocalProcess(self._exec)
		for line in proc.stdout.iter(timeout=self._timeout):
			if not line.startswith(' ') and len(line) > 1:
				node = line.strip()
			if ('state = ' in line) and ('down' not in line) and ('offline' not in line):
				yield {'name': node}
		proc.status_raise(timeout=0)


class PBSDiscoverQueues(BackendDiscovery):
	def __init__(self, config):
		BackendDiscovery.__init__(self, config)
		self._exec = resolve_install_path('qstat')

	def discover(self):
		active = False
		keys = [WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME]
		parser = dict(izip(keys, [int, parse_time, parse_time]))
		proc = LocalProcess(self._exec, '-q')
		for line in proc.stdout.iter(timeout=10):
			if line.startswith('-'):
				active = True
			elif line.startswith(' '):
				active = False
			elif active:
				fields = lmap(str.strip, line.split()[:4])
				queue_dict = {'name': fields[0]}
				for key, value in ifilter(lambda k_v: not k_v[1].startswith('-'), izip(keys, fields[1:])):
					queue_dict[key] = parser[key](value)
				yield queue_dict
		proc.status_raise(timeout=0)


class PBSCheckJobs(CheckJobsWithProcess):
	def __init__(self, config, fqid_fun=identity):
		proc_factory = ProcessCreatorAppendArguments(config, 'qstat', ['-f'],
			lambda wms_id_list: lmap(fqid_fun, wms_id_list))
		CheckJobsWithProcess.__init__(self, config, proc_factory, status_map={
			Job.DONE: ['C', 'E', 'T', 'fail', 'success'],
			Job.QUEUED: ['Q'],
			Job.RUNNING: ['R'],
			Job.SUBMITTED: ['H', 'S'],
			Job.WAITING: ['W'],
		})

	def _parse(self, proc):
		for section in accumulate(proc.stdout.iter(self._timeout), '', lambda x, buf: x == '\n'):
			try:
				lines = section.replace('\n\t', '').split('\n')
				job_info = DictFormat(' = ').parse(lines[1:])
				job_info[CheckInfo.WMSID] = lines[0].split(':')[1].split('.')[0].strip()
				job_info[CheckInfo.RAW_STATUS] = job_info.pop('job_state')
				job_info[CheckInfo.QUEUE] = job_info.pop('queue', None)
				if 'exec_host' in job_info:
					exec_host = job_info.pop('exec_host').split('/')[0]
					job_info[CheckInfo.WN] = exec_host + '.' + job_info.get('server', '')
			except Exception:
				raise BackendError('Error reading job info:\n%s' % section)
			yield job_info


class PBS(PBSGECommon):  # pylint:disable=too-many-ancestors
	config_section_list = PBSGECommon.config_section_list + ['PBS']

	def __init__(self, config, name):
		cancel_executor = CancelJobsWithProcessBlind(config, 'qdel',
			fmt=lambda wms_id_list: lmap(self._fqid, wms_id_list), unknown_id='Unknown Job Id')
		PBSGECommon.__init__(self, config, name,
			cancel_executor=cancel_executor,
			check_executor=CheckJobsMissingState(config, PBSCheckJobs(config, self._fqid)),
			nodes_finder=PBSDiscoverNodes(config), queues_finder=PBSDiscoverQueues(config))
		self._server = config.get('server', '', on_change=None)

	def parse_submit_output(self, data):
		# 1667161.ekpplusctl.ekpplus.cluster
		return data.split('.')[0].strip()

	def _fqid(self, wms_id):
		if not self._server:
			return wms_id
		return '%s.%s' % (wms_id, self._server)

	def _get_submit_arguments(self, jobnum, job_name, reqs, sandbox, stdout, stderr):
		req_map = {WMS.MEMORY: ('pvmem', lambda m: '%dmb' % m)}
		params = PBSGECommon._get_common_submit_arguments(self, jobnum, job_name,
			reqs, sandbox, stdout, stderr, req_map)
		# Job requirements
		if reqs.get(WMS.QUEUES):
			params += ' -q %s' % reqs[WMS.QUEUES][0]
		if reqs.get(WMS.SITES):
			params += ' -l host=%s' % str.join('+', reqs[WMS.SITES])
		return params
