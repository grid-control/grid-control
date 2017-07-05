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

import xml.dom.minidom
from grid_control.backends.aspect_cancel import CancelJobsWithProcessBlind
from grid_control.backends.aspect_status import CheckInfo, CheckJobsMissingState, CheckJobsWithProcess, CheckStatus  # pylint:disable=line-too-long
from grid_control.backends.backend_tools import BackendDiscoveryProcess, ProcessCreatorViaArguments
from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import BackendError, WMS
from grid_control.config import ConfigError
from grid_control.job_db import Job
from grid_control.utils import get_local_username, resolve_install_path
from grid_control.utils.parsing import parse_time
from grid_control.utils.process_base import LocalProcess
from grid_control_usb.wms_pbsge import PBSGESubmit
from python_compat import any, identity, imap, izip, lmap, set, sorted


class GridEngineDiscoverNodes(BackendDiscoveryProcess):
	def __init__(self, config):
		BackendDiscoveryProcess.__init__(self, config, 'qconf')

	def discover(self):
		nodes = set()
		proc = LocalProcess(self._exec, '-shgrpl')
		for group in proc.stdout.iter(timeout=self._timeout):
			yield {'name': group.strip()}
			proc_g = LocalProcess(self._exec, '-shgrp_resolved', group)
			for host_list in proc_g.stdout.iter(timeout=self._timeout):
				nodes.update(host_list.split())
			proc_g.status_raise(timeout=0)
		for host in sorted(nodes):
			yield {'name': host.strip()}
		proc.status_raise(timeout=0)


class GridEngineDiscoverQueues(BackendDiscoveryProcess):
	def __init__(self, config):
		BackendDiscoveryProcess.__init__(self, config, 'qconf')

	def discover(self):
		attr_list = ['h_vmem', 'h_cpu', 's_rt']
		attr_req_map = dict(izip(attr_list, [WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME]))
		attr_parser = dict(izip(attr_list, [int, parse_time, parse_time]))

		proc = LocalProcess(self._exec, '-sql')
		for queue in imap(str.strip, proc.stdout.iter(timeout=self._timeout)):
			proc_q = LocalProcess(self._exec, '-sq', queue)
			queue_dict = {'name': queue}
			for line in proc_q.stdout.iter(timeout=self._timeout):
				attr, value = lmap(str.strip, line.split(' ', 1))
				if (attr in attr_list) and (value != 'INFINITY'):
					queue_dict[attr_req_map[attr]] = attr_parser[attr](value)
			proc_q.status_raise(timeout=0)
			yield queue_dict
		proc.status_raise(timeout=0)


class GridEngineCheckJobs(CheckJobsWithProcess):
	def __init__(self, config, user=None):
		CheckJobsWithProcess.__init__(self, config, GridEngineCheckJobsProcessCreator(config))
		self._job_status_key = lmap(str.lower, config.get_list('job status key',
			['JB_jobnum', 'JB_jobnumber', 'JB_job_number'], on_change=None))

	def _parse(self, proc):
		proc.status(timeout=self._timeout)
		status_string_raw = proc.stdout.read(timeout=0)
		status_string = _fix_unknown_jobs_xml(status_string_raw)
		if not status_string:
			self._status = CheckStatus.ERROR
		else:
			for result in self._parse_status_string(status_string):
				yield result

	def _parse_status(self, value, default):
		if any(imap(value.__contains__, ['E', 'e'])):
			return Job.UNKNOWN
		if any(imap(value.__contains__, ['h', 's', 'S', 'T', 'w'])):
			return Job.QUEUED
		if any(imap(value.__contains__, ['r', 't'])):
			return Job.RUNNING
		return Job.READY

	def _parse_status_string(self, status_string):
		try:
			dom = xml.dom.minidom.parseString(status_string)
		except Exception:
			raise BackendError("Couldn't parse qstat XML output!")
		for job_node in dom.getElementsByTagName('job_list'):
			job_info = {}
			try:
				for node in job_node.childNodes:
					if node.nodeType != xml.dom.minidom.Node.ELEMENT_NODE:
						continue
					if node.hasChildNodes():
						job_info[str(node.nodeName)] = str(node.childNodes[0].nodeValue)
				for job_info_key in job_info:
					if str(job_info_key).lower() in self._job_status_key:
						job_info[CheckInfo.WMSID] = job_info.pop(job_info_key)
				job_info[CheckInfo.RAW_STATUS] = job_info.pop('state')
				if 'queue_name' in job_info:
					queue, node = job_info['queue_name'].split('@')
					job_info[CheckInfo.QUEUE] = queue
					job_info[CheckInfo.WN] = node
			except Exception:
				raise BackendError('Error reading job info:\n%s' % job_node.toxml())
			yield job_info


class GridEngine(LocalWMS):  # pylint:disable=too-many-ancestors
	alias_list = ['SGE', 'UGE', 'OGE']
	config_section_list = LocalWMS.config_section_list + ['GridEngine'] + alias_list

	def __init__(self, config, name):
		cancel_executor = CancelJobsWithProcessBlind(config, 'qdel',
			fmt=lambda wms_id_list: [str.join(',', wms_id_list)], unknown_id='Unknown Job Id')
		queue_broker = config.get_composited_plugin('queue broker', pargs=('queue',), cls=Broker,
			bind_kwargs={'inherit': True, 'tags': [self]}, pkwargs={'req_type': WMS.QUEUES,
				'discovery_fun': self._discover(GridEngineDiscoverQueues(config), 'queues')},
			default='FilterBroker RandomBroker LimitBroker', default_compositor='MultiBroker')
		nodes_broker = config.get_composited_plugin('nodes broker', pargs=('nodes',), cls=Broker,
			bind_kwargs={'inherit': True, 'tags': [self]}, pkwargs={'req_type': WMS.WN,
				'discovery_fun': self._discover(GridEngineDiscoverNodes(config), 'nodes')},
			default='FilterBroker RandomBroker', default_compositor='MultiBroker')
		LocalWMS.__init__(self, config, name,
			broker_list=[queue_broker, nodes_broker],
			local_submit_executor=GridEngineSubmit(config, 'qsub'),
			cancel_executor=cancel_executor,
			check_executor=CheckJobsMissingState(config, GridEngineCheckJobs(config)))


class GridEngineSubmit(PBSGESubmit):
	def __init__(self, config, submit_exec):
		def _time_str(secs):
			return '%02d:%02d:%02d' % (secs / 3600, (secs / 60) % 60, secs % 60)

		PBSGESubmit.__init__(self, config, submit_exec, {
			WMS.MEMORY: ('-l', lambda memory: 'h_vmem=%dM' % memory, identity),
			WMS.WALLTIME: ('-l', lambda walltime: 's_rt=%s' % _time_str(walltime), identity),
			WMS.CPUTIME: ('-l', lambda cputime: 'h_cpu=%s' % _time_str(cputime), identity)
		})
		self._project = config.get('project name', '', on_change=None)

	def get_array_key_list(self):
		return ['SGE_TASK_ID']

	def _get_submit_arguments(self, job_desc, exec_fn, req_list, stdout_fn, stderr_fn):
		arg_list = ['-r', 'n', '-notify']  # Restart jobs = no, send signal before killing
		req_dict = dict(req_list)
		if self._project:
			arg_list.extend(['-P', self._project])
		# Job requirements
		(queue, nodes) = ((req_dict.get(WMS.QUEUES) or [''])[0], req_dict.get(WMS.WN))
		if not nodes and queue:
			arg_list.extend(['-q', queue])
		elif nodes and queue:
			arg_list.extend(['-q', str.join(',', imap(lambda node: '%s@%s' % (queue, node), nodes))])
		elif nodes:
			raise ConfigError('Please also specify queue when selecting nodes!')
		return arg_list + PBSGESubmit._get_submit_arguments(self, job_desc,
			exec_fn, req_list, stdout_fn, stderr_fn)

	def _parse_submit_output(self, wms_id_str):
		# Your job 424992 ("test.sh") has been submitted
		return wms_id_str.split()[2].strip()


class GridEngineCheckJobsProcessCreator(ProcessCreatorViaArguments):
	def __init__(self, config):
		ProcessCreatorViaArguments.__init__(self, config)
		self._cmd = resolve_install_path('qstat')
		self._user = config.get('user', get_local_username(), on_change=None)

	def _arguments(self, wms_id_list):
		if not self._user:
			return [self._cmd, '-xml']
		return [self._cmd, '-xml', '-u', self._user]


def _fix_unknown_jobs_xml(status_string):
	(uk_start_tag, uk_end_tag) = ('<unknown_jobs', '</unknown_jobs>')  # start: <unknown_jobs xmlns...
	(new_start_tag, new_end_tag) = ('<unknown_job>', '</unknown_job>')
	# qstat gives invalid xml in <unknown_jobs> node
	uk_start = status_string.find(uk_start_tag)
	uk_jobs_string = ''
	if uk_start >= 0:
		uk_end = status_string.find(uk_end_tag) + len(uk_end_tag)
		uk_jobs_string = status_string[uk_start:uk_end]  # select xml in "unknown_jobs" node
		uk_jobs_string_fixed = uk_jobs_string.replace('<>', new_start_tag).replace('</>', new_end_tag)
		return status_string.replace(uk_jobs_string, uk_jobs_string_fixed)
	return status_string
