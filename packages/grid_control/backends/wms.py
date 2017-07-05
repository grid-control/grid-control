# | Copyright 2007-2017 Karlsruhe Institute of Technology
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

# Generic base class for workload management systems

from grid_control.gc_plugin import NamedPlugin
from grid_control.utils import Result
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import make_enum
from hpfwk import AbstractError, NestedException
from python_compat import sorted


class BackendError(NestedException):
	pass


BackendJobState = make_enum([  # pylint:disable=invalid-name
	'ABORTED',  # job was aborted by the WMS
	'CANCELLED',  # job was cancelled
	'DONE',  # job is finished
	'QUEUED',  # job is at WMS and is assigned a place to run
	'RUNNING',  # job is running
	'UNKNOWN',  # job status is unknown
	'WAITING',  # job is at WMS but was not yet assigned some place to run
])


class WMS(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['wms', 'backend']
	config_tag_name = 'wms'
	alias_list = ['NullWMS']

	def __init__(self, config, name):
		name = (name or self.__class__.__name__).upper().replace('.', '_')
		NamedPlugin.__init__(self, config, name)
		self._wait_idle = config.get_int('wait idle', 60, on_change=None)
		self._wait_work = config.get_int('wait work', 10, on_change=None)
		self._job_parser = config.get_plugin('job parser', 'JobInfoProcessor',
			cls='JobInfoProcessor', on_change=None)

	def can_submit(self, needed_time, can_currently_submit):
		# TODO: This should take a job requirement list
		raise AbstractError

	def cancel_jobs(self, gc_id_list):
		# Cancel jobs and return list of successfully cancelled gc_id_list
		raise AbstractError

	def check_jobs(self, gc_id_list):
		# Check status and return (gc_id, job_state, job_info) for active jobs
		raise AbstractError

	def deploy_task(self, task, transfer_se, transfer_sb):
		raise AbstractError

	def get_access_token(self, gc_id):
		raise AbstractError  # Return access token instance responsible for this gc_id

	def get_interval_info(self):  # Return (waitIdle, wait)
		return Result(wait_on_idle=self._wait_idle, wait_between_steps=self._wait_work)

	def retrieve_jobs(self, gc_id_jobnum_list):
		raise AbstractError  # Return (jobnum, exit_code, data, outputdir) for retrived jobs

	def submit_jobs(self, jobnum_list, task):  # jobnum_list = [1, 2, ...]
		raise AbstractError  # Return (jobnum, gc_id, data) for successfully submitted jobs

	def _create_gc_id(self, wms_id):
		if not wms_id:
			return None
		return 'WMSID.%s.%s' % (self._name, wms_id)

	def _discover(self, discover_module, item_name):
		def _discovery_fun():
			if not _discovery_fun.discovery_cache:
				_discovery_fun.discovery_cache = {}
				for entry in discover_module.discover():
					_discovery_fun.discovery_cache[entry.pop('name')] = entry
				msg = 'an unknown number of'
				if _discovery_fun.discovery_cache:
					msg = str(len(_discovery_fun.discovery_cache))
				self._log.info('Backend discovered %s %s', msg, item_name)
			return _discovery_fun.discovery_cache
		return _discovery_fun

	def _get_map_wms_id2gc_id(self, gc_id_list):
		result = {}
		for gc_id in gc_id_list:
			wms_id = self._split_gc_id(gc_id)[1]
			if wms_id in result:
				raise BackendError('Multiple gc_id_list map to the same wms_id!')
			result[wms_id] = gc_id
		return result

	def _iter_wms_ids(self, gc_id_jobnum_list):
		for (gc_id, _) in gc_id_jobnum_list:
			yield self._split_gc_id(gc_id)[1]

	def _run_executor(self, desc, executor, fmt_result, gc_id_list, *args):
		# Perform some action with the executor, translate wms_id -> gc_id and format the result
		activity = Activity('%s: %s' % (self._name, desc))
		map_wms_id2gc_id = self._get_map_wms_id2gc_id(gc_id_list)
		wms_id_list = sorted(map_wms_id2gc_id.keys())

		for result in executor.execute(self._log, wms_id_list, *args):
			wms_id = result[0]  # result[0] is the wms_id by convention
			gc_id = map_wms_id2gc_id.pop(wms_id, None)
			if gc_id is not None:
				yield fmt_result((gc_id,) + result[1:])
			else:
				self._log.debug('unable to find gc_id for wms_id %r', wms_id)
		activity.finish()

	def _split_gc_id(self, gc_id):
		return tuple(gc_id.split('.', 2)[1:])

make_enum(['WALLTIME', 'CPUTIME', 'MEMORY', 'CPUS', 'BACKEND', 'DISKSPACE',
	'SITES', 'QUEUES', 'WN', 'ENDPOINT', 'SOFTWARE', 'STORAGE', 'ARRAY_SIZE'], WMS)
