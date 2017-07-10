# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import WMS
from grid_control.utils import Result
from python_compat import ifilter, lmap, sorted


class MultiWMS(WMS):
	alias_list = ['']

	# Distribute to WMS according to job id prefix
	def __init__(self, config, name, backend_list):
		WMS.__init__(self, config, name)
		self._default_backend = backend_list[0]
		default_timing = self._default_backend.get_interval_info()
		self._timing = Result(wait_on_idle=default_timing.wait_on_idle,
			wait_between_steps=default_timing.wait_between_steps)
		self._map_backend_name2backend = {
			self._default_backend.get_object_name().lower(): self._default_backend
		}
		for backend_entry in backend_list[1:]:
			backend = backend_entry
			self._map_backend_name2backend[backend.get_object_name().lower()] = backend
			wms_timing = backend.get_interval_info()
			self._timing.wait_on_idle = max(self._timing.wait_on_idle,
				wms_timing.wait_on_idle)
			self._timing.wait_between_steps = max(self._timing.wait_between_steps,
				wms_timing.wait_between_steps)

		self._broker_wms = config.get_plugin('wms broker', 'RandomBroker',
			cls=Broker, bind_kwargs={'inherit': True, 'tags': [self]},
			pargs=('wms', 'wms', self._map_backend_name2backend.keys))

	def can_submit(self, needed_time, can_currently_submit):
		can_currently_submit = self._default_backend.can_submit(needed_time, can_currently_submit)
		for backend in self._map_backend_name2backend.values():
			can_currently_submit = backend.can_submit(needed_time, can_currently_submit)
		return can_currently_submit

	def cancel_jobs(self, gc_id_list):
		tmp = lmap(lambda gc_id: (gc_id, None), gc_id_list)
		return self._forward_call(tmp, self._find_backend,
			lambda backend, args: backend.cancel_jobs(lmap(lambda x: x[0], args)))

	def check_jobs(self, gc_id_list):
		tmp = lmap(lambda gc_id: (gc_id, None), gc_id_list)
		return self._forward_call(tmp, self._find_backend,
			lambda backend, args: backend.check_jobs(lmap(lambda x: x[0], args)))

	def deploy_task(self, task, transfer_se, transfer_sb):
		for backend in self._map_backend_name2backend.values():
			backend.deploy_task(task, transfer_se, transfer_sb)

	def get_access_token(self, gc_id):
		backend_name = self._split_gc_id(gc_id)[0].lower()
		backend = self._map_backend_name2backend.get(backend_name, self._default_backend)
		return backend.get_access_token(gc_id)

	def get_interval_info(self):
		return self._timing

	def retrieve_jobs(self, gc_id_jobnum_list):
		return self._forward_call(gc_id_jobnum_list, self._find_backend,
			lambda backend, args: backend.retrieve_jobs(args))

	def submit_jobs(self, jobnum_list, task):
		return self._forward_call(jobnum_list, lambda jobnum: self._choose_backend(jobnum, task),
			lambda backend, args: backend.submit_jobs(args, task))

	def _choose_backend(self, jobnum, task):
		job_req_list = self._broker_wms.broker(task.get_requirement_list(jobnum), WMS.BACKEND)
		return list(dict(job_req_list).get(WMS.BACKEND))[0]

	def _find_backend(self, gc_id_jobnum):
		return self._split_gc_id(gc_id_jobnum[0])[0]

	def _forward_call(self, args, assign_fun, call_fun):
		backend_name2args = self._get_map_backend_name2args(args, assign_fun)
		avail_backend_name_list = sorted(self._map_backend_name2backend)
		for backend_name in ifilter(backend_name2args.__contains__, avail_backend_name_list):
			wms = self._map_backend_name2backend[backend_name]
			for result in call_fun(wms, backend_name2args[backend_name]):
				yield result

	def _get_map_backend_name2args(self, args, assign_fun):
		backend_name2args = {}
		default_backend = self._default_backend.get_object_name()
		for arg in args:  # Assign args to backends
			backend = assign_fun(arg) or default_backend
			backend_name2args.setdefault(backend.lower(), []).append(arg)
		return backend_name2args
