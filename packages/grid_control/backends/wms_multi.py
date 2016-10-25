# | Copyright 2012-2016 Karlsruhe Institute of Technology
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


# Distribute to WMS according to job id prefix

class MultiWMS(WMS):
	def __init__(self, config, name, wmsList):
		WMS.__init__(self, config, name)
		self._defaultWMS = wmsList[0]
		defaultT = self._defaultWMS.get_interval_info()
		self._timing = Result(wait_on_idle = defaultT.wait_on_idle, wait_between_steps = defaultT.wait_between_steps)
		self._wmsMap = {self._defaultWMS.get_object_name().lower(): self._defaultWMS}
		for wmsEntry in wmsList[1:]:
			wms_obj = wmsEntry
			self._wmsMap[wms_obj.get_object_name().lower()] = wms_obj
			wmsT = wms_obj.get_interval_info()
			self._timing.wait_on_idle = max(self._timing.wait_on_idle, wmsT.wait_on_idle)
			self._timing.wait_between_steps = max(self._timing.wait_between_steps, wmsT.wait_between_steps)

		self._brokerWMS = config.get_plugin('wms broker', 'RandomBroker',
			cls = Broker, bkwargs={'inherit': True, 'tags': [self]}, pargs = ('wms', 'wms', self._wmsMap.keys))


	def get_interval_info(self):
		return self._timing


	def can_submit(self, needed_time, can_currently_submit):
		can_currently_submit = self._defaultWMS.can_submit(needed_time, can_currently_submit)
		for wms_obj in self._wmsMap.values():
			can_currently_submit = wms_obj.can_submit(needed_time, can_currently_submit)
		return can_currently_submit


	def get_access_token(self, gc_id):
		return self._wmsMap.get(self._split_gc_id(gc_id)[0].lower(), self._defaultWMS).get_access_token(gc_id)


	def deploy_task(self, task, monitor, transfer_se, transfer_sb):
		for wms_obj in self._wmsMap.values():
			wms_obj.deploy_task(task, monitor, transfer_se, transfer_sb)


	def submit_jobs(self, jobnumList, task):
		def chooseBackend(jobnum):
			jobReq = self._brokerWMS.brokerAdd(task.get_requirement_list(jobnum), WMS.BACKEND)
			return list(dict(jobReq).get(WMS.BACKEND))[0]
		return self._forwardCall(jobnumList, chooseBackend, lambda wms_obj, args: wms_obj.submit_jobs(args, task))


	def _findBackend(self, gc_id_jobnum):
		return self._split_gc_id(gc_id_jobnum[0])[0]


	def check_jobs(self, gc_id_list):
		tmp = lmap(lambda gc_id: (gc_id, None), gc_id_list)
		return self._forwardCall(tmp, self._findBackend, lambda wms_obj, args: wms_obj.check_jobs(lmap(lambda x: x[0], args)))


	def cancel_jobs(self, gc_id_list):
		tmp = lmap(lambda gc_id: (gc_id, None), gc_id_list)
		return self._forwardCall(tmp, self._findBackend, lambda wms_obj, args: wms_obj.cancel_jobs(lmap(lambda x: x[0], args)))


	def retrieve_jobs(self, gc_id_jobnum_List):
		return self._forwardCall(gc_id_jobnum_List, self._findBackend, lambda wms_obj, args: wms_obj.retrieve_jobs(args))


	def _getMapID2Backend(self, args, assignFun):
		argMap = {}
		default_backend = self._defaultWMS.get_object_name()
		for arg in args: # Assign args to backends
			backend = assignFun(arg) or default_backend
			argMap.setdefault(backend.lower(), []).append(arg)
		return argMap


	def _forwardCall(self, args, assignFun, callFun):
		argMap = self._getMapID2Backend(args, assignFun)
		for wmsPrefix in ifilter(argMap.__contains__, sorted(self._wmsMap)):
			wms = self._wmsMap[wmsPrefix]
			for result in callFun(wms, argMap[wmsPrefix]):
				yield result
