# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

import time
from grid_control.backends.backend_tools import BackendExecutor, ProcessCreatorAppendArguments
from grid_control.utils import abort
from grid_control.utils.activity import Activity
from hpfwk import AbstractError
from python_compat import identity, lmap


class CancelJobs(BackendExecutor):
	def execute(self, wms_id_list, wms_name):  # yields list of (wms_id,)
		raise AbstractError


class CancelAndPurgeJobs(CancelJobs):
	def __init__(self, config, cancel_executor, purge_executor):
		CancelJobs.__init__(self, config)
		(self._cancel_executor, self._purge_executor) = (cancel_executor, purge_executor)

	def execute(self, wms_id_list, wms_name):  # yields list of (wms_id,)
		marked_wms_id_list = lmap(lambda result: result[0],
			self._cancel_executor.execute(wms_id_list, wms_name))
		time.sleep(5)
		activity = Activity('Purging jobs')
		for result in self._purge_executor.execute(marked_wms_id_list, wms_name):
			yield result
		activity.finish()

	def setup(self, log):
		CancelJobs.setup(self, log)
		self._cancel_executor.setup(log)
		self._purge_executor.setup(log)


class CancelJobsWithProcess(CancelJobs):
	def __init__(self, config, proc_factory):
		CancelJobs.__init__(self, config)
		self._timeout = config.get_time('cancel timeout', 60, on_change=None)
		self._errormsg = 'Job cancel command returned with exit code %(proc_status)s'
		self._proc_factory = proc_factory

	def execute(self, wms_id_list, wms_name):
		proc = self._proc_factory.create_proc(wms_id_list)
		for result in self._parse(wms_id_list, proc):
			if not abort():
				yield result
		if proc.status(timeout=0, terminate=True) != 0:
			self._handle_error(proc)

	def _handle_error(self, proc):
		self._filter_proc_log(proc, self._errormsg)

	def _parse(self, wms_id_list, proc):  # yield list of (wms_id,)
		raise AbstractError


class CancelJobsWithProcessBlind(CancelJobsWithProcess):
	def __init__(self, config, cmd, args=None, fmt=identity, unknown_id=None):
		proc_factory = ProcessCreatorAppendArguments(config, cmd, args, fmt)
		CancelJobsWithProcess.__init__(self, config, proc_factory)
		self._blacklist = None
		if unknown_id is not None:
			self._blacklist = [unknown_id]

	def _handle_error(self, proc):
		self._filter_proc_log(proc, self._errormsg, blacklist=self._blacklist, log_empty=False)

	def _parse(self, wms_id_list, proc):  # yield list of (wms_id,)
		proc.status(self._timeout, terminate=True)
		return lmap(lambda wms_id: (wms_id,), wms_id_list)
