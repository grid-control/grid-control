# | Copyright 2016 Karlsruhe Institute of Technology
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
from grid_control import utils
from grid_control.backends.backend_tools import BackendExecutor, ProcessCreatorAppendArguments
from grid_control.utils.activity import Activity
from hpfwk import AbstractError
from python_compat import identity, lmap

class CancelJobs(BackendExecutor):
	def execute(self, wmsIDs, wmsName): # yields list of (wmsID,)
		raise AbstractError


class CancelJobsWithProcess(CancelJobs):
	def __init__(self, config, proc_factory):
		CancelJobs.__init__(self, config)
		self._timeout = config.getTime('cancel timeout', 60, onChange = None)
		self._errormsg = 'Job cancel command returned with exit code %(proc_status)s'
		self._proc_factory = proc_factory

	def _parse(self, wmsIDs, proc): # yield list of (wmsID,)
		raise AbstractError

	def execute(self, wmsIDs, wmsName):
		proc = self._proc_factory.create_proc(wmsIDs)
		for result in self._parse(wmsIDs, proc):
			if not utils.abort():
				yield result
		if proc.status(timeout = 0, terminate = True) != 0:
			self._handleError(proc)

	def _handleError(self, proc):
		self._filter_proc_log(proc, self._errormsg)


class CancelJobsWithProcessBlind(CancelJobsWithProcess):
	def __init__(self, config, cmd, args = None, fmt = identity, unknownID = None):
		proc_factory = ProcessCreatorAppendArguments(config, cmd, args, fmt)
		CancelJobsWithProcess.__init__(self, config, proc_factory)
		self._blacklist = None
		if unknownID is not None:
			self._blacklist = [unknownID]

	def _parse(self, wmsIDs, proc): # yield list of (wmsID,)
		proc.status(self._timeout, terminate = True)
		return lmap(lambda wmsID: (wmsID,), wmsIDs)

	def _handleError(self, proc):
		self._filter_proc_log(proc, self._errormsg, blacklist = self._blacklist, log_empty = False)


class CancelAndPurgeJobs(CancelJobs):
	def __init__(self, config, cancel_executor, purge_executor):
		CancelJobs.__init__(self, config)
		(self._cancel_executor, self._purge_executor) = (cancel_executor, purge_executor)

	def setup(self, log):
		CancelJobs.setup(self, log)
		self._cancel_executor.setup(log)
		self._purge_executor.setup(log)

	def execute(self, wmsIDs, wmsName): # yields list of (wmsID,)
		marked_wmsIDs = lmap(lambda result: result[0], self._cancel_executor.execute(wmsIDs, wmsName))
		time.sleep(5)
		activity = Activity('Purging jobs')
		for result in self._purge_executor.execute(marked_wmsIDs, wmsName):
			yield result
		activity.finish()
