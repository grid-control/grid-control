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

import logging
from grid_control.backends.backend_tools import BackendError, BackendExecutor
from grid_control.job_db import Job
from grid_control.utils import abort
from grid_control.utils.data_structures import make_enum
from hpfwk import AbstractError
from python_compat import set


CheckInfo = make_enum(['WMSID', 'RAW_STATUS', 'QUEUE', 'WN', 'SITE'])  # pylint:disable=invalid-name
CheckStatus = make_enum(['OK', 'ERROR'])  # pylint:disable=invalid-name

# TODO: Error Handler Plugins - logging, exception, errorcode - with abort / continue


def expand_status_map(map_job_status2status_str_list):
	result = {}
	for job_status, status_str_list in map_job_status2status_str_list.items():
		result.update(dict.fromkeys(status_str_list, job_status))
	return result


class CheckJobs(BackendExecutor):
	def execute(self, wms_id_list):  # yields list of (wms_id, job_status, job_info)
		raise AbstractError

	def get_status(self):
		return CheckStatus.OK


class CheckJobsMissingState(CheckJobs):
	def __init__(self, config, executor, missing_state=Job.DONE):
		CheckJobs.__init__(self, config)
		(self._executor, self._missing_state) = (executor, missing_state)

	def execute(self, wms_id_list):  # yields list of (wms_id, job_status, job_info)
		checked_ids = set()
		for (wms_id, job_status, job_info) in self._executor.execute(wms_id_list):
			checked_ids.add(wms_id)
			yield (wms_id, job_status, job_info)
		if self._executor.get_status() == CheckStatus.OK:
			for wms_id in wms_id_list:
				if wms_id not in checked_ids:
					yield (wms_id, self._missing_state, {})

	def setup(self, log):
		CheckJobs.setup(self, log)
		self._executor.setup(log)


class CheckJobsWithProcess(CheckJobs):
	def __init__(self, config, proc_factory, status_map=None):
		CheckJobs.__init__(self, config)
		self._timeout = config.get_time('check timeout', 60, on_change=None)
		self._log_everything = config.get_bool('check promiscuous', False, on_change=None)
		self._errormsg = 'Job status command returned with exit code %(proc_status)s'
		self._status_map = expand_status_map(status_map or {})
		(self._status, self._proc_factory) = (CheckStatus.OK, proc_factory)

	def execute(self, wms_id_list):  # yields list of (wms_id, job_status, job_info)
		self._status = CheckStatus.OK
		proc = self._proc_factory.create_proc(wms_id_list)
		for job_info in self._parse(proc):
			if job_info and not abort():
				yield self._parse_job_info(job_info)
		if proc.status(timeout=0, terminate=True) != 0:
			self._handle_error(proc)
		if self._log_everything:
			self._log.log_process(proc, level=logging.DEBUG, msg='Finished checking jobs')

	def get_status(self):
		return self._status

	def _handle_error(self, proc):
		self._filter_proc_log(proc, self._errormsg)

	def _parse(self, proc):  # return job_info(s)
		raise AbstractError

	def _parse_job_info(self, job_info):  # return (wms_id, job_status, job_info)
		try:
			job_status = self._parse_status(job_info.get(CheckInfo.RAW_STATUS), Job.UNKNOWN)
			return (job_info.pop(CheckInfo.WMSID), job_status, job_info)
		except Exception:
			raise BackendError('Unable to parse job info %s' % repr(job_info))

	def _parse_status(self, value, default):
		return self._status_map.get(value, default)
