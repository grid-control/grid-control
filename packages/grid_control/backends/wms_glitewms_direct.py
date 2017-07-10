# | Copyright 2010-2017 Karlsruhe Institute of Technology
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

import os, sys
from grid_control.backends.aspect_status import CheckInfo, CheckJobs, expand_status_map
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_glitewms import GliteWMS
from grid_control.job_db import Job
from grid_control.utils import abort
from grid_control.utils.algos import filter_dict
from hpfwk import ExceptionCollector, clear_current_exception
from python_compat import imap, lmap, lzip


class GliteWMSDirectCheckJobs(CheckJobs):
	def __init__(self, config, status_fun):
		CheckJobs.__init__(self, config)
		self._status_fun = status_fun
		self._status_map = expand_status_map(GliteWMS.grid_status_map)

	def execute(self, wms_id_list):  # yields list of (wms_id, job_status, job_info)
		exc = ExceptionCollector()
		for wms_id in wms_id_list:
			try:
				job_info = filter_dict(dict(self._status_fun(wms_id)),
					value_filter=lambda v: v not in ['', '0'])
				job_info[CheckInfo.RAW_STATUS] = job_info.pop('status', '').lower()
				if 'destination' in job_info:
					try:
						dest_info = job_info['destination'].split('/', 1)
						job_info[CheckInfo.SITE] = dest_info[0].strip()
						job_info[CheckInfo.QUEUE] = dest_info[1].strip()
					except Exception:
						clear_current_exception()
				yield (wms_id, self._status_map.get(job_info[CheckInfo.RAW_STATUS], Job.UNKNOWN), job_info)
			except Exception:
				exc.collect()
				if abort():
					break
		exc.raise_any(BackendError('Encountered errors while checking job status'))


class GliteWMSDirect(GliteWMS):  # pylint:disable=too-many-ancestors
	def __init__(self, config, name):
		glite_path = os.environ.get('GLITE_WMS_LOCATION', os.environ.get('GLITE_LOCATION', ''))
		stored_sys_path = list(sys.path)
		for dn in ['lib', 'lib64', os.path.join('lib', 'python'), os.path.join('lib64', 'python')]:
			sys.path.append(os.path.join(glite_path, dn))

		try:  # gLite 3.2
			import wmsui_api
			glite_state_name_list = wmsui_api.states_names

			def _get_status_direct(wms_id):
				try:  # new parameter json
					job_status = wmsui_api.getStatus(wmsui_api.getJobIdfromList(None, [wms_id])[0], 0)
				except Exception:
					clear_current_exception()
					job_status = wmsui_api.getStatus(wmsui_api.getJobIdfromList([wms_id])[0], 0)
				return lmap(lambda name: (name.lower(),
					job_status.getAttribute(glite_state_name_list.index(name))), glite_state_name_list)
		except Exception:  # gLite 3.1
			clear_current_exception()
			try:
				from glite_wmsui_LbWrapper import Status
				wrapper_status = Status()
				job_status = __import__('Job').JobStatus(wrapper_status)

				def _get_status_direct(wms_id):
					wrapper_status.getStatus(wms_id, 0)
					err, api_msg = wrapper_status.get_error()
					if err:
						raise BackendError(api_msg)
					info = wrapper_status.loadStatus()
					return lzip(imap(str.lower, job_status.states_names), info[0:job_status.ATTR_MAX])
			except Exception:
				clear_current_exception()
				_get_status_direct = None
		sys.path = stored_sys_path

		check_executor = None
		if _get_status_direct:
			check_executor = GliteWMSDirectCheckJobs(config, _get_status_direct)
		GliteWMS.__init__(self, config, name, check_executor=check_executor)
