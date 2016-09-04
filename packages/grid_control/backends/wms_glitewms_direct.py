# | Copyright 2010-2016 Karlsruhe Institute of Technology
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
from grid_control import utils
from grid_control.backends.aspect_status import CheckInfo, CheckJobs
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_glitewms import GliteWMS
from grid_control.backends.wms_grid import GridStatusMap
from grid_control.job_db import Job
from hpfwk import ExceptionCollector, clear_current_exception
from python_compat import imap, lmap, lzip

class GliteWMSDirect_CheckJobs(CheckJobs):
	def __init__(self, config, status_fun):
		CheckJobs.__init__(self, config)
		self._status_fun = status_fun
		self._status_map = GridStatusMap

	def execute(self, wmsIDs): # yields list of (wmsID, job_status, job_info)
		ec = ExceptionCollector()
		for wmsID in wmsIDs:
			try:
				job_info = utils.filterDict(dict(self._status_fun(wmsID)), vF = lambda v: v not in ['', '0'])
				job_info[CheckInfo.RAW_STATUS] = job_info.pop('status', '').lower()
				if 'destination' in job_info:
					try:
						dest_info = job_info['destination'].split('/', 1)
						job_info[CheckInfo.SITE] = dest_info[0].strip()
						job_info[CheckInfo.QUEUE] = dest_info[1].strip()
					except Exception:
						clear_current_exception()
				yield (wmsID, self._status_map.get(job_info[CheckInfo.RAW_STATUS], Job.UNKNOWN), job_info)
			except Exception:
				ec.collect()
				if utils.abort():
					break
		ec.raise_any(BackendError('Encountered errors while checking job status'))


class GliteWMSDirect(GliteWMS):
	def __init__(self, config, name):
		glite_path = os.environ.get('GLITE_WMS_LOCATION', os.environ.get('GLITE_LOCATION', ''))
		stored_sys_path = list(sys.path)
		for p in ['lib', 'lib64', os.path.join('lib', 'python'), os.path.join('lib64', 'python')]:
			sys.path.append(os.path.join(glite_path, p))

		try: # gLite 3.2
			import wmsui_api
			glStates = wmsui_api.states_names
			def getStatusDirect(wmsID):
				try: # new parameter json
					jobStatus = wmsui_api.getStatus(wmsui_api.getJobIdfromList(None, [wmsID])[0], 0)
				except Exception:
					jobStatus = wmsui_api.getStatus(wmsui_api.getJobIdfromList([wmsID])[0], 0)
				return lmap(lambda name: (name.lower(), jobStatus.getAttribute(glStates.index(name))), glStates)
		except Exception: # gLite 3.1
			try:
				from glite_wmsui_LbWrapper import Status
				import Job
				wrStatus = Status()
				jobStatus = Job.JobStatus(wrStatus)
				def getStatusDirect(wmsID):
					wrStatus.getStatus(wmsID, 0)
					err, apiMsg = wrStatus.get_error()
					if err:
						raise BackendError(apiMsg)
					info = wrStatus.loadStatus()
					return lzip(imap(str.lower, jobStatus.states_names), info[0:jobStatus.ATTR_MAX])
			except Exception:
				getStatusDirect = None
		sys.path = stored_sys_path

		checkExecutor = None
		if getStatusDirect:
			checkExecutor = GliteWMSDirect_CheckJobs(config, getStatusDirect)
		GliteWMS.__init__(self, config, name, checkExecutor = checkExecutor)
