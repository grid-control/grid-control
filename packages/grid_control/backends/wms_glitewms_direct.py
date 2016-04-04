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

glite = os.environ.get('GLITE_WMS_LOCATION', os.environ.get('GLITE_LOCATION', ''))
for p in ['lib', 'lib64', os.path.join('lib', 'python'), os.path.join('lib64', 'python')]:
	sys.path.append(os.path.join(glite, p))

from grid_control import utils
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_glitewms import GliteWMS
from python_compat import imap, lmap, lzip

try: # gLite 3.2
	import wmsui_api
	glStates = wmsui_api.states_names
	def getStatusDirect(wmsId):
		jobStatusDirect = wmsui_api.getStatusDirect(wmsui_api.getJobIdfromList([wmsId])[0], 0)
		return lmap(lambda name: (name.lower(), jobStatusDirect.getAttribute(glStates.index(name))), glStates)
except Exception: # gLite 3.1
	try:
		from glite_wmsui_LbWrapper import Status
		import Job
		wrStatus = Status()
		jobStatus = Job.JobStatus(wrStatus)
		def getStatusDirect(wmsId):
			wrStatus.getStatusDirect(wmsId, 0)
			err, apiMsg = wrStatus.get_error()
			if err:
				raise BackendError(apiMsg)
			info = wrStatus.loadStatus()
			return lzip(imap(str.lower, jobStatus.states_names), info[0:jobStatus.ATTR_MAX])
	except Exception:
		getStatusDirect = None

class GliteWMSDirect(GliteWMS):
	# Check status of jobs and yield (jobNum, wmsID, status, other data)
	def checkJobs(self, ids):
		if getStatusDirect:
			return self.checkJobsDirect(ids)
		return GliteWMS.checkJobs(self, ids)


	def checkJobsDirect(self, ids):
		if len(ids) == 0:
			raise StopIteration

		activity = utils.ActivityLog('checking job status')
		errors = []
		for (wmsId, jobNum) in ids:
			try:
				data = utils.filterDict(dict(getStatusDirect(self._splitId(wmsId)[0])), vF = lambda v: (v != '') and (v != '0'))
				data['id'] = self._createId(data.get('jobid', wmsId))
				data['dest'] = data.get('destination', 'N/A')
				yield (jobNum, data['id'], self._statusMap[data['status'].lower()], data)
			except Exception:
				errors.append(repr(sys.exc_info()[1]))
				if utils.abort():
					break
		del activity
		if errors:
			utils.eprint('The following glite errors have occured:\n%s' % str.join('\n', errors))
