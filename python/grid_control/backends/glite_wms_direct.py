import os, sys
from grid_control import utils
from glite_wms import GliteWMS

glite = os.environ.get('GLITE_WMS_LOCATION', os.environ.get('GLITE_LOCATION', ''))
sys.path.append(os.path.join(glite, 'lib'))
sys.path.append(os.path.join(glite, 'lib', 'python'))

from glite_wmsui_LbWrapper import Status
import Job

class GliteWMSDirect(GliteWMS):
	def __init__(self, config, module, monitor):
		GliteWMS.__init__(self, config, module, monitor)
		self.wrStatus = Status()
		self.jobStatus = Job.JobStatus(self.wrStatus)


	# Check status of jobs and yield (jobNum, wmsID, status, other data)
	def checkJobs(self, ids):
		if len(ids) == 0:
			raise StopIteration

		errors = []
		activity = utils.ActivityLog("checking job status")
		for (wmsId, jobNum) in ids:
			self.wrStatus.getStatus(wmsId, 0)
			err, apiMsg = self.wrStatus.get_error()
			if err:
				errors.append(apiMsg)
			else:
				info = self.wrStatus.loadStatus()
				tmp = zip(map(str.lower, self.jobStatus.states_names), info[0:self.jobStatus.ATTR_MAX])
				data = utils.filterDict(dict(tmp), vF = lambda v: (v != '') and (v != '0'))
				data['id'] = data['jobid']
				yield (jobNum, data['id'], self._statusMap[data['status'].lower()], data)
		del activity
		if errors:
			utils.eprint("The following glite errors have occured:\n%s" % str.join('\n', errors))
