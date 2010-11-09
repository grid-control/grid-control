import os, sys
glite = os.environ.get('GLITE_WMS_LOCATION', os.environ.get('GLITE_LOCATION', ''))
for p in ['lib', 'lib64', os.path.join('lib', 'python'), os.path.join('lib64', 'python')]:
	sys.path.append(os.path.join(glite, p))

try: # gLite 3.2
	import wmsui_api
	glStates = wmsui_api.states_names
	def getStatus(wmsId):
		jobStatus = wmsui_api.getStatus(wmsui_api.getJobIdfromList([wmsId])[0], 0)
		return map(lambda name: (name.lower(), jobStatus.getAttribute(glStates.index(name))), glStates)
except: # gLite 3.1
	from glite_wmsui_LbWrapper import Status
	import Job
	wrStatus = Status()
	jobStatus = Job.JobStatus(wrStatus)
	def getStatus(wmsId):
		wrStatus.getStatus(wmsId, 0)
		err, apiMsg = wrStatus.get_error()
		if err:
			raise GridError(apiMsg)
		info = wrStatus.loadStatus()
		return zip(map(str.lower, jobStatus.states_names), info[0:jobStatus.ATTR_MAX])

from grid_control import utils, GridError
from glite_wms import GliteWMS

class GliteWMSDirect(GliteWMS):
	def __init__(self, config, module, monitor):
		GliteWMS.__init__(self, config, module, monitor)


	# Check status of jobs and yield (jobNum, wmsID, status, other data)
	def checkJobs(self, ids):
		if len(ids) == 0:
			raise StopIteration

		activity = utils.ActivityLog("checking job status")
		errors = []
		for (wmsId, jobNum) in ids:
			try:
				data = utils.filterDict(dict(getStatus(wmsId)), vF = lambda v: (v != '') and (v != '0'))
				data['id'] = data.get('jobid', wmsId)
				data['dest'] = data.get('destination', 'N/A')
				yield (jobNum, data['id'], self._statusMap[data['status'].lower()], data)
			except:
				errors.append(repr(sys.exc_info()[1]))
				if utils.abort():
					break
		del activity
		if errors:
			utils.eprint("The following glite errors have occured:\n%s" % str.join('\n', errors))
