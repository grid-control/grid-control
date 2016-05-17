# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

import os, time
from grid_control.job_db import Job
from grid_control.monitoring import Monitoring
from grid_control.utils import filterDict, getVersion, mergeDicts, pathShare
from grid_control.utils.thread_tools import GCThreadPool
from grid_control_cms.DashboardAPI.DashboardAPI import DashboardAPI

class DashBoard(Monitoring):
	configSections = Monitoring.configSections + ['dashboard']

	def __init__(self, config, name, task):
		Monitoring.__init__(self, config, name, task)
		jobDesc = task.getDescription(None) # TODO: use the other variables for monitoring
		self._app = config.get('application', 'shellscript', onChange = None)
		self._runningMax = config.getInt('dashboard timeout', 5, onChange = None)
		self._tasktype = config.get('task', jobDesc.jobType or 'analysis', onChange = None)
		self._taskname = config.get('task name', '@GC_TASK_ID@_@DATASETNICK@', onChange = None)
		self._statusMap = {Job.DONE: 'DONE', Job.FAILED: 'DONE', Job.SUCCESS: 'DONE',
			Job.RUNNING: 'RUNNING', Job.ABORTED: 'ABORTED', Job.CANCELLED: 'CANCELLED'}
		self._tp = GCThreadPool()


	def getScript(self):
		yield pathShare('mon.dashboard.sh', pkg = 'grid_control_cms')


	def getTaskConfig(self):
		return { 'TASK_NAME': self._taskname, 'DB_EXEC': self._app, 'DATASETNICK': '' }


	def getFiles(self):
		for fn in ('DashboardAPI.py', 'Logger.py', 'apmon.py', 'report.py'):
			yield pathShare('..', 'DashboardAPI', fn, pkg = 'grid_control_cms')


	def _publish(self, jobObj, jobNum, taskId, usermsg):
		(_, backend, rawId) = jobObj.wmsId.split('.', 2)
		dashId = '%s_%s' % (jobNum, rawId)
		if 'http' not in jobObj.wmsId:
			dashId = '%s_https://%s:/%s' % (jobNum, backend, rawId)
		msg = mergeDicts([{'taskId': taskId, 'jobId': dashId, 'sid': rawId}] + usermsg)
		DashboardAPI(taskId, dashId).publish(**filterDict(msg, vF = lambda v: v is not None))


	def _start_publish(self, jobObj, jobNum, desc, message):
		taskId = self._task.substVars('dashboard task id', self._taskname, jobNum,
			addDict = {'DATASETNICK': ''}).strip('_')
		self._tp.start_thread('Notifying dashboard about %s of job %d' % (desc, jobNum),
			self._publish, jobObj, jobNum, taskId, message)


	# Called on job submission
	def onJobSubmit(self, wms, jobObj, jobNum):
		token = wms.getAccessToken(jobObj.wmsId)
		self._start_publish(jobObj, jobNum, 'submission', [{
			'user': os.environ['LOGNAME'], 'GridName': '/CN=%s' % token.getUsername(), 'CMSUser': token.getUsername(),
			'tool': 'grid-control', 'JSToolVersion': getVersion(),
			'SubmissionType':'direct', 'tool_ui': os.environ.get('HOSTNAME', ''),
			'application': self._app, 'exe': 'shellscript', 'taskType': self._tasktype,
			'scheduler': wms.wmsName, 'vo': token.getGroup()}, self._task.getSubmitInfo(jobNum)])


	# Called on job status update and output
	def _updateDashboard(self, wms, jobObj, jobNum, data, addMsg):
		# Translate status into dashboard status message
		statusDashboard = self._statusMap.get(jobObj.state, 'PENDING')
		self._start_publish(jobObj, jobNum, 'status', [{'StatusValue': statusDashboard,
			'StatusValueReason': data.get('reason', statusDashboard).upper(),
			'StatusEnterTime': data.get('timestamp', time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime())),
			'StatusDestination': data.get('dest', '') }, addMsg])


	def onJobUpdate(self, wms, jobObj, jobNum, data):
		self._updateDashboard(wms, jobObj, jobNum, jobObj, {})


	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		self._updateDashboard(wms, jobObj, jobNum, jobObj, {'ExeExitCode': retCode})


	def onFinish(self):
		self._tp.wait_and_drop(self._runningMax)
