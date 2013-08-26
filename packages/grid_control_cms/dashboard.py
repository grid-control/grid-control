import os
from grid_control import QM, utils, Monitoring
from time import localtime, strftime
from DashboardAPI import DashboardAPI

class DashBoard(Monitoring):
	def __init__(self, config, module):
		Monitoring.__init__(self, config, module)
		(taskName, jobName, jobType) = module.getDescription(None) # TODO: use the other variables for monitoring
		self.app = config.get('dashboard', 'application', 'shellscript', mutable=True)
		jobType = QM(jobType, jobType, 'analysis')
		self.tasktype = config.get('dashboard', 'task', jobType, mutable=True)
		self.taskname = config.get('dashboard', 'task name', '@TASK_ID@_@DATASETNICK@', mutable=True, noVar=False)


	def getScript(self):
		yield utils.pathShare('mon.dashboard.sh', pkg = 'grid_control_cms')


	def getTaskConfig(self):
		return { 'TASK_NAME': self.taskname, 'DB_EXEC': self.app, 'DATASETNICK': '' }


	def getFiles(self):
		for fn in ('DashboardAPI.py', 'Logger.py', 'ProcInfo.py', 'apmon.py', 'report.py'):
			yield utils.pathShare('..', 'DashboardAPI', fn, pkg = 'grid_control_cms')


	def publish(self, jobObj, jobNum, taskId, usermsg):
		(header, backend, rawId) = jobObj.wmsId.split('.', 2)
		dashId = '%s_%s' % (jobNum, rawId)
		if "http" not in jobObj.wmsId:
			dashId = '%s_https://%s:/%s' % (jobNum, backend, rawId)
		msg = utils.mergeDicts([{'taskId': taskId, 'jobId': dashId, 'sid': rawId}] + usermsg)
		DashboardAPI(taskId, dashId).publish(**utils.filterDict(msg, vF = lambda v: v != None))


	# Called on job submission
	def onJobSubmit(self, wms, jobObj, jobNum):
		taskId = self.module.substVars(self.taskname, jobNum, addDict = {'DATASETNICK': ''}).strip('_')
		proxy = wms.getProxy(jobObj.wmsId)
		utils.gcStartThread("Notifying dashboard about job submission %d" % jobNum,
			self.publish, jobObj, jobNum, taskId, [{
			'user': os.environ['LOGNAME'], 'GridName': proxy.getUsername(), 'CMSUser': proxy.getUsername(),
			'tool': 'grid-control', 'JSToolVersion': utils.getVersion(),
			'SubmissionType':'direct', 'tool_ui': os.environ.get('HOSTNAME',''),
			'application': self.app, 'exe': 'shellscript', 'taskType': self.tasktype,
			'scheduler': wms.wmsName, 'vo': proxy.getGroup()}, self.module.getSubmitInfo(jobNum)])


	# Called on job status update
	def onJobUpdate(self, wms, jobObj, jobNum, data):
		taskId = self.module.substVars(self.taskname, jobNum, addDict = {'DATASETNICK': ''}).strip('_')
		utils.gcStartThread("Notifying dashboard about status of job %d" % jobNum,
			self.publish, jobObj, jobNum, taskId, [{
			'StatusValue': data.get('status', 'pending').upper(),
			'StatusValueReason': data.get('reason', data.get('status', 'pending')).upper(),
			'StatusEnterTime': data.get('timestamp', strftime('%Y-%m-%d_%H:%M:%S', localtime())),
			'StatusDestination': data.get('dest', '') }])

	def onTaskFinish(self, nJobs):
		utils.wait(5)
