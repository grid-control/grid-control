import os.path
from grid_control import Module, utils
from DashboardAPI import DashboardAPI
from time import time, localtime, strftime

class UserMod(Module):
	def __init__(self, workDir, config, opts):
		Module.__init__(self, workDir, config, opts)
		self._executable = config.getPath('UserMod', 'executable')
		self._arguments = config.get('UserMod', 'arguments', '')


	def getInFiles(self):
		files = Module.getInFiles(self) + [ self._executable ]
		if self.dashboard:
			files.append(utils.atRoot('share', 'run.dash.sh')),
			for file in ('DashboardAPI.py', 'Logger.py', 'ProcInfo.py', 'apmon.py', 'report.py'):
				files.append(utils.atRoot('python/DashboardAPI', file))
		return files


	def getCommand(self):
		cmd = os.path.basename(self._executable)
		if self.dashboard:
			return 'chmod u+x %s; ./run.dash.sh "./%s $@"' % (cmd, cmd)
		else:
			return 'chmod u+x %s; ./%s "$@"' % (cmd, cmd)


	def getJobArguments(self, jobNum):
		return self._arguments


	# Called on job submission
	def onJobSubmit(self, job, id):
		Module.onJobSubmit(self, job, id)

		if self.dashboard:
			dashboard = DashboardAPI(self.taskID, "%s_%s" % (id, job.id))
			dashboard.publish(
				taskId=self.taskID, jobId="%s_%s" % (id, job.id), sid="%s_%s" % (id, job.id),
				application="shellscript", exe="shellscript",
				tool="grid-control", GridName=self.proxy.getUsername(),
				scheduler="gLite", taskType="analysis", vo=self.proxy.getVO(),
				user=os.environ['LOGNAME']
			)


	# Called on job status update
	def onJobUpdate(self, job, id, data):
		Module.onJobUpdate(self, job, id, data)

		if self.dashboard:
			dashboard = DashboardAPI(self.taskID, "%s_%s" % (id, job.id))
			dashboard.publish(
				taskId=self.taskID, jobId="%s_%s" % (id, job.id), sid="%s_%s" % (id, job.id),
				StatusValue=data.get('status', 'pending').upper(),
				StatusValueReason=data.get('reason', data.get('status', 'pending')).upper(),
				StatusEnterTime=data.get('timestamp', strftime("%Y-%m-%d_%H:%M:%S", localtime())),
				StatusDestination=data.get('dest', "")
			)
