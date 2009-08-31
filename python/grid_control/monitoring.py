import os, threading
from grid_control import AbstractObject, utils, Job

class Monitoring(AbstractObject):
	# Read configuration options and init vars
	def __init__(self, config, module):
		self.config = config
		self.module = module

		self.evtSubmit = config.getPath('events', 'on submit', '', volatile=True)
		self.evtStatus = config.getPath('events', 'on status', '', volatile=True)
		self.evtOutput = config.getPath('events', 'on output', '', volatile=True)


	def getEnv(self, wms):
		return {}

	def getFiles(self):
		return []

	# Get both task and job config / state dicts
	def setEventEnviron(self, jobObj, jobNum):
		tmp = {}
		tmp.update(self.module.getTaskConfig())
		tmp.update(self.module.getJobConfig(jobNum))
		tmp.update(self.module.getSubmitInfo(jobNum))
		tmp.update(jobObj.getAll())
		tmp.update({'WORKDIR': self.config.workDir})
		for key, value in tmp.iteritems():
			os.environ["GC_%s" % key] = str(value)

	# Called on job submission
	def onJobSubmit(self, wms, jobObj, jobNum):
		if self.evtSubmit != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s" % (self.evtSubmit, jobNum, jobObj.wmsId)
			threading.Thread(target = os.system, args = (params,)).start()

	# Called on job status update
	def onJobUpdate(self, wms, jobObj, jobNum, data):
		if self.evtStatus != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s %s" % (self.evtStatus, jobNum, jobObj.wmsId, Job.states[jobObj.state])
			threading.Thread(target = os.system, args = (params,)).start()

	# Called on job status update
	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		if self.evtOutput != '':
			self.setEventEnviron(jobObj, jobNum)
			params = "%s %d %s %d" % (self.evtOutput, jobNum, jobObj.wmsId, retCode)
			threading.Thread(target = os.system, args = (params,)).start()
