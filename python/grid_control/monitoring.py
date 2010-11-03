import os, itertools
from grid_control import AbstractObject, Job, utils

# Monitoring base class with submodule support
class Monitoring(AbstractObject):
	def __init__(self, config, module, submodules = ""):
		self.config = config
		self.module = module
		self.submodules = submodules

	def getEnv(self, wms):
		return utils.mergeDicts(map(lambda m: m.getEnv(wms), self.submodules))

	def getFiles(self):
		return itertools.chain(map(lambda m: m.getFiles(), self.submodules))

	def onJobSubmit(self, wms, jobObj, jobNum):
		for submodule in self.submodules:
			submodule.onJobSubmit(wms, jobObj, jobNum)

	def onJobUpdate(self, wms, jobObj, jobNum, data):
		for submodule in self.submodules:
			submodule.onJobUpdate(wms, jobObj, jobNum, data)

	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		for submodule in self.submodules:
			submodule.onJobOutput(wms, jobObj, jobNum, retCode)

	def onTaskFinish(self, nJobs):
		for submodule in self.submodules:
			submodule.onTaskFinish(nJobs)

Monitoring.dynamicLoaderPath()

class ScriptMonitoring(Monitoring):
	Monitoring.moduleMap["scripts"] = "ScriptMonitoring"
	def __init__(self, config, module):
		Monitoring.__init__(self, config, module)
		self.silent = config.getBool('events', 'silent', True, volatile=True)
		self.evtSubmit = config.get('events', 'on submit', '', volatile=True)
		self.evtStatus = config.get('events', 'on status', '', volatile=True)
		self.evtOutput = config.get('events', 'on output', '', volatile=True)
		self.evtFinish = config.get('events', 'on finish', '', volatile=True)

	# Get both task and job config / state dicts
	def scriptThread(self, script, jobNum = None, jobObj = None, allDict = {}):
		try:
			tmp = {}
			if jobNum != None:
				tmp.update(self.module.getSubmitInfo(jobNum))
			if jobObj != None:
				tmp.update(jobObj.getAll())
			tmp.update({'WORKDIR': self.config.workDir, 'CFGFILE': self.config.configFile})
			script = self.module.substVars(script, jobNum, tmp)

			tmp.update(self.module.getTaskConfig())
			tmp.update(self.module.getJobConfig(jobNum))
			if jobNum != None:
				tmp.update(self.module.getSubmitInfo(jobNum))

			tmp.update(allDict)
			for key, value in tmp.iteritems():
				os.environ["GC_%s" % key] = str(value)
			if self.silent:
				utils.LoggedProcess(script).wait()
			else:
				os.system(script)
		except GCError:
			sys.stderr.write(GCError.message)

	def runInBackground(self, script, jobNum = None, jobObj = None, addDict =  {}):
		if script != '':
			utils.gcStartThread(ScriptMonitoring.scriptThread, self, script, jobNum, jobObj)

	# Called on job submission
	def onJobSubmit(self, wms, jobObj, jobNum):
		self.runInBackground(self.evtSubmit, jobNum, jobObj)

	# Called on job status update
	def onJobUpdate(self, wms, jobObj, jobNum, data):
		self.runInBackground(self.evtStatus, jobNum, jobObj, {'STATUS': Job.states[jobObj.state]})

	# Called on job status update
	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		self.runInBackground(self.evtOutput, jobNum, jobObj, {'RETCODE': retCode})

	# Called at the end of the task
	def onTaskFinish(self, nJobs):
		self.runInBackground(self.evtFinish, addDict = {'NJOBS': nJobs})
