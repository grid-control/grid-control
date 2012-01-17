import os
from grid_control import AbstractObject, Job, utils

class EventHandler(AbstractObject):
	def __init__(self, config, module, submodules = []):
		(self.config, self.module, self.submodules) = (config, module, submodules)

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

EventHandler.dynamicLoaderPath()


# Monitoring base class with submodule support
class Monitoring(EventHandler):
	# Script to call later on
	def getScript(self):
		return utils.listMapReduce(lambda m: list(m.getScript()), self.submodules)

	def getTaskConfig(self):
		tmp = {'GC_MONITORING': str.join(" ", map(os.path.basename, self.getScript()))}
		return utils.mergeDicts(map(lambda m: m.getTaskConfig(), self.submodules) + [tmp])

	def getFiles(self):
		return utils.listMapReduce(lambda m: list(m.getFiles()), self.submodules, self.getScript())

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
			tmp.update(self.module.getTaskConfig())
			tmp.update(self.module.getJobConfig(jobNum))
			if jobNum != None:
				tmp.update(self.module.getSubmitInfo(jobNum))
			tmp.update(allDict)
			for key, value in tmp.iteritems():
				os.environ["GC_%s" % key] = str(value)

			script = self.module.substVars(script, jobNum, tmp)
			if self.silent:
				utils.LoggedProcess(script).wait()
			else:
				os.system(script)
		except GCError:
			utils.eprint(GCError.message)

	def runInBackground(self, script, jobNum = None, jobObj = None, addDict =  {}):
		if script != '':
			utils.gcStartThread("Running monitoring script %s" % script,
				ScriptMonitoring.scriptThread, self, script, jobNum, jobObj)

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
