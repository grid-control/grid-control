import os
from grid_control import NamedObject, Job, utils

class EventHandler(NamedObject):
	def __init__(self, config, name, task, submodules = []):
		NamedObject.__init__(self, config, name)
		(self.config, self.task, self.submodules) = (config, task, submodules)

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

EventHandler.registerObject()


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

Monitoring.registerObject()

class ScriptMonitoring(Monitoring):
	Monitoring.moduleMap["scripts"] = "ScriptMonitoring"
	def __init__(self, config, name, task):
		Monitoring.__init__(self, config, name, task)
		self.silent = config.getBool('events', 'silent', True, onChange = None)
		self.evtSubmit = config.get('events', 'on submit', '', onChange = None)
		self.evtStatus = config.get('events', 'on status', '', onChange = None)
		self.evtOutput = config.get('events', 'on output', '', onChange = None)
		self.evtFinish = config.get('events', 'on finish', '', onChange = None)

	# Get both task and job config / state dicts
	def scriptThread(self, script, jobNum = None, jobObj = None, allDict = {}):
		try:
			tmp = {}
			if jobNum != None:
				tmp.update(self.task.getSubmitInfo(jobNum))
			if jobObj != None:
				tmp.update(jobObj.getAll())
			tmp.update({'WORKDIR': self.config.getWorkPath(), 'CFGFILE': self.config.configFile})
			tmp.update(self.task.getTaskConfig())
			tmp.update(self.task.getJobConfig(jobNum))
			if jobNum != None:
				tmp.update(self.task.getSubmitInfo(jobNum))
			tmp.update(allDict)
			for key, value in tmp.iteritems():
				os.environ["GC_%s" % key] = str(value)

			script = self.task.substVars(script, jobNum, tmp)
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
