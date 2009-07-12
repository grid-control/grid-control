import os.path
from grid_control import Module, AbstractError

# Parameterized Module
class ParaMod(Module):
	def __init__(self, config, opts, proxy):
		Module.__init__(self, config, opts, proxy)
		self.baseMod = Module.open(config.get('ParaMod', 'module'), config, opts)
		self.paramSpace = None

	def onJobSubmit(self, jobObj, jobNum, dbmessage = [{}]):
		return self.baseMod.onJobSubmit(jobObj, jobNum, dbmessage)

	def onJobUpdate(self, jobObj, jobNum, data, dbmessage = [{}]):
		return self.baseMod.onJobUpdate(jobObj, jobNum, data, dbmessage)

	def onJobOutput(self, jobObj, jobNum, retCode):
		return self.baseMod.onJobOutput(jobObj, jobNum, retCode)

	def getTaskConfig(self):
		self.baseMod.proxy = self.proxy
		return self.baseMod.getTaskConfig()

	def getRequirements(self, jobNum):
		return self.baseMod.getRequirements(jobNum / self.getParamSpace())

	def getInFiles(self):
		return self.baseMod.getInFiles()

	def getOutFiles(self):
		return self.baseMod.getOutFiles()

	def getSubstFiles(self):
		return self.baseMod.getSubstFiles()

	def getCommand(self):
		return self.baseMod.getCommand()

	def getJobArguments(self, jobNum):
		return self.baseMod.getJobArguments(jobNum / self.getParamSpace())

	def getJobConfig(self, jobNum):
		config = self.baseMod.getJobConfig(jobNum / self.getParamSpace())
		config.update(self.getParams()[jobNum % self.getParamSpace()])
		return config

	def getParamSpace(self):
		if self.paramSpace == None:
			self.paramSpace = len(self.getParams())
		return self.paramSpace

	def getMaxJobs(self):
		try:
			maxJobs = self.paramSpace * self.baseMod.getMaxJobs()
		except:
			maxJobs = 1
		return max(1, maxJobs) * self.getParamSpace()

	def getParams(self):
		raise AbstractError


class SimpleParaMod(ParaMod):
	def __init__(self, config, opts, proxy):
		ParaMod.__init__(self, config, opts, proxy)
		self.paraValues = config.get('ParaMod', 'parameter values').split()
		self.paraName = config.get('ParaMod', 'parameter name', 'PARAMETER')

	def getParams(self):
		# returns list of dictionaries
		return map(lambda x: {self.paraName: x}, self.paraValues)
