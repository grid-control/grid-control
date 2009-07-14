import os.path, copy
from grid_control import Module, AbstractError

# Parameterized Module
class ParaMod(Module):
	def __init__(self, config, opts, proxy):
		Module.__init__(self, config, opts, proxy)
		self.baseMod = Module.open(config.get('ParaMod', 'module'), config, opts, proxy)
		self.baseJobs = config.getInt('ParaMod', 'jobs', 1)
		self.paramSpace = None

		# adopt functions from basemod
		for fkt in [ 'getTaskConfig', 'onJobSubmit', 'onJobUpdate', 'onJobOutput',
			'getInFiles', 'getOutFiles', 'getSubstFiles', 'getCommand' ]:
			setattr(self, fkt, getattr(self.baseMod, fkt))
		self.getRequirements = lambda x: self.baseMod.getRequirements(x / self.getParamSpace())
		self.getJobArguments = lambda x: self.baseMod.getJobArguments(x / self.getParamSpace())

	def getJobConfig(self, jobNum):
		config = self.baseMod.getJobConfig(jobNum / self.getParamSpace())
		config.update(Module.getJobConfig(self, jobNum))
		config.update(self.getParams()[jobNum % self.getParamSpace()])
		config.update({'PARAM_ID': jobNum % self.getParamSpace()})
		return config

	def getVarMapping(self):
		mapping = self.baseMod.getVarMapping()
		mapping.update(Module.getVarMapping(self))
		for param in self.getParams():
			mapping.update(dict(zip(param.keys(), param.keys())))
		return mapping

	def getMaxJobs(self):
		maxJobs = None
		try:
			maxJobs = self.baseMod.getMaxJobs()
		except:
			pass
		if maxJobs == None:
			maxJobs = self.baseJobs
		return max(1, maxJobs) * self.getParamSpace()

	def getParamSpace(self):
		if self.paramSpace == None:
			self.paramSpace = len(self.getParams())
		return self.paramSpace

	def getParams(self):
		raise AbstractError


class SimpleParaMod(ParaMod):
	def __init__(self, config, opts, proxy):
		ParaMod.__init__(self, config, opts, proxy)
		self.paraValues = config.get('ParaMod', 'parameter values')
		self.paraName = config.get('ParaMod', 'parameter name', 'PARAMETER').strip()

	def getParams(self):
		# returns list of dictionaries
		return map(lambda x: {self.paraName: x}, map(str.strip, self.paraValues.split()))


class LinkedParaMod(SimpleParaMod):
	def __init__(self, config, opts, proxy):
		SimpleParaMod.__init__(self, config, opts, proxy)

	def getParams(self):
		result = []
		for value in filter(lambda x: x != '', map(str.strip, self.paraValues.split('\n'))):
			result += [dict(zip(map(str.strip, self.paraName.split(":")), map(str.strip, value.split(":"))))]
		return result
