from grid_control import Module, AbstractError, WMS, utils

# Parameterized Module
class ParaMod(Module):
	def __init__(self, config):
		Module.__init__(self, config)
		self.baseMod = Module.open(config.get('ParaMod', 'module'), config)
		self.baseJobs = config.getInt('ParaMod', 'jobs', 1, volatile=True)
		self.paramSpace = None

		# adopt functions from basemod
		for fkt in [ 'getInFiles', 'getOutFiles', 'getSubstFiles',
			'getTaskConfig', 'getCommand', 'getDependencies' ]:
			setattr(self, fkt, getattr(self.baseMod, fkt))
		self.getJobArguments = lambda x: self.baseMod.getJobArguments(x / self.getParamSpace())

	def getRequirements(self, jobNum):
		reqs = self.baseMod.getRequirements(jobNum / self.getParamSpace())
		params = self.getParamsExt()[jobNum % self.getParamSpace()]
		def replaceTag(tag, newValue):
			reqs = filter(lambda (k,v): k != tag, reqs)
			reqs.append((tag, newValue))
		for key, value in params.items():
			if key == 'WALLTIME':
				replaceTag(WMS.WALLTIME, utils.parseTime(value))
			elif key == 'CPUTIME':
				replaceTag(WMS.CPUTIME, utils.parseTime(value))
			elif key == 'MEMORY':
				replaceTag(WMS.MEMORY, int(value))
		return reqs

	def getJobConfig(self, jobNum):
		config = self.baseMod.getJobConfig(jobNum / self.getParamSpace())
		config.update(Module.getJobConfig(self, jobNum))
		config.update(self.getParamsExt()[jobNum % self.getParamSpace()])
		config.update({'PARAM_ID': jobNum % self.getParamSpace()})
		return config

	def getVarMapping(self):
		mapping = self.baseMod.getVarMapping()
		mapping.update(Module.getVarMapping(self))
		for param in self.getParamsExt():
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

	def report(self, jobNum):
		return self.getParamsExt()[jobNum % self.getParamSpace()]

	def getParamSpace(self):
		if self.paramSpace == None:
			self.paramSpace = len(self.getParamsExt())
		return self.paramSpace

	def getParamsExt(self):
		params = []
		for pset in self.getParams():
			if 'JOBS' in pset:
				jobs = int(pset.pop('JOBS'))
				params.extend([pset] * jobs)
			else:
				params.append(pset)
		return params

	def getParams(self):
		# [{VAR1:VALUE1, VAR2:VALUE2}, {VAR1:VALUE1}, {VAR3:VALUE3}]
		# Results in 3 parameter sets with VAR1=VARLUE1,VAR2=VALUE2 in 1st job
		raise AbstractError
