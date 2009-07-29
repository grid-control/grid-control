import os.path, copy, csv
from grid_control import Module, AbstractError, WMS, utils

# Parameterized Module
class ParaMod(Module):
	def __init__(self, config, opts, proxy):
		Module.__init__(self, config, opts, proxy)
		self.baseMod = Module.open(config.get('ParaMod', 'module'), config, opts, proxy)
		self.baseJobs = config.getInt('ParaMod', 'jobs', 1)
		self.paramSpace = None
		self.baseMod.hookenv = lambda tmp, jobNum: tmp.update(self.getJobConfig(jobNum))

		# adopt functions from basemod
		for fkt in [ 'getTaskConfig', 'onJobSubmit', 'onJobUpdate', 'onJobOutput',
			'getInFiles', 'getOutFiles', 'getSubstFiles', 'getCommand' ]:
			setattr(self, fkt, getattr(self.baseMod, fkt))
		self.getJobArguments = lambda x: self.baseMod.getJobArguments(x / self.getParamSpace())

	def getRequirements(self, jobNum):
		reqs = self.baseMod.getRequirements(jobNum / self.getParamSpace())
		params = self.getParams()[jobNum % self.getParamSpace()]
		for key, value in params.items():
			if key == 'WALLTIME':
				reqs.append((WMS.WALLTIME, utils.parseTime(value)))
			elif key == 'CPUTIME':
				reqs.append((WMS.CPUTIME, utils.parseTime(value)))
			elif key == 'MEMORY':
				reqs.append((WMS.MEMORY, int(value)))
		return reqs

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

	def report(self, jobNum):
		return self.getParams()[jobNum % self.getParamSpace()]

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


class FileParaMod(ParaMod):
	def __init__(self, config, opts, proxy):
		ParaMod.__init__(self, config, opts, proxy)
		self.path = config.getPath('ParaMod', 'parameter source')
		sniffed = csv.Sniffer().sniff(open(self.path).read(1024))
		csv.register_dialect('sniffed', sniffed)
		self.dialect = config.get('ParaMod', 'parameter source dialect', 'sniffed')

	def getParams(self):
		return list(csv.DictReader(open(self.path), dialect = self.dialect))


class LinkedParaMod(SimpleParaMod):
	def __init__(self, config, opts, proxy):
		SimpleParaMod.__init__(self, config, opts, proxy)

	def getParams(self):
		result = []
		for value in filter(lambda x: x != '', map(str.strip, self.paraValues.split('\n'))):
			result += [dict(zip(map(str.strip, self.paraName.split(":")), map(str.strip, value.split(":"))))]
		return result

class UberParaMod(ParaMod):
	def __init__(self, config, opts, proxy):
		ParaMod.__init__(self, config, opts, proxy)

		names = map(str.strip,
			    config.get('ParaMod', 'parameters').split())
		
		self.pars = {}
		for p in names:
			lines = config.get('ParaMod',
					   '%s values' % (p,)).split()
			self.pars[p] = map(str.strip, lines)

	def getParams(self):
		res = [[]]

		for p in self.pars.keys():
			m = len(res)
			n = len(self.pars[p])

			tmp = []
			if ':' in p:
				ps = p.split(':')
				tmp = map(lambda e: [zip(ps, e.split(':'))],
					  self.pars[p])
			else:
				tmp = [[[p, e]] for e in self.pars[p]]

			tmp_ = []
			for e in tmp:
				tmp_ += [e] * m
				
			res = map(lambda e: e[0] + e[1], zip(tmp_, res * n))

		return map(dict, res)
