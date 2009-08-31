import os.path, copy, csv
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
		# [{VAR1:VALUE1, VAR2:VALUE2}, {VAR1:VALUE1}, {VAR3:VALUE3}]
		# Results in 3 parameter sets with VAR1=VARLUE1,VAR2=VALUE2 in 1st job
		raise AbstractError


class SimpleParaMod(ParaMod):
	def __init__(self, config):
		ParaMod.__init__(self, config)
		self.paraValues = config.get('ParaMod', 'parameter values')
		self.paraName = config.get('ParaMod', 'parameter name', 'PARAMETER').strip()

	def getParams(self):
		# returns list of dictionaries
		return map(lambda x: {self.paraName: x}, map(str.strip, self.paraValues.split()))


class FileParaMod(ParaMod):
	def __init__(self, config):
		ParaMod.__init__(self, config)
		self.path = config.getPath('ParaMod', 'parameter source')
		sniffed = csv.Sniffer().sniff(open(self.path).read(1024))
		csv.register_dialect('sniffed', sniffed)
		self.dialect = config.get('ParaMod', 'parameter source dialect', 'sniffed')

	def getParams(self):
		def cleanupDict(d):
			# strip all key value entries
			tmp = tuple(map(lambda item: map(str.strip, item), d.items()))
			# filter empty parameters
			return filter(lambda (k, v): k != '', tmp)
		tmp = list(csv.DictReader(open(self.path), dialect = self.dialect))
		return map(lambda d: dict(cleanupDict(d)), tmp)


class LinkedParaMod(SimpleParaMod):
	def __init__(self, config):
		SimpleParaMod.__init__(self, config)

	def getParams(self):
		result = []
		for value in filter(lambda x: x != '', map(str.strip, self.paraValues.split('\n'))):
			result += [dict(zip(map(str.strip, self.paraName.split(":")), map(str.strip, value.split(":"))))]
		return result


class UberParaMod(ParaMod):
	"""This module builds all possible combinations of parameters
	and/or tuples of parameters.  For example,

		parameters  = spam (ham, eggs)
		spam        = A B
		(ham, eggs) = (1, 2) (3, 4)

	gives the following parameter combinations of (spam, ham, egg):

		(A, 1, 2), (B, 1, 2), (A, 3, 4), (B, 3, 4)
	"""
	def __init__(self, config):
		ParaMod.__init__(self, config)

		lowercase_tuple = lambda t: tuple(map(str.lower, t))
		option_to_tuple = lambda o: utils.parseTuples(o)[0]

		names = utils.parseTuples(config.get('ParaMod', 'parameters'))
		options = dict(map(lambda (o, v): (repr(option_to_tuple(o)),
						   utils.parseTuples(v)),
				   config.parser.items('ParaMod')))

		self.pars = {}
		for p in names:
			if isinstance(p, tuple):
				p_key = repr(lowercase_tuple(p))
				if p_key in options:
					self.pars[repr(p)] = options[p_key]
				else:
					self.pars[repr(p)] = []
			else:
				self.pars[repr(p)] = map(str.strip,
					config.get('ParaMod', p).split())

	def getParams(self):
		res = [[]]

		for p in self.pars.keys():
			m = len(res)
			n = len(self.pars[p])

			tmp = []
			p_ = eval(p)
			if isinstance(p_, tuple):
				tmp = map(lambda t: zip(p_, t),
					  self.pars[p])
			else:
				tmp = [[[p_, v]] for v in self.pars[p]]

			tmp_ = []
			for e in tmp:
				tmp_ += [e] * m
				
			res = map(lambda (x, y): x + y, zip(tmp_, res * n))

		return map(dict, res)
