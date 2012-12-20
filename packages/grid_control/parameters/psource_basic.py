import random, re
from python_compat import *
from grid_control import ConfigError, utils, WMS, APIError
from psource_base import ParameterSource, ParameterMetadata, ParameterInfo

class InternalParameterSource(ParameterSource):
	def __init__(self, values, keys):
		ParameterSource.__init__(self)
		(self.values, self.keys) = (values, keys)

	def getMaxJobs(self):
		return len(self.values)

	def fillParameterInfo(self, pNum, result):
		result.update(self.values[pNum])

	def fillParameterKeys(self, result):
		result.extend(self.keys)


class RequirementParameterSource(ParameterSource):
	def fillParameterKeys(self, result):
		for key in ['WALLTIME', 'CPUTIME', 'MEMORY']:
			if key in result:
				result.remove(key)

	def fillParameterInfo(self, pNum, result):
		if 'WALLTIME' in result:
			result[ParameterInfo.REQS].append((WMS.WALLTIME, utils.parseTime(result.pop('WALLTIME'))))
		if 'CPUTIME' in result:
			result[ParameterInfo.REQS].append((WMS.CPUTIME, utils.parseTime(result.pop('CPUTIME'))))
		if 'MEMORY' in result:
			result[ParameterInfo.REQS].append((WMS.MEMORY, int(result.pop('MEMORY'))))


class SingleParameterSource(ParameterSource):
	def __init__(self, key):
		ParameterSource.__init__(self)
		self.key = key.lstrip('!')
		self.meta = ParameterMetadata(self.key, untracked = '!' in key)

	def fillParameterKeys(self, result):
		result.append(self.meta)


class KeyParameterSource(SingleParameterSource):
	pass
ParameterSource.managerMap['key'] = KeyParameterSource


class SimpleParameterSource(SingleParameterSource):
	def __init__(self, key, values):
		SingleParameterSource.__init__(self, key)
		if values == None:
			raise ConfigError('Missing values for %s' % key)
		self.values = values

	def show(self, level = 0):
		ParameterSource.show(self, level, 'var = %s, len = %d' % (self.key, len(self.values)))

	def getMaxJobs(self):
		return len(self.values)

	def fillParameterInfo(self, pNum, result):
		result[self.key] = self.values[pNum]

	def create(cls, pconfig, key):
		return SimpleParameterSource(key, pconfig.getParameter(key.lstrip('!')))
	create = classmethod(create)
ParameterSource.managerMap['var'] = SimpleParameterSource


class ConstParameterSource(SingleParameterSource):
	def __init__(self, key, value):
		SingleParameterSource.__init__(self, key)
		self.value = value

	def show(self, level = 0):
		ParameterSource.show(self, level, 'const = %s, value = %s' % (self.key, self.value))

	def fillParameterInfo(self, pNum, result):
		result[self.key] = self.value

	def create(cls, pconfig, key, value = None):
		if value == None:
			value = pconfig.get(key)
		return ConstParameterSource(key, value)
	create = classmethod(create)
ParameterSource.managerMap['const'] = ConstParameterSource


class RNGParameterSource(SingleParameterSource):
	def __init__(self, key = 'JOB_RANDOM', low = 1e6, high = 1e7-1):
		SingleParameterSource.__init__(self, '!%s' % key)
		(self.low, self.high) = (low, high)

	def show(self, level = 0):
		ParameterSource.show(self, level, 'var = %s, range = (%s, %s)' % (self.key, self.low, self.high))

	def fillParameterInfo(self, pNum, result):
		result[self.key] = random.randint(self.low, self.high)
ParameterSource.managerMap['rng'] = RNGParameterSource


class CounterParameterSource(SingleParameterSource):
	def __init__(self, key, seed):
		SingleParameterSource.__init__(self, '!%s' % key)
		self.seed = seed

	def show(self, level = 0):
		ParameterSource.show(self, level, 'var = %s, start = %s' % (self.key, self.seed))

	def fillParameterInfo(self, pNum, result):
		result[self.key] = self.seed + result['MY_JOBID']
ParameterSource.managerMap['counter'] = CounterParameterSource


class FormatterParameterSource(SingleParameterSource):
	def __init__(self, key, fmt, source, default = ''):
		SingleParameterSource.__init__(self, '!%s' % key)
		(self.fmt, self.source, self.default) = (fmt, source, default)

	def show(self, level = 0):
		ParameterSource.show(self, level, 'var = %s, fmt = %s, source = %s, default = %s' %
			(self.key, self.fmt, self.source, self.default))

	def fillParameterInfo(self, pNum, result):
		result[self.key] = self.fmt % utils.parseType(str(result.get(self.source, self.default)))
ParameterSource.managerMap['format'] = FormatterParameterSource


class CollectParameterSource(SingleParameterSource): # Merge parameter values
	def __init__(self, target, *sources):
		SingleParameterSource.__init__(self, target)
		self.sources = map(lambda regex: re.compile('^%s$' % regex.replace('...', '.*')), list(sources))

	def fillParameterInfo(self, pNum, result):
		for src in self.sources:
			for key in result:
				if src.search(str(key)):
					result[self.key] = result[key]
					return
ParameterSource.managerMap['collect'] = CollectParameterSource
