import random
from python_compat import *
from grid_control import ConfigError, utils
from plugin_base import ParameterPlugin

class InternalPlugin(ParameterPlugin):
	def __init__(self, values, names):
		ParameterPlugin.__init__(self)
		(self.values, self.names) = (values, names)

	def getMaxJobs(self):
		return len(self.values)

	def getParameterNames(self):
		return self.names

	def getParameters(self, pNum, result):
		result.transient.update(self.values[pNum].transient)
		result.store.update(self.values[pNum].store)
		result.reqs.extend(self.values[pNum].reqs)


class SimpleParaPlugin(ParameterPlugin):
	def __init__(self, key, values):
		ParameterPlugin.__init__(self)
		if values == None:
			raise ConfigError('Missing values for %s' % key)
		(self.key, self.values) = (key, values)

	def getMaxJobs(self):
		return len(self.values)

	def getParameterNames(self):
		return ([self.key], [])

	def getParameters(self, pNum, result):
		result.store[self.key] = self.values[pNum]
ParameterPlugin.varManagerMap['var'] = lambda varMap, key: SimpleParaPlugin(key, varMap.get(key.lower()))


class ConstParaPlugin(ParameterPlugin):
	def __init__(self, key, value):
		ParameterPlugin.__init__(self)
		(self.key, self.value) = (key, value)

	def getParameters(self, pNum, result):
		result.store[self.key] = self.value

def createConstParaPlugin(varMap, key, value = None):
	if value == None:
		value = varMap.get(key.lower())
	return ConstParaPlugin(key, value)
ParameterPlugin.varManagerMap['const'] = createConstParaPlugin


class LookupParaPlugin(ParameterPlugin):
	def __init__(self, key, values, lookup):
		ParameterPlugin.__init__(self)
		if values == None:
			raise ConfigError('Missing values for %s' % key)
		if not isinstance(values, dict) and len(values) == 1:
			values = {None: values[0]}
		(self.key, self.values, self.lookup) = (key, values, lookup)

	def getParameterDeps(self):
		return [self.lookup]

	def getParameterNames(self):
		return ([self.key], [])

	def getParameters(self, pNum, result):
		src = result.store
		if self.lookup in result.transient:
			src = result.transient
		lookup = src.get(self.lookup, None)
		value = self.values.get(lookup, self.values.get(None))
		if value != None:
			result.store[self.key] = value
ParameterPlugin.varManagerMap['lookup'] = lambda varMap, key, lookup: LookupParaPlugin(key, varMap.get(key.lower()), lookup)


class RNGParaPlugin(ParameterPlugin):
	def __init__(self, key = 'JOB_RANDOM', low = 1e6, high = 1e7-1):
		ParameterPlugin.__init__(self)
		(self.key, self.low, self.high) = (key, low, high)

	def getParameters(self, pNum, result):
		result.transient[self.key] = random.randint(self.low, self.high)
ParameterPlugin.rawManagerMap['rng'] = RNGParaPlugin


class CounterParaPlugin(ParameterPlugin):
	def __init__(self, key, seed):
		ParameterPlugin.__init__(self)
		(self.key, self.seed) = (key, seed)

	def getParameterNames(self):
		return ([], [self.key])

	def getParameters(self, pNum, result):
		result.transient[self.key] = self.seed + result.transient['MY_JOBID']
ParameterPlugin.rawManagerMap['counter'] = CounterParaPlugin


class FilterParaPlugin(ParameterPlugin):
	def __init__(self, plugin):
		self.plugin = plugin
		ParameterPlugin.__init__(self)

	def getMaxJobs(self):
		return self.plugin.getMaxJobs()

	def getIntervention(self):
		return self.plugin.getIntervention()

	def getParameterDeps(self):
		return self.plugin.getParameterDeps()

	def getParameterNames(self):
		return self.plugin.getParameterNames()

	def getParameters(self, pNum, result):
		return self.plugin.getParameters(pNum, result)


class RequirementParaPlugin(FilterParaPlugin):
	def getParameterNames(self):
		fl = lambda lst: filter(lambda e: e not in ['WALLTIME', 'CPUTIME', 'MEMORY'], lst)
		tmp = self.plugin.getParameterNames()
		return (fl(tmp[0]), fl(tmp[1]))

	def getParameters(self, pNum, result):
		from grid_control import WMS
		def dict2req(d):
			if 'WALLTIME' in d:
				result.reqs.append(WMS.WALLTIME, utils.parseTime(d.pop('WALLTIME')))
			if 'CPUTIME' in d:
				result.reqs.append(WMS.CPUTIME, utils.parseTime(d.pop('CPUTIME')))
			if 'MEMORY' in d:
				result.reqs.append(WMS.MEMORY, utils.parseTime(d.pop('MEMORY')))
		dict2req(result.transient)
		dict2req(result.store)
