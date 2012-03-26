import random
from python_compat import *
from grid_control import ConfigError, utils, WMS
from plugin_base import ParameterPlugin, ParameterMetadata

class InternalPlugin(ParameterPlugin):
	def __init__(self, values, names):
		ParameterPlugin.__init__(self)
		(self.values, self.names) = (values, names)

	def getMaxJobs(self):
		return len(self.values)

	def getParameterNames(self, result):
		result.update(self.names)

	def getParameters(self, pNum, result):
		result.update(self.values[pNum])


class SimpleParaPlugin(ParameterPlugin):
	def __init__(self, key, values):
		ParameterPlugin.__init__(self)
		if values == None:
			raise ConfigError('Missing values for %s' % key)
		(self.key, self.values) = (key, values)

	def getMaxJobs(self):
		return len(self.values)

	def getParameterNames(self, result):
		result.add(ParameterMetadata(self.key))

	def getParameters(self, pNum, result):
		result[self.key] = self.values[pNum]
ParameterPlugin.varManagerMap['var'] = lambda varMap, key: SimpleParaPlugin(key, varMap.get(key.lower()))


class ConstParaPlugin(ParameterPlugin):
	def __init__(self, key, value):
		ParameterPlugin.__init__(self)
		(self.key, self.value) = (key, value)

	def getParameters(self, pNum, result):
		result[self.key] = self.value

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

	def resolveDeps(self):
		return [self.lookup]

	def getParameterNames(self, result):
		result.add(ParameterMetadata(self.key))

	def getParameters(self, pNum, result):
		lookup = result.get(self.lookup, None)
		value = self.values.get(lookup, self.values.get(None))
		if value != None:
			result[self.key] = value
ParameterPlugin.varManagerMap['lookup'] = lambda varMap, key, lookup: LookupParaPlugin(key, varMap.get(key.lower()), lookup)


class RNGParaPlugin(ParameterPlugin):
	def __init__(self, key = 'JOB_RANDOM', low = 1e6, high = 1e7-1):
		ParameterPlugin.__init__(self)
		(self.key, self.low, self.high) = (key, low, high)

	def getParameterNames(self, result):
		result.add(ParameterMetadata(self.key, transient=True))

	def getParameters(self, pNum, result):
		result[self.key] = random.randint(self.low, self.high)
ParameterPlugin.rawManagerMap['rng'] = RNGParaPlugin


class CounterParaPlugin(ParameterPlugin):
	def __init__(self, key, seed):
		ParameterPlugin.__init__(self)
		(self.key, self.seed) = (key, seed)

	def getParameterNames(self, result):
		result.add(ParameterMetadata(self.key, transient=True))

	def getParameters(self, pNum, result):
		result[self.key] = self.seed + result['MY_JOBID']
ParameterPlugin.rawManagerMap['counter'] = CounterParaPlugin


class RequirementParaPlugin(ParameterPlugin):
	def resolveDeps(self):
		return ['WALLTIME', 'CPUTIME', 'MEMORY']

	def getParameterNames(self, result):
		for key in ['WALLTIME', 'CPUTIME', 'MEMORY']:
			if key in result:
				result.remove(key)

	def getParameters(self, pNum, result):
		if 'WALLTIME' in result:
			result[ParameterInfo.REQS].append(WMS.WALLTIME, utils.parseTime(result.pop('WALLTIME')))
		if 'CPUTIME' in result:
			result[ParameterInfo.REQS].append(WMS.CPUTIME, utils.parseTime(result.pop('CPUTIME')))
		if 'MEMORY' in result:
			result[ParameterInfo.REQS].append(WMS.MEMORY, utils.parseTime(result.pop('MEMORY')))


class FormatterParaPlugin(ParameterPlugin):
	def __init__(self, key, source, fmt, default = ''):
		ParameterPlugin.__init__(self)
		(self.key, self.fmt, self.source, self.default) = (key, fmt, source, default)

	def getParameterNames(self, result):
		result.add(ParameterMetadata(self.key, transient=True))

	def getParameters(self, pNum, result):
		result[self.key] = self.fmt % utils.parseType(str(result.get(self.source, self.default)))
ParameterPlugin.rawManagerMap['format'] = FormatterParaPlugin
