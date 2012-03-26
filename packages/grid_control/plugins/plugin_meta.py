from python_compat import *
from plugin_base import ParameterPlugin, ParameterMetadata

# Meta processing of parameter plugins
# Aggregates and propagates results and changes to plugins
class MultiParaPlugin(ParameterPlugin):
	def __init__(self, plugins, maxJobs):
		ParameterPlugin.__init__(self)
		self.plugins = map(lambda p: (p.getMaxJobs(), p), plugins)
		self.maxJobs = maxJobs

	def getMaxJobs(self):
		return self.maxJobs

	# Get local parameter numbers (result) from plugin index (pIdx) and subplugin parameter number (pNum)
	def translateNum(self, pIdx, pNum):
		raise

	def getPNumIntervention(self):
		(result_redo, result_disable, result_sChange) = ParameterPlugin.getPNumIntervention(self)
		for (idx, (maxN, plugin)) in enumerate(self.plugins):
			(plugin_redo, plugin_disable, plugin_sChange) = plugin.getPNumIntervention()
			for pNum in plugin_redo:
				result_redo.update(self.translateNum(idx, pNum))
			for pNum in plugin_disable:
				result_disable.update(self.translateNum(idx, pNum))
			result_sChange = result_sChange or plugin_sChange
		return (result_redo, result_disable, result_sChange)

	def getParameterNames(self, result):
		for (maxN, plugin) in self.plugins:
			plugin.getParameterNames(result)

	def resolveDeps(self):
		self.plugins.sort(key = lambda (n, p): p.resolveDeps())
		return reduce(lambda a, b: a + b, map(lambda (n, p): p.resolveDeps(), self.plugins))


# Base class for plugins invoking their sub-plugins in parallel
class BaseZipParaPlugin(MultiParaPlugin):
	def getPNumIntervention(self): # Quicker version than the general purpose implementation
		(result_redo, result_disable, result_sChange) = ParameterPlugin.getPNumIntervention(self)
		for (idx, (maxN, plugin)) in enumerate(self.plugins):
			(plugin_redo, plugin_disable, plugin_sChange) = plugin.getPNumIntervention()
			result_redo.update(plugin_redo)
			result_disable.update(plugin_disable)
			result_sChange = result_sChange or plugin_sChange
		return (result_redo, result_disable, result_sChange)

	def getParameters(self, pNum, result):
		for (maxN, plugin) in self.plugins:
			if maxN:
				if pNum < maxN:
					plugin.getParameters(pNum, result)
			else:
				plugin.getParameters(None, result)


class ZipShortPlugin(BaseZipParaPlugin):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		if maxN:
			BaseZipParaPlugin.__init__(self, plugins, min(maxN))
		else:
			BaseZipParaPlugin.__init__(self, plugins, None)


class ZipLongParaPlugin(BaseZipParaPlugin):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		if maxN:
			BaseZipParaPlugin.__init__(self, plugins, max(maxN))
		else:
			BaseZipParaPlugin.__init__(self, plugins, None)


class CombinePlugin(ZipLongParaPlugin):
	pass
ParameterPlugin.rawManagerMap['zip'] = CombinePlugin


class ChainParaPlugin(MultiParaPlugin):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		MultiParaPlugin.__init__(self, plugins, sum(maxN))

	def translateNum(self, pIdx, pNum):
		offset = sum(map(lambda (n, p): n, self.plugins[:pIdx]))
		return [pNum + offset]

	def resolveDeps(self): # Dont change order in chain
		return reduce(lambda a, b: a + b, map(lambda (n, p): p.resolveDeps(), self.plugins))

	def getParameters(self, pNum, result):
		limit = 0
		for (maxN, p) in self.plugins:
			if pNum < limit + maxN:
				return p.getParameters(pNum - limit, result)
			limit += maxN
ParameterPlugin.rawManagerMap['chain'] = ChainParaPlugin


class RepeatParaPlugin(ChainParaPlugin):
	def __init__(self, plugin, times):
		self.plugin = plugin
		self.maxN = plugin.getMaxJobs()
		if self.maxN:
			MultiParaPlugin.__init__(self, [plugin], self.maxN * times)
		else:
			MultiParaPlugin.__init__(self, [plugin], times)

	def translateNum(self, pIdx, pNum):
		return map(lambda i: pNum + i * self.maxN, range(times))

	def getParameters(self, pNum, result):
		return self.plugin.getParameters(pNum % self.maxN, result)
ParameterPlugin.rawManagerMap['repeat'] = RepeatParaPlugin


class CrossParaPlugin(MultiParaPlugin):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		MultiParaPlugin.__init__(self, plugins, reduce(lambda a, b: a * b, maxN))

	def translateNum(self, pIdx, pNum):
		tmp = reduce(lambda a, b: a * b, map(lambda (n, p): n, self.plugins[:pIdx]), 1)
		return filter(lambda x: (x / tmp) % self.plugins[pIdx][0] == pNum, range(self.getMaxJobs()))

	def getParameters(self, pNum, result):
		prev = 1
		for (maxN, plugin) in self.plugins:
			if maxN:
				plugin.getParameters((pNum / prev) % maxN, result)
				prev *= maxN
			else:
				plugin.getParameters(None, result)
ParameterPlugin.rawManagerMap['cross'] = CrossParaPlugin


class ErrorParaPlugin(ChainParaPlugin):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		central = map(lambda p: RangePlugin(p, 0, 0), plugins)
		chain = [CombinePlugin(central)]
		for pidx, p in enumerate(plugins):
			if p.getMaxJobs():
				tmp = list(central)
				tmp[pidx] = RangePlugin(plugins[pidx], 1, None)
				chain.append(CrossPlugin(tmp))
		ChainParaPlugin.__init__(self, *chain)
ParameterPlugin.rawManagerMap['variation'] = ErrorParaPlugin
