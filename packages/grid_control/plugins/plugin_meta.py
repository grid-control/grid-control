from python_compat import *
from plugin_base import ParameterPlugin

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

	def getIntervention(self):
		(result_redo, result_disable) = (set(), set())
		for (idx, (maxN, plugin)) in enumerate(self.plugins):
			tmp = plugin.getIntervention()
			if tmp:
				(plugin_redo, plugin_disable) = tmp
				for pNum in plugin_redo:
					result_redo.update(self.translateNum(idx, pNum))
				for pNum in plugin_disable:
					result_disable.update(self.translateNum(idx, pNum))
		result_redo = result_redo.difference(result_disable)
		if len(result_redo) or len(result_disable):
			return (list(result_redo), list(result_disable))
		return None

	def getParameterNames(self):
		(result_store, result_transient) = (set(), set())
		for (maxN, plugin) in self.plugins:
			(plugin_store, plugin_transient) = plugin.getParameterNames()
			result_store.update(plugin_store)
			result_transient.update(plugin_transient)
		return (list(result_store), list(result_transient))


# Base class for plugins invoking their sub-plugins in parallel
class BaseZipParaPlugin(MultiParaPlugin):
	def translateNum(self, pIdx, pNum):
		return [pNum]

	def getParameters(self, pNum, result):
		for (maxN, plugin) in self.plugins:
			if maxN:
				if pNum < maxN:
					plugin.getParameters(pNum, result)
			else:
				plugin.getParameters(None, result)


class ZipLongParaPlugin(BaseZipParaPlugin):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		if maxN:
			BaseZipParaPlugin.__init__(self, plugins, max(maxN))
		else:
			BaseZipParaPlugin.__init__(self, plugins, None)


class ZipShortPlugin(BaseZipParaPlugin):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		if maxN:
			BaseZipParaPlugin.__init__(self, plugins, min(maxN))
		else:
			BaseZipParaPlugin.__init__(self, plugins, None)


class CombinePlugin(ZipLongParaPlugin):
	def __init__(self, *plugins):
		ZipLongParaPlugin.__init__(self, *plugins)
#		assert(self.maxJobs == min(filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))))
ParameterPlugin.rawManagerMap['zip'] = CombinePlugin


class ChainParaPlugin(MultiParaPlugin):
	def __init__(self, *plugins):
		self.maxN = map(lambda p: p.getMaxJobs(), plugins)
		MultiParaPlugin.__init__(self, plugins, reduce(lambda a, b: a + b, filter(lambda n: n != None, self.maxN)))

	def translateNum(self, pIdx, pNum):
		return [pNum + sum(self.maxN[:pIdx])]

	def getParameters(self, pNum, result):
		limit = 0
		for (maxN, p) in self.plugins:
			if pNum < limit + maxN:
				return p.getParameters(pNum - limit, result)
			limit += maxN
ParameterPlugin.rawManagerMap['chain'] = ChainParaPlugin


class ChainParaPlugin(MultiParaPlugin):
	def __init__(self, *plugins):
		self.maxN = map(lambda p: p.getMaxJobs(), plugins)
		MultiParaPlugin.__init__(self, plugins, reduce(lambda a, b: a + b, filter(lambda n: n != None, self.maxN)))

	def translateNum(self, pIdx, pNum):
		return [pNum + sum(self.maxN[:pIdx])]

	def getParameters(self, pNum, result):
		limit = 0
		for (maxN, p) in self.plugins:
			if pNum < limit + maxN:
				return p.getParameters(pNum - limit, result)
			limit += maxN
ParameterPlugin.rawManagerMap['variation'] = ChainParaPlugin


class RepeatParaPlugin(ChainParaPlugin):
	def __init__(self, plugin, times):
		plugins = [plugin] * times
		ChainParaPlugin.__init__(self, *plugins)
ParameterPlugin.rawManagerMap['repeat'] = RepeatParaPlugin


class CrossParaPlugin(MultiParaPlugin):
	def __init__(self, *plugins):
		self.maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		MultiParaPlugin.__init__(self, plugins, reduce(lambda a, b: a * b, self.maxN))

	def translateNum(self, pIdx, pNum):
		tmp = reduce(lambda a, b: a * b, self.maxN[:pIdx], 1)
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
