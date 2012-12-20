from python_compat import *
from grid_control import *
from psource_base import ParameterSource, ParameterMetadata

class ForwardingParameterSource(ParameterSource):
	def __init__(self, plugin):
		ParameterSource.__init__(self)
		self.plugin = plugin

	def show(self, level = 0, other = ''):
		ParameterSource.show(self, level, other)
		self.plugin.show(level + 1)

	def getMaxJobs(self):
		return self.plugin.getMaxJobs()

	def fillParameterKeys(self, result):
		self.plugin.fillParameterKeys(result)

	def fillParameterInfo(self, pNum, result):
		self.plugin.fillParameterInfo(pNum, result)

	def getParameterIntervention(self):
		(result_redo, result_disable, result_sChange) = ParameterSource.getParameterIntervention(self)
		(plugin_redo, plugin_disable, plugin_sChange) = self.plugin.getParameterIntervention()
		result_redo.update(plugin_redo)
		result_disable.update(plugin_disable)
		return (result_redo, result_disable, result_sChange or plugin_sChange)


class RangeParameterSource(ForwardingParameterSource):
	def __init__(self, plugin, posStart = None, posEnd = None):
		ForwardingParameterSource.__init__(self, plugin)
		self.posStart = QM(posStart == None, 0, posStart)
		self.posEnd = QM(posEnd == None, self.plugin.getMaxJobs() - 1, posEnd)

	def show(self, level = 0):
		ForwardingParameterSource.show(self, level, 'range = (%s, %s)' % (self.posStart, self.posEnd))

	def getMaxJobs(self):
		return self.posEnd - self.posStart + 1

	def getParameterIntervention(self):
		(result_redo, result_disable, result_sChange) = ParameterSource.getParameterIntervention(self)
		(plugin_redo, plugin_disable, plugin_sChange) = self.plugin.getParameterIntervention()
		for pNum in plugin_redo:
			if (pNum >= self.posStart) and (pNum <= self.posEnd):
				result_redo.add(pNum - self.posStart)
		for pNum in plugin_disable:
			if (pNum >= self.posStart) and (pNum <= self.posEnd):
				result_disable.add(pNum - self.posStart)
		return (result_redo, result_disable, result_sChange or plugin_sChange)

	def fillParameterInfo(self, pNum, result):
		self.plugin.fillParameterInfo(pNum + self.posStart, result)
ParameterSource.managerMap['range'] = RangeParameterSource


class BaseMultiParameterSource(ParameterSource):
	def __init__(self, *plugins):
		ParameterSource.__init__(self)
		self.pluginsX = plugins

	def show(self, level = 0):
		ParameterSource.show(self, level)
		for plugin in self.pluginsX:
			plugin.show(level + 1)

	def fillParameterKeys(self, result):
		for plugin in self.pluginsX:
			plugin.fillParameterKeys(result)


class QuickMultiParameterSource(BaseMultiParameterSource):
	def __init__(self, *plugins):
		BaseMultiParameterSource.__init__(self, *filter(lambda p: p, plugins))
		self.mainPlugin = filter(lambda p: p.getMaxJobs(), self.pluginsX)

	def getMaxJobs(self):
		return self.maxJobs

	def fillParameterInfo(self, pNum, result):
		for plugin in self.pluginsX:
			plugin.fillParameterInfo(pNum, result)

	def getParameterIntervention(self): # Quicker version than the general purpose implementation
		(result_redo, result_disable, result_sChange) = ParameterSource.getParameterIntervention(self)
		for (idx, (maxN, plugin)) in enumerate(self.plugins):
			(plugin_redo, plugin_disable, plugin_sChange) = plugin.getParameterIntervention()
			result_redo.update(plugin_redo)
			result_disable.update(plugin_disable)
			result_sChange = result_sChange or plugin_sChange
		return (result_redo, result_disable, result_sChange)


# Meta processing of parameter plugins
# Aggregates and propagates results and changes to plugins
class MultiParameterSource(BaseMultiParameterSource):
	def __init__(self, plugins, maxJobs):
		BaseMultiParameterSource.__init__(self, *plugins)
		self.plugins = map(lambda p: (p.getMaxJobs(), p), plugins)
		self.maxJobs = maxJobs

	def getMaxJobs(self):
		return self.maxJobs

	# Get local parameter numbers (result) from plugin index (pIdx) and subplugin parameter number (pNum)
	def translateNum(self, pIdx, pNum):
		raise

	def getParameterIntervention(self):
		(result_redo, result_disable, result_sChange) = ParameterSource.getParameterIntervention(self)
		for (idx, (maxN, plugin)) in enumerate(self.plugins):
			(plugin_redo, plugin_disable, plugin_sChange) = plugin.getParameterIntervention()
			for pNum in plugin_redo:
				result_redo.update(self.translateNum(idx, pNum))
			for pNum in plugin_disable:
				result_disable.update(self.translateNum(idx, pNum))
			result_sChange = result_sChange or plugin_sChange
		return (result_redo, result_disable, result_sChange)

	def fillParameterKeys(self, result):
		for (maxN, plugin) in self.plugins:
			plugin.fillParameterKeys(result)


# Base class for plugins invoking their sub-plugins in parallel
class BaseZipParameterSource(MultiParameterSource):
	def getParameterIntervention(self): # Quicker version than the general purpose implementation
		(result_redo, result_disable, result_sChange) = ParameterSource.getParameterIntervention(self)
		for (idx, (maxN, plugin)) in enumerate(self.plugins):
			(plugin_redo, plugin_disable, plugin_sChange) = plugin.getParameterIntervention()
			result_redo.update(plugin_redo)
			result_disable.update(plugin_disable)
			result_sChange = result_sChange or plugin_sChange
		return (result_redo, result_disable, result_sChange)

	def fillParameterInfo(self, pNum, result):
		for (maxN, plugin) in self.plugins:
			if maxN:
				if pNum < maxN:
					plugin.fillParameterInfo(pNum, result)
			else:
				plugin.fillParameterInfo(None, result)


class ZipShortParameterSource(BaseZipParameterSource):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		if maxN:
			BaseZipParameterSource.__init__(self, plugins, min(maxN))
		else:
			BaseZipParameterSource.__init__(self, plugins, None)


class ZipLongParameterSource(BaseZipParameterSource):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		if maxN:
			BaseZipParameterSource.__init__(self, plugins, max(maxN))
		else:
			BaseZipParameterSource.__init__(self, plugins, None)
ParameterSource.managerMap['zip'] = ZipLongParameterSource


class ChainParameterSource(MultiParameterSource):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		MultiParameterSource.__init__(self, plugins, sum(maxN))

	def translateNum(self, pIdx, pNum):
		offset = sum(map(lambda (n, p): n, self.plugins[:pIdx]))
		return [pNum + offset]

	def fillParameterInfo(self, pNum, result):
		limit = 0
		for (maxN, p) in self.plugins:
			if pNum < limit + maxN:
				return p.fillParameterInfo(pNum - limit, result)
			limit += maxN
ParameterSource.managerMap['chain'] = ChainParameterSource


class RepeatParameterSource(ChainParameterSource):
	def __init__(self, plugin, times):
		self.plugin = plugin
		self.maxN = plugin.getMaxJobs()
		self.times = times
		if self.maxN:
			MultiParameterSource.__init__(self, [plugin], self.maxN * times)
		else:
			MultiParameterSource.__init__(self, [plugin], times)

	def translateNum(self, pIdx, pNum):
		return map(lambda i: pNum + i * self.maxN, range(self.times))

	def fillParameterInfo(self, pNum, result):
		self.plugin.fillParameterInfo(pNum % self.maxN, result)
ParameterSource.managerMap['repeat'] = RepeatParameterSource


class CrossParameterSource(MultiParameterSource):
	def __init__(self, *plugins):
		maxN = filter(lambda n: n != None, map(lambda p: p.getMaxJobs(), plugins))
		if maxN:
			MultiParameterSource.__init__(self, plugins, reduce(lambda a, b: a * b, maxN))
		else:
			MultiParameterSource.__init__(self, plugins, None)

	def translateNum(self, pIdx, pNum):
		tmp = reduce(lambda a, b: a * b, map(lambda (n, p): n, filter(lambda (n, p): n != None, self.plugins[:pIdx])), 1)
		return filter(lambda x: (x / tmp) % self.plugins[pIdx][0] == pNum, range(self.getMaxJobs()))

	def fillParameterInfo(self, pNum, result):
		prev = 1
		for (maxN, plugin) in self.plugins:
			if maxN:
				plugin.fillParameterInfo((pNum / prev) % maxN, result)
				prev *= maxN
			else:
				plugin.fillParameterInfo(pNum, result)
ParameterSource.managerMap['cross'] = CrossParameterSource


class ErrorParameterSource(ChainParameterSource):
	def __init__(self, *plugins):
		self.rawPlugins = plugins
		central = map(lambda p: RangeParameterSource(p, 0, 0), plugins)
		chain = [ZipLongParameterSource(*central)]
		for pidx, p in enumerate(plugins):
			if p.getMaxJobs():
				tmp = list(central)
				tmp[pidx] = RangeParameterSource(plugins[pidx], 1, None)
				chain.append(CrossParameterSource(*tmp))
		ChainParameterSource.__init__(self, *chain)

	def fillParameterKeys(self, result):
		for plugin in self.rawPlugins:
			plugin.fillParameterKeys(result)
ParameterSource.managerMap['variation'] = ErrorParameterSource


class CombineParameterSource(ZipLongParameterSource):
	# combine according to common parameter value
	pass
ParameterSource.managerMap['combine'] = CombineParameterSource
