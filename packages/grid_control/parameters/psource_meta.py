from grid_control import QM
from python_compat import *
from psource_base import ParameterSource, ParameterMetadata

def combineSyncResult(a, b, sc_fun = lambda x, y: x or y):
	if a == None:
		return b
	(redo_a, disable_a, sizeChange_a) = a
	(redo_b, disable_b, sizeChange_b) = b
	redo_a.update(redo_b)
	disable_a.update(disable_b)
	return (redo_a, disable_a, sc_fun(sizeChange_a, sizeChange_b))

class ForwardingParameterSource(ParameterSource):
	def __init__(self, plugin):
		ParameterSource.__init__(self)
		self.plugin = plugin

	def getMaxParameters(self):
		return self.plugin.getMaxParameters()

	def fillParameterKeys(self, result):
		self.plugin.fillParameterKeys(result)

	def fillParameterInfo(self, pNum, result):
		self.plugin.fillParameterInfo(pNum, result)

	def resync(self):
		return self.plugin.resync()

	def show(self, level = 0, other = ''):
		ParameterSource.show(self, level, other)
		self.plugin.show(level + 1)

	def getHash(self):
		return self.plugin.getHash()


class RangeParameterSource(ForwardingParameterSource):
	def __init__(self, plugin, posStart = None, posEnd = None):
		ForwardingParameterSource.__init__(self, plugin)
		self.posStart = QM(posStart == None, 0, posStart)
		self.posEndUser = posEnd
		self.posEnd = QM(self.posEndUser == None, self.plugin.getMaxParameters() - 1, self.posEndUser)

	def getMaxParameters(self):
		return self.posEnd - self.posStart + 1

	def getHash(self):
		return md5(self.plugin.getHash() + str([self.posStart, self.posEnd])).hexdigest()

	def fillParameterInfo(self, pNum, result):
		self.plugin.fillParameterInfo(pNum + self.posStart, result)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = self.resyncCreate()
		(plugin_redo, plugin_disable, plugin_sizeChange) = self.plugin.resync()
		for pNum in plugin_redo:
			if (pNum >= self.posStart) and (pNum <= self.posEnd):
				result_redo.add(pNum - self.posStart)
		for pNum in plugin_disable:
			if (pNum >= self.posStart) and (pNum <= self.posEnd):
				result_disable.add(pNum - self.posStart)
		oldPosEnd = self.posEnd
		self.posEnd = QM(self.posEndUser == None, self.plugin.getMaxParameters() - 1, self.posEndUser)
		return (result_redo, result_disable, result_sizeChange or (oldPosEnd != self.posEnd))

	def show(self, level = 0):
		ForwardingParameterSource.show(self, level, 'range = (%s, %s)' % (self.posStart, self.posEnd))
ParameterSource.managerMap['range'] = RangeParameterSource


# Meta processing of parameter plugins
class BaseMultiParameterSource(ParameterSource):
	def __init__(self, *plugins):
		ParameterSource.__init__(self)
		self.pluginList = plugins
		self.pluginMaxList = map(lambda p: p.getMaxParameters(), self.pluginList)
		self.maxParameters = self.initMaxParameters()

	def getMaxParameters(self):
		return self.maxParameters

	def initMaxParameters(self):
		raise AbstractError

	def fillParameterKeys(self, result):
		for plugin in self.pluginList:
			plugin.fillParameterKeys(result)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = self.resyncCreate()
		self.pluginMaxList = map(lambda p: p.getMaxParameters(), self.pluginList)
		oldMaxParameters = self.maxParameters
		self.maxParameters = self.initMaxParameters()
		return (result_redo, result_disable, result_sizeChange or (oldMaxParameters != self.maxParameters))

	def show(self, level = 0):
		ParameterSource.show(self, level)
		for plugin in self.pluginList:
			plugin.show(level + 1)

	def getHash(self):
		return md5(str(map(lambda p: str(p.getMaxParameters()) + p.getHash(), self.pluginList))).hexdigest()


# Aggregates and propagates results and changes to plugins
class MultiParameterSource(BaseMultiParameterSource):
	# Get local parameter numbers (result) from plugin index (pIdx) and subplugin parameter number (pNum)
	def translateNum(self, pIdx, pNum):
		raise AbstractError

	def resync(self):
		plugin_resyncList = map(lambda p: p.resync(), self.pluginList)
		(result_redo, result_disable, result_sizeChange) = BaseMultiParameterSource.resync(self)
		for (idx, plugin_resync) in enumerate(plugin_resyncList):
			(plugin_redo, plugin_disable, plugin_sizeChange) = plugin_resync
			for pNum in plugin_redo:
				result_redo.update(self.translateNum(idx, pNum))
			for pNum in plugin_disable:
				result_disable.update(self.translateNum(idx, pNum))
		return (result_redo, result_disable, result_sizeChange)


# Base class for plugins invoking their sub-plugins in parallel
class BaseZipParameterSource(MultiParameterSource):
	def fillParameterInfo(self, pNum, result):
		for (plugin, maxN) in zip(self.pluginList, self.pluginMaxList):
			if maxN != None:
				if pNum < maxN:
					plugin.fillParameterInfo(pNum, result)
			else:
				plugin.fillParameterInfo(pNum, result)

	def resync(self): # Quicker version than the general purpose implementation
		result = self.resyncCreate()
		for plugin in self.pluginList:
			result = combineSyncResult(result, plugin.resync())
		oldMaxParameters = self.maxParameters
		self.maxParameters = self.initMaxParameters()
		return (result[0], result[1], oldMaxParameters != self.maxParameters)


class ZipShortParameterSource(BaseZipParameterSource):
	def initMaxParameters(self):
		maxN = filter(lambda n: n != None, self.pluginMaxList)
		if len(maxN):
			return min(maxN)

class ZipLongParameterSource(BaseZipParameterSource):
	def initMaxParameters(self):
		maxN = filter(lambda n: n != None, self.pluginMaxList)
		if len(maxN):
			return max(maxN)

	def __repr__(self):
		return 'zip(%s)' % str.join(', ', map(repr, self.pluginList))
ParameterSource.managerMap['zip'] = ZipLongParameterSource


class ChainParameterSource(MultiParameterSource):
	def initMaxParameters(self):
		self.offsetList = map(lambda pIdx: sum(self.pluginMaxList[:pIdx]), range(len(self.pluginList)))
		return sum(self.pluginMaxList)

	def translateNum(self, pIdx, pNum):
		return [pNum + self.offsetList[pIdx]]

	def fillParameterInfo(self, pNum, result):
		limit = 0
		for (plugin, maxN) in zip(self.pluginList, self.pluginMaxList):
			if pNum < limit + maxN:
				return plugin.fillParameterInfo(pNum - limit, result)
			limit += maxN

	def __repr__(self):
		return 'chain(%s)' % str.join(', ', map(repr, self.pluginList))
ParameterSource.managerMap['chain'] = ChainParameterSource


class RepeatParameterSource(ChainParameterSource):
	def __init__(self, plugin, times):
		self.plugin = plugin
		self.times = times
		MultiParameterSource.__init__(self, plugin)

	def initMaxParameters(self):
		self.maxN = self.plugin.getMaxParameters()
		if self.maxN != None:
			return self.times * self.maxN
		return self.times

	def translateNum(self, pIdx, pNum):
		return map(lambda i: pNum + i * self.maxN, range(self.times))

	def fillParameterInfo(self, pNum, result):
		self.plugin.fillParameterInfo(pNum % self.maxN, result)

	def show(self, level = 0):
		ParameterSource.show(self, level, 'times = %d' % self.times)
		self.plugin.show(level + 1)

	def __repr__(self):
		return 'repeat(%s, %d)' % (repr(self.plugin), self.times)
ParameterSource.managerMap['repeat'] = RepeatParameterSource


class CrossParameterSource(MultiParameterSource):
	def initMaxParameters(self):
		self.quickFill = []
		prev = 1
		for (plugin, maxN) in zip(self.pluginList, self.pluginMaxList):
			self.quickFill.append((plugin, maxN, prev))
			if maxN:
				prev *= maxN
		maxList = filter(lambda n: n != None, self.pluginMaxList)
		if maxList:
			return reduce(lambda a, b: a * b, maxList)

	def translateNum(self, pIdx, pNum):
		plugin, maxN, prev = self.quickFill[pIdx]
		return filter(lambda x: (x / prev) % maxN == pNum, range(self.getMaxParameters()))

	def fillParameterInfo(self, pNum, result):
		for (plugin, maxN, prev) in self.quickFill:
			if maxN:
				plugin.fillParameterInfo((pNum / prev) % maxN, result)
			else:
				plugin.fillParameterInfo(pNum, result)

	def __repr__(self):
		return 'cross(%s)' % str.join(', ', map(repr, self.pluginList))
ParameterSource.managerMap['cross'] = CrossParameterSource


class ErrorParameterSource(ChainParameterSource):
	def __init__(self, *plugins):
		self.rawPlugins = plugins
		central = map(lambda p: RangeParameterSource(p, 0, 0), plugins)
		chain = [ZipLongParameterSource(*central)]
		for pidx, p in enumerate(plugins):
			if p.getMaxParameters() != None:
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
