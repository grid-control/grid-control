from psource_base import ParameterSource
from psource_basic import SingleParameterSource

class LookupMatcher:
	def __init__(self, lookupKeys, lookupFunctions, lookupDictConfig):
		(self.lookupKeys, self.lookupFunctions) = (lookupKeys, lookupFunctions)
		if len(lookupDictConfig) == 2 and isinstance(lookupDictConfig[0], dict):
			self.lookupDict, self.lookupOrder = lookupDictConfig
		else:
			self.lookupDict, self.lookupOrder = ({None: lookupDictConfig}, [])

	def matchRule(self, src):
		srcValues = map(lambda key: src.get(key, None), self.lookupKeys)
		for lookupValues in self.lookupOrder:
			match = True
			for (sval, lval, lmatch) in zip(srcValues, lookupValues, self.lookupFunctions):
				if sval != None:
					match = match and lmatch(sval, lval)
			if match:
				return lookupValues

	def lookup(self, info):
		rule = self.matchRule(info)
		return self.lookupDict.get(rule, None)


def lookupConfigParser(pconfig, key, lookup):
	def collectKeys(src):
		result = []
		src.fillParameterKeys(result)
		return result
	key = collectKeys(key)[0]
	lookup = collectKeys(lookup)
	matchstr = pconfig.get(key.lstrip('!'), 'matcher', 'start').lower()
	if matchstr == 'start':
		matchfun = lambda value, pat: value.startswith(pat)
	elif matchstr == 'end':
		matchfun = lambda value, pat: value.endswith(pat)
	elif matchstr == 'regex':
		class MatchObj:
			def __init__(self, expr):
				self.expr = re.compile(expr)
			def __call__(self, value, pat):
				return self.expr.search(value)
		matchfun = MatchObj(pat)
	return (key, lookup, [matchfun] * len(lookup), pconfig.getParameter(key.lstrip('!')))


class SimpleLookupParameterSource(SingleParameterSource):
	def __init__(self, outputKey, lookupKeys, lookupFunctions, lookupDictConfig):
		SingleParameterSource.__init__(self, outputKey)
		self.matcher = LookupMatcher(lookupKeys, lookupFunctions, lookupDictConfig)

	def fillParameterInfo(self, pNum, result):
		lookupResult = self.matcher.lookup(result)
		if lookupResult == None:
			return
		elif len(lookupResult) != 1:
			raise ConfigError('Use "switch" for multiple lookup parameter sets!')
		elif lookupResult[0] != None:
			result[self.key] = lookupResult[0]

	def create(cls, pconfig, key, lookup):
		return SimpleLookupParameterSource(*lookupConfigParser(pconfig, key, lookup))
	create = classmethod(create)
ParameterSource.managerMap['lookup'] = SimpleLookupParameterSource


class SwitchingLookupParameterSource(SingleParameterSource):
	def __init__(self, plugin, outputKey, lookupKeys, lookupFunctions, lookupDictConfig):
		SingleParameterSource.__init__(self, outputKey)
		self.matcher = LookupMatcher(lookupKeys, lookupFunctions, lookupDictConfig)
		(self.plugin, self.pSpace) = (plugin, [])
		def addEntry(jobNum):
			lookupResult = self.matcher.lookup(self.plugin.getJobInfo(jobNum))
			if lookupResult:
				for (lookupIdx, tmp) in enumerate(lookupResult):
					self.pSpace.append((jobNum, lookupIdx))

		if self.plugin.getMaxJobs() == None:
			addEntry(None)
		else:
			for jobNum in range(self.plugin.getMaxJobs()):
				addEntry(jobNum)

	def getMaxJobs(self):
		return len(self.pSpace)

	def fillParameterInfo(self, pNum, result):
		subNum, lookupIndex = self.pSpace[pNum]
		self.plugin.fillParameterInfo(subNum, result)
		result[self.key] = self.matcher.lookup(result)[lookupIndex]

	def fillParameterKeys(self, result):
		result.append(self.meta)
		self.plugin.fillParameterKeys(result)

	def getParameterIntervention(self):
		(result_redo, result_disable, result_sChange) = ParameterSource.getParameterIntervention(self)
		(plugin_redo, plugin_disable, plugin_sChange) = self.plugin.getParameterIntervention()
		for jobNum, pInfo in enumerate(self.pSpace):
			subNum = pInfo[0]
			if subNum in plugin_redo:
				result_redo.add(jobNum)
			if subNum in plugin_disable:
				result_disable.add(jobNum)
		return (result_redo, result_disable, result_sChange or plugin_sChange)

	def create(cls, pconfig, plugin, key, lookup):
		return SwitchingLookupParameterSource(plugin, *lookupConfigParser(pconfig, key, lookup))
	create = classmethod(create)
ParameterSource.managerMap['switch'] = SwitchingLookupParameterSource
