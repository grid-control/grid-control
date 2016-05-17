# | Copyright 2012-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

from grid_control import utils
from grid_control.config import ConfigError, Matcher
from grid_control.parameters.psource_base import ParameterInfo, ParameterSource
from grid_control.parameters.psource_basic import KeyParameterSource, SingleParameterSource
from python_compat import imap, irange, izip, lmap, md5_hex

class LookupMatcher:
	def __init__(self, lookupKeys, lookupFunctions, lookupDictConfig):
		(self._lookup_keys, self._lookup_functions) = (lookupKeys, lookupFunctions)
		if len(lookupDictConfig) == 2 and isinstance(lookupDictConfig[0], dict):
			self._lookup_dict, self._lookup_order = lookupDictConfig
		else:
			self._lookup_dict, self._lookup_order = ({None: lookupDictConfig}, [])

	def getHash(self):
		return md5_hex(str(lmap(lambda x: self._lookup_dict, self._lookup_order)))

	def __repr__(self):
		return 'key(%s)' % str.join(', ', imap(lambda x: "'%s'" % x, self._lookup_keys))

	def matchRule(self, src):
		srcValues = lmap(lambda key: src.get(key, None), self._lookup_keys)
		for lookupValues in self._lookup_order:
			match = True
			for (sval, lval, lmatch) in izip(srcValues, lookupValues, self._lookup_functions):
				if sval is not None:
					match = match and (lmatch.matcher(sval, lval) > 0)
			if match:
				return lookupValues

	def lookup(self, info):
		rule = self.matchRule(info)
		return self._lookup_dict.get(rule, None)


def lookupConfigParser(pconfig, outputKey, lookupKeys):
	def collectKeys(src):
		result = []
		src.fillParameterKeys(result)
		return result
	outputKey = collectKeys(outputKey)[0]
	if lookupKeys is None:
		lookupKeys = [pconfig.get('default lookup')]
	else:
		lookupKeys = collectKeys(lookupKeys)
	if not lookupKeys or lookupKeys == ['']:
		raise ConfigError('Lookup parameter not defined!')
	defaultMatcher = pconfig.get('', 'default matcher', 'equal')
	matchstr = pconfig.get(outputKey.lstrip('!'), 'matcher', defaultMatcher)
	matchstrList = matchstr.lower().splitlines()
	if len(matchstrList) != len(lookupKeys):
		if len(matchstrList) == 1:
			matchstrList = matchstrList * len(lookupKeys)
		else:
			raise ConfigError('Match-functions (length %d) and match-keys (length %d) do not match!' %
				(len(matchstrList), len(lookupKeys)))
	matchfun = []
	for matcherName in matchstrList:
		matchfun.append(Matcher.createInstance(matcherName, pconfig, outputKey))
	(content, order) = pconfig.getParameter(outputKey.lstrip('!'))
	if not pconfig.getBool(outputKey.lstrip('!'), 'empty set', False):
		for k in content:
			if len(content[k]) == 0:
				content[k].append('')
	return (outputKey, lookupKeys, matchfun, (content, order))


class SimpleLookupParameterSource(SingleParameterSource):
	def __init__(self, outputKey, lookupKeys, lookupFunctions, lookupDictConfig):
		SingleParameterSource.__init__(self, outputKey)
		self._lookupKeys = lookupKeys
		self._matcher = LookupMatcher(lookupKeys, lookupFunctions, lookupDictConfig)

	def depends(self):
		return self._lookupKeys

	def fillParameterInfo(self, pNum, result):
		lookupResult = self._matcher.lookup(result)
		if lookupResult is None:
			return
		elif len(lookupResult) != 1:
			raise ConfigError("%s can't handle multiple lookup parameter sets!" % self.__class__.__name__)
		elif lookupResult[0] is not None:
			result[self._key] = lookupResult[0]

	def show(self):
		return ['%s: var = %s, lookup = %s' % (self.__class__.__name__, self._key, repr(self._matcher))]

	def getHash(self):
		return md5_hex(str(self._key) + self._matcher.getHash())

	def __repr__(self):
		return "lookup(key('%s'), %s)" % (self._key, repr(self._matcher))

	def create(cls, pconfig, key, lookup = None): # pylint:disable=arguments-differ
		return SimpleLookupParameterSource(*lookupConfigParser(pconfig, key, lookup))
	create = classmethod(create)


class SwitchingLookupParameterSource(SingleParameterSource):
	def __init__(self, psource, outputKey, lookupKeys, lookupFunctions, lookupDictConfig):
		SingleParameterSource.__init__(self, outputKey)
		self._matcher = LookupMatcher(lookupKeys, lookupFunctions, lookupDictConfig)
		self._psource = psource
		self._pSpace = self.initPSpace()

	def initPSpace(self):
		result = []
		def addEntry(pNum):
			tmp = {ParameterInfo.ACTIVE: True, ParameterInfo.REQS: []}
			self._psource.fillParameterInfo(pNum, tmp)
			lookupResult = self._matcher.lookup(tmp)
			if lookupResult:
				for (lookupIdx, tmp) in enumerate(lookupResult):
					result.append((pNum, lookupIdx))

		if self._psource.getMaxParameters() is None:
			addEntry(None)
		else:
			for pNum in irange(self._psource.getMaxParameters()):
				addEntry(pNum)
		if len(result) == 0:
			utils.vprint('Lookup parameter "%s" has no matching entries!' % self._key, -1)
		return result

	def getMaxParameters(self):
		return len(self._pSpace)

	def fillParameterInfo(self, pNum, result):
		if len(self._pSpace) == 0:
			self._psource.fillParameterInfo(pNum, result)
			return
		subNum, lookupIndex = self._pSpace[pNum]
		self._psource.fillParameterInfo(subNum, result)
		result[self._key] = self._matcher.lookup(result)[lookupIndex]

	def fillParameterKeys(self, result):
		result.append(self._meta)
		self._psource.fillParameterKeys(result)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = ParameterSource.resync(self)
		if self.resyncEnabled():
			(psource_redo, psource_disable, psource_sizeChange) = self._psource.resync()
			self._pSpace = self.initPSpace()
			for pNum, pInfo in enumerate(self._pSpace):
				subNum, _ = pInfo # ignore lookupIndex
				if subNum in psource_redo:
					result_redo.add(pNum)
				if subNum in psource_disable:
					result_disable.add(pNum)
			self.resyncFinished()
		return (result_redo, result_disable, result_sizeChange or psource_sizeChange)

	def getHash(self):
		return md5_hex(str(self._key) + self._matcher.getHash() + self._psource.getHash())

	def __repr__(self):
		return "switch(%r, key('%s'), %s)" % (self._psource, self._key, repr(self._matcher))

	def show(self):
		result = ['%s: var = %s, lookup = %s' % (self.__class__.__name__, self._key, repr(self._matcher))]
		return result + lmap(lambda x: '\t' + x, self._psource.show())

	def create(cls, pconfig, psource, key, lookup = None): # pylint:disable=arguments-differ
		return SwitchingLookupParameterSource(psource, *lookupConfigParser(pconfig, key, lookup))
	create = classmethod(create)


def createLookupHelper(pconfig, var_list, lookup_list):
	# Return list of (doElevate, PSourceClass, arguments) entries
	if len(var_list) != 1: # multi-lookup handling
		result = []
		for var_name in var_list:
			result.extend(createLookupHelper(pconfig, [var_name], lookup_list))
		return result
	var_name = var_list[0]

	pvalue = pconfig.getParameter(var_name.lstrip('!'))
	if isinstance(pvalue, list): # simple parameter source
		if len(pvalue) == 1:
			return [(False, ParameterSource.getClass('ConstParameterSource'), [var_name, pvalue[0]])]
		else:
			return [(False, ParameterSource.getClass('SimpleParameterSource'), [var_name, pvalue])]
	elif isinstance(pvalue, tuple) and pvalue[0] == 'format':
		return [(False, ParameterSource.getClass('FormatterParameterSource'), pvalue[1:])]

	lookup_key = None
	if lookup_list: # default lookup key
		lookup_key = KeyParameterSource(*lookup_list)

	# Determine kind of lookup, [3] == lookupDictConfig, [0] == lookupContent
	tmp = lookupConfigParser(pconfig, KeyParameterSource(var_name), lookup_key)
	lookupContent = tmp[3][0]
	lookupLen = lmap(len, lookupContent.values())

	if (min(lookupLen) == 1) and (max(lookupLen) == 1): # simple lookup sufficient for this setup
		return [(False, SimpleLookupParameterSource, list(tmp))]
	# switch needs elevation beyond local scope
	return [(True, SwitchingLookupParameterSource, list(tmp))]
