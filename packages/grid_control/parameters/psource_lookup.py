#-#  Copyright 2012-2015 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import re
from grid_control import utils
from grid_control.exceptions import ConfigError
from grid_control.parameters.psource_base import ParameterInfo, ParameterSource
from grid_control.parameters.psource_basic import FormatterParameterSource, KeyParameterSource, SimpleParameterSource, SingleParameterSource

class LookupMatcher:
	def __init__(self, lookupKeys, lookupFunctions, lookupDictConfig):
		(self.lookupKeys, self.lookupFunctions) = (lookupKeys, lookupFunctions)
		if len(lookupDictConfig) == 2 and isinstance(lookupDictConfig[0], dict):
			self.lookupDict, self.lookupOrder = lookupDictConfig
		else:
			self.lookupDict, self.lookupOrder = ({None: lookupDictConfig}, [])

	def getHash(self):
		return utils.md5(str(map(lambda x: self.lookupDict, self.lookupOrder))).hexdigest()

	def __repr__(self):
		return 'key(%s)' % str.join(', ', map(lambda x: "'%s'" % x, self.lookupKeys))

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
	if lookup == None:
		lookup = [pconfig.get('default lookup')]
	else:
		lookup = collectKeys(lookup)
	if not lookup or lookup == ['']:
		raise ConfigError('Lookup parameter not defined!')
	matchfun = []
	defaultMatcher = pconfig.get('', 'default matcher', 'equal')
	matchstrList = pconfig.get(key.lstrip('!'), 'matcher', defaultMatcher).lower().splitlines()
	if len(matchstrList) != len(lookup):
		if len(matchstrList) == 1:
			matchstrList = matchstrList * len(lookup)
		else:
			raise ConfigError('Match-functions (length %d) and match-keys (length %d) do not match!' %
				(len(matchstrList), len(lookup)))
	for matchstr in matchstrList:
		if matchstr == 'start':
			matchfun.append(lambda value, pat: value.startswith(pat))
		elif matchstr == 'end':
			matchfun.append(lambda value, pat: value.endswith(pat))
		elif matchstr == 'equal':
			matchfun.append(lambda value, pat: value == pat)
		elif matchstr == 'expr':
			matchfun.append(lambda value, pat: eval('lambda value: %s' % pat)(value))
		elif matchstr == 'regex':
			class MatchObj:
				def __init__(self):
					self.expr = {}
				def __call__(self, value, pat):
					if pat not in self.expr:
						self.expr[pat] = re.compile(pat)
					return self.expr[pat].search(value)
			matchfun.append(MatchObj())
		else:
			raise ConfigError('Invalid matcher selected! "%s"' % matchstr)
	(content, order) = pconfig.getParameter(key.lstrip('!'))
	if pconfig.getBool(key.lstrip('!'), 'empty set', False) == False:
		for k in content:
			if len(content[k]) == 0:
				content[k].append('')
	return (key, lookup, matchfun, (content, order))


class SimpleLookupParameterSource(SingleParameterSource):
	def __init__(self, outputKey, lookupKeys, lookupFunctions, lookupDictConfig):
		SingleParameterSource.__init__(self, outputKey)
		self.matcher = LookupMatcher(lookupKeys, lookupFunctions, lookupDictConfig)

	def fillParameterInfo(self, pNum, result):
		lookupResult = self.matcher.lookup(result)
		if lookupResult == None:
			return
		elif len(lookupResult) != 1:
			raise ConfigError("%s can't handle multiple lookup parameter sets!" % self.__class__.__name__)
		elif lookupResult[0] != None:
			result[self.key] = lookupResult[0]

	def show(self, level = 0):
		ParameterSource.show(self, level, 'var = %s, lookup = %s' % (self.key, str.join(',', self.matcher.lookupKeys)))

	def getHash(self):
		return utils.md5(str(self.key) + self.matcher.getHash()).hexdigest()

	def __repr__(self):
		return "lookup(key('%s'), %s)" % (self.key, repr(self.matcher))

	def create(cls, pconfig, key, lookup = None):
		return SimpleLookupParameterSource(*lookupConfigParser(pconfig, key, lookup))
	create = classmethod(create)


class SwitchingLookupParameterSource(SingleParameterSource):
	def __init__(self, psource, outputKey, lookupKeys, lookupFunctions, lookupDictConfig):
		SingleParameterSource.__init__(self, outputKey)
		self.matcher = LookupMatcher(lookupKeys, lookupFunctions, lookupDictConfig)
		self.psource = psource
		self.pSpace = self.initPSpace()

	def initPSpace(self):
		result = []
		def addEntry(pNum):
			tmp = {ParameterInfo.ACTIVE: True, ParameterInfo.REQS: []}
			self.psource.fillParameterInfo(pNum, tmp)
			lookupResult = self.matcher.lookup(tmp)
			if lookupResult:
				for (lookupIdx, tmp) in enumerate(lookupResult):
					result.append((pNum, lookupIdx))

		if self.psource.getMaxParameters() == None:
			addEntry(None)
		else:
			for pNum in range(self.psource.getMaxParameters()):
				addEntry(pNum)
		if len(result) == 0:
			utils.vprint('Lookup parameter "%s" has no matching entries!' % self.key, -1)
		return result

	def getMaxParameters(self):
		return len(self.pSpace)

	def fillParameterInfo(self, pNum, result):
		if len(self.pSpace) == 0:
			self.psource.fillParameterInfo(pNum, result)
			return
		subNum, lookupIndex = self.pSpace[pNum]
		self.psource.fillParameterInfo(subNum, result)
		result[self.key] = self.matcher.lookup(result)[lookupIndex]

	def fillParameterKeys(self, result):
		result.append(self.meta)
		self.psource.fillParameterKeys(result)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = ParameterSource.resync(self)
		if self.resyncEnabled():
			(psource_redo, psource_disable, psource_sizeChange) = self.psource.resync()
			self.pSpace = self.initPSpace()
			for pNum, pInfo in enumerate(self.pSpace):
				subNum, lookupIndex = pInfo
				if subNum in psource_redo:
					result_redo.add(pNum)
				if subNum in psource_disable:
					result_disable.add(pNum)
			self.resyncFinished()
		return (result_redo, result_disable, result_sizeChange or psource_sizeChange)

	def getHash(self):
		return utils.md5(str(self.key) + self.matcher.getHash() + self.psource.getHash()).hexdigest()

	def __repr__(self):
		return "switch(%r, key('%s'), %s)" % (self.psource, self.key, repr(self.matcher))

	def show(self, level = 0):
		ParameterSource.show(self, level, 'var = %s, lookup = %s' % (self.key, str.join(',', self.matcher.lookupKeys)))
		self.psource.show(level + 1)

	def create(cls, pconfig, psource, key, lookup = None):
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
		return [(False, SimpleParameterSource, [var_name, pvalue])]
	elif isinstance(pvalue, tuple) and pvalue[0] == 'format':
		return [(False, FormatterParameterSource, pvalue[1:])]

	lookup_key = None
	if lookup_list: # default lookup key
		lookup_key = KeyParameterSource(*lookup_list)

	# Determine kind of lookup
	tmp = lookupConfigParser(pconfig, KeyParameterSource(var_name), lookup_key)
	(outputKey, lookupKeys, lookupFunctions, lookupDictConfig) = tmp
	(lookupContent, lookupOrder) = lookupDictConfig
	lookupLen = map(len, lookupContent.values())

	if (min(lookupLen) == 1) and (max(lookupLen) == 1): # simple lookup sufficient for this setup
		return [(False, SimpleLookupParameterSource, list(tmp))]
	# switch needs elevation beyond local scope
	return [(True, SwitchingLookupParameterSource, list(tmp))]
