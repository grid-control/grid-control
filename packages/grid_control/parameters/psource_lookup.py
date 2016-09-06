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

from grid_control.config import ConfigError, Matcher
from grid_control.parameters.psource_base import ParameterError, ParameterInfo, ParameterSource
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
		if len(self._lookup_keys) == 1:
			return repr(self._lookup_keys[0])
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


class SimpleLookupParameterSource(SingleParameterSource):
	alias = ['lookup']

	def __init__(self, vn_output, lookupKeys, lookupFunctions, lookupDictConfig):
		self._lookupKeys = lookupKeys
		self._matcher = LookupMatcher(lookupKeys, lookupFunctions, lookupDictConfig)
		SingleParameterSource.__init__(self, vn_output, [vn_output, self._matcher.getHash()])

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

	def __repr__(self):
		return "lookup('%s', %s)" % (self._key, repr(self._matcher))

	def create(cls, pconfig, repository, key, lookup = None): # pylint:disable=arguments-differ
		return SimpleLookupParameterSource(*parse_lookup_create_args(pconfig, key, lookup))
	create = classmethod(create)


class SwitchingLookupParameterSource(SingleParameterSource):
	alias = ['switch']

	def __init__(self, psource, vn_output, lookupKeys, lookupFunctions, lookupDictConfig):
		SingleParameterSource.__init__(self, vn_output, [])
		self._matcher = LookupMatcher(lookupKeys, lookupFunctions, lookupDictConfig)
		self._psource = psource
		self._pSpace = self.initPSpace()

	def getHash(self):
		return md5_hex(self._key + self._matcher.getHash() + self._psource.getHash())

	def getUsedSources(self):
		return [self] + self._psource.getUsedSources()

	def initPSpace(self):
		result = []
		def addEntry(pNum):
			tmp = {ParameterInfo.ACTIVE: True, ParameterInfo.REQS: [], 'GC_JOB_ID': pNum, 'GC_PARAM': pNum}
			self._psource.fillParameterInfo(pNum, tmp)
			lookupResult = self._matcher.lookup(tmp)
			if lookupResult:
				for (lookupIdx, tmp) in enumerate(lookupResult):
					result.append((pNum, lookupIdx))

		if self._psource.getMaxParameters() is None:
			raise ParameterError('Unable to use %r with an infinite parameter space!' % self.__class__.__name__)
		else:
			for pNum in irange(self._psource.getMaxParameters()):
				addEntry(pNum)
		if len(result) == 0:
			self._log.critical('Lookup parameter "%s" has no matching entries!', self._key)
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
		(result_redo, result_disable, _) = ParameterSource.EmptyResyncResult()
		(psource_redo, psource_disable, psource_sizeChange) = self._psource.resync()
		self._pSpace = self.initPSpace()
		for pNum, pInfo in enumerate(self._pSpace):
			subNum, _ = pInfo # ignore lookupIndex
			if subNum in psource_redo:
				result_redo.add(pNum)
			if subNum in psource_disable:
				result_disable.add(pNum)
		return (result_redo, result_disable, psource_sizeChange)

	def __repr__(self):
		return "switch(%r, '%s', %s)" % (self._psource, self._key, repr(self._matcher))

	def show(self):
		result = ['%s: var = %s, lookup = %s' % (self.__class__.__name__, self._key, repr(self._matcher))]
		return result + lmap(lambda x: '\t' + x, self._psource.show())

	def create(cls, pconfig, repository, psource, key, lookup = None): # pylint:disable=arguments-differ
		return SwitchingLookupParameterSource(psource, *parse_lookup_create_args(pconfig, key, lookup))
	create = classmethod(create)


def parse_lookup_create_args(pconfig, user_output, user_lookup_list):
	# Transform output and lookup input: eg. key('A', 'B') -> ['A', 'B']
	def keys_to_vn_list(src):
		result = []
		src.fillParameterKeys(result)
		return lmap(lambda meta: meta.value, result)
	if isinstance(user_output, str):
		vn_output = user_output
	else:
		vn_output = keys_to_vn_list(user_output)[0]
	if isinstance(user_lookup_list, str):
		vn_lookup_list = user_lookup_list.split()
	elif user_lookup_list is not None:
		vn_lookup_list = keys_to_vn_list(user_lookup_list)
	else: # no lookup information given - query config for default lookup variable
		vn_lookup_list = [pconfig.get('default lookup')]
	if not vn_lookup_list or vn_lookup_list == ['']:
		raise ConfigError('Lookup parameter not defined!')

	# configure lookup matcher
	name_matcher_default = pconfig.get('', 'default matcher', 'equal')
	name_matcher_raw = pconfig.get(vn_output, 'matcher', name_matcher_default)
	name_matcher_list = name_matcher_raw.lower().splitlines()
	if len(name_matcher_list) == 1: # single matcher given - extend to same length as lookup_list
		name_matcher_list = name_matcher_list * len(vn_lookup_list)
	elif len(name_matcher_list) != len(vn_lookup_list):
		raise ConfigError('Match-functions (length %d) and match-keys (length %d) do not match!' %
			(len(name_matcher_list), len(vn_lookup_list)))
	matcher_list = []
	for name_matcher in name_matcher_list:
		matcher_list.append(Matcher.createInstance(name_matcher, pconfig, vn_output))

	# configure lookup dictionary
	(lookup_content, lookup_order) = pconfig.getParameter(vn_output)
	if not pconfig.getBool(vn_output, 'empty set', False):
		for k in lookup_content:
			if len(lookup_content[k]) == 0:
				lookup_content[k].append('')
	return (vn_output, vn_lookup_list, matcher_list, (lookup_content, lookup_order))


def createLookupHelper(pconfig, vn_output_list, vn_lookup_list):
	# Return list of (doElevate, PSourceClass, arguments) entries
	if len(vn_output_list) != 1: # multi-lookup handling
		result = []
		for var_name in vn_output_list:
			assert(isinstance(var_name, str))
			result.extend(createLookupHelper(pconfig, [var_name], vn_lookup_list))
		return result
	vn_output = vn_output_list[0]
	assert(isinstance(vn_output, str))

	pvalue = pconfig.getParameter(vn_output.lstrip('!'))
	if isinstance(pvalue, list): # simple parameter source
		if len(pvalue) == 1:
			return [(False, ParameterSource.getClass('ConstParameterSource'), [vn_output, pvalue[0]])]
		else:
			return [(False, ParameterSource.getClass('SimpleParameterSource'), [vn_output, pvalue])]
	elif isinstance(pvalue, tuple) and pvalue[0] == 'format':
		return [(False, ParameterSource.getClass('FormatterParameterSource'), pvalue[1:])]

	lookup_key = None
	if vn_lookup_list: # default lookup key
		lookup_key = KeyParameterSource(*vn_lookup_list)

	# Determine kind of lookup, [3] == lookupDictConfig, [0] == lookupContent
	tmp = parse_lookup_create_args(pconfig, KeyParameterSource(vn_output), lookup_key)
	lookupContent = tmp[3][0]
	lookupLen = lmap(len, lookupContent.values())

	if (min(lookupLen) == 1) and (max(lookupLen) == 1): # simple lookup sufficient for this setup
		return [(False, SimpleLookupParameterSource, list(tmp))]
	# switch needs elevation beyond local scope
	return [(True, SwitchingLookupParameterSource, list(tmp))]
