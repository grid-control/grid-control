#-#  Copyright 2016 Karlsruhe Institute of Technology
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
from grid_control.config.cinterface_typed import appendOption
from grid_control.gc_plugin import ConfigurablePlugin
from hpfwk import AbstractError, Plugin
from python_compat import imap, lfilter

class MatcherHolder(object):
	def __init__(self, selector):
		self._selector = selector
		self.init(selector)

	def init(self, fixedSelector):
		pass

	def match(self, value):
		raise AbstractError

	def __repr__(self):
		return self.__class__.__name__ + '(%s)' % repr(self._selector)


def getFixedFunctionObject(instance, fo, selector):
	fo.__name__ = instance.__class__.__name__ + '_FixedSelector'
	return fo(selector)


# Matcher class
class MatcherBase(ConfigurablePlugin):
	def __init__(self, config, option_prefix):
		pass

	def matcher(self, value, selector):
		raise AbstractError

	def matchWith(self, selector):
		matcher = self.matcher
		class FunctionObject(MatcherHolder):
			def match(self, value):
				return matcher(value, self._selector)
		return getFixedFunctionObject(self, FunctionObject, selector)


class BasicMatcher(MatcherBase):
	matcher = None

	def __init__(self, config, option_prefix):
		MatcherBase.__init__(self, config, option_prefix)
		self.matcher = self.__class__.matcher


class StartMatcher(BasicMatcher):
	alias = ['start']
	matcher = str.startswith


class EndMatcher(BasicMatcher):
	alias = ['end']
	matcher = str.endswith


class EqualMatcher(BasicMatcher):
	alias = ['equal']
	matcher = str.__eq__


class ExprMatcher(MatcherBase):
	alias = ['expr', 'eval']

	def _getExpr(selector):
		return eval('lambda value: %s' % selector)
	_getExpr = staticmethod(_getExpr)

	def matcher(self, value, selector):
		return self._getExpr(selector)(value)

	def matchWith(self, selector):
		class FunctionObject(MatcherHolder):
			def init(self, fixedSelector):
				self.match = self._getExpr(fixedSelector)
		return getFixedFunctionObject(self, FunctionObject, selector)


class RegExMatcher(MatcherBase):
	alias = ['regex']

	def matcher(self, value, selector):
		return re.search(selector, value) is not None

	def matchWith(self, selector):
		class FunctionObject(MatcherHolder):
			def init(self, fixedSelector):
				self._regex = re.compile(fixedSelector)
			def match(self, value):
				return self._regex.search(value) is not None
		return getFixedFunctionObject(self, FunctionObject, selector)


class BlackWhiteMatcher(MatcherBase):
	alias = ['blackwhite']

	def __init__(self, config, option_prefix):
		MatcherBase.__init__(self, config, option_prefix)
		baseMatchOpt = appendOption(option_prefix, 'matcher')
		self._baseMatcher = config.getPlugin(baseMatchOpt, 'start',
			cls = MatcherBase, pargs = (option_prefix,))

	def matcher(self, value, selector):
		return self.matchWith(selector).match(value)

	def selectorToMatchFunList(self, selector):
		for subselector in selector.split():
			if subselector.startswith('-'):
				yield lambda value: not self._baseMatcher.matchWith(subselector[1:]).match(value)
			else:
				yield self._baseMatcher.matchWith(subselector).match

	def matchWith(self, selector):
		s2ml = self.selectorToMatchFunList
		class FunctionObject(MatcherHolder):
			def init(self, fixedSelector):
				self._pat_ret = list(s2ml(fixedSelector))
			def match(self, value):
				result = None
				for matchResult in imap(lambda matchFun: matchFun(value), self._pat_ret):
					if matchResult is not None:
						result = matchResult
				return result
		return getFixedFunctionObject(self, FunctionObject, selector)


class AdvancedBlackWhiteMatcher(BlackWhiteMatcher):
	alias = ['blackwhite2']

	def matcher(self, value, selector):
		result = None
		for subselector in selector.split():
			if subselector.startswith('-'):
				if self._baseMatcher.matcher(value, subselector[1:]):
					result = False
			elif self._baseMatcher.matcher(value, subselector):
				result = True
		return result


class ListFilterBase(Plugin):
	def __init__(self, selector, matcher):
		(self._matchFun, self._positive) = (None, None)
		if selector:
			self._matchFun = matcher.matchWith(selector)

	def filterList(self, entries):
		raise AbstractError


class StrictListFilter(Plugin):
	alias = ['strict', 'require']

	def filterList(self, entries):
		if entries is None:
			return self._positive
		if self._matchFun:
			return lfilter(self._matchFun.match, entries)
		return entries


class MediumListFilter(Plugin):
	alias = ['try_strict']

	def filterList(self, entries):
		if entries is None:
			return self._positive
		strict_result = lfilter(self._matchFun.match, entries)
		if strict_result:
			return strict_result
		return lfilter(lambda entry: self._matchFun.match(entry) != False, entries)


class WeakListFilter(Plugin):
	alias = ['weak', 'prefer']

	def filterList(self, entries):
		if entries is None:
			return self._positive
		return lfilter(lambda entry: self._matchFun.match(entry) != False, entries)
