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
from grid_control.config.config_entry import appendOption
from grid_control.gc_plugin import ConfigurablePlugin
from hpfwk import AbstractError, Plugin
from python_compat import lfilter

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
	matchFunction = None

	def matcher(self, value, selector):
		return self.__class__.matchFunction(value, selector)

	def matchWith(self, selector):
		matcher = self.__class__.matchFunction
		class FunctionObject(MatcherHolder):
			def match(self, value):
				return matcher(value, self._selector)
		return getFixedFunctionObject(self, FunctionObject, selector)


class StartMatcher(BasicMatcher):
	alias = ['start']
	matchFunction = str.startswith


class EndMatcher(BasicMatcher):
	alias = ['end']
	matchFunction = str.endswith


class EqualMatcher(BasicMatcher):
	alias = ['equal']
	matchFunction = str.__eq__


class ExprMatcher(MatcherBase):
	alias = ['expr', 'eval']

	def getExpr(selector):
		return eval('lambda value: (%s) == True' % selector)
	getExpr = staticmethod(getExpr)

	def matcher(self, value, selector):
		return ExprMatcher.getExpr(selector)(value)

	def matchWith(self, selector):
		class FunctionObject(MatcherHolder):
			def init(self, fixedSelector):
				self.match = ExprMatcher.getExpr(fixedSelector)
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
		result = None
		for subselector in selector.split():
			if subselector.startswith('-') and self._baseMatcher.matcher(value, subselector[1:]):
				result = False
			elif self._baseMatcher.matcher(value, subselector):
				result = True
		return result


class ListFilterBase(Plugin):
	def __init__(self, selector, matcher):
		(self._matchFunction, self._positive) = (None, None)
		if selector:
			self._matchFunction = matcher.matchWith(selector)

	def filterList(self, entries):
		raise AbstractError


class StrictListFilter(ListFilterBase):
	alias = ['strict', 'require']

	def filterList(self, entries):
		if entries is None:
			return self._positive
		if self._matchFunction:
			return lfilter(self._matchFunction.match, entries)
		return entries


class MediumListFilter(ListFilterBase):
	alias = ['try_strict']

	def filterList(self, entries):
		if entries is None:
			return self._positive
		strict_result = lfilter(self._matchFunction.match, entries)
		if strict_result:
			return strict_result
		return lfilter(lambda entry: self._matchFunction.match(entry) != False, entries)


class WeakListFilter(ListFilterBase):
	alias = ['weak', 'prefer']

	def filterList(self, entries):
		if entries is None:
			return self._positive
		return lfilter(lambda entry: self._matchFunction.match(entry) != False, entries)


class DictLookup(Plugin):
	def __init__(self, values, order, matcher, only_first = True, always_default = False):
		(self._values, self._order, self._matcher) = (values, order, matcher)
		(self._only_first, self._always_default) = (only_first, always_default)

	def _lookup(self, key):
		for entry in self._order:
			if self._matcher.matcher(entry, key):
				yield self._values[entry]

	def lookup(self, key):
		result = list(self._lookup(key))
		if (None in self._values) and (self._always_default or not result):
			result.append(self._values[None])
		if not self._only_first:
			return result
		elif result:
			return result[0]
