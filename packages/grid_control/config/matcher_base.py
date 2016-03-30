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
from grid_control.config.config_entry import appendOption, noDefault
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils import QM
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.parsing import strDict
from hpfwk import AbstractError, Plugin
from python_compat import lfilter, sorted

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
class Matcher(ConfigurablePlugin):
	def __init__(self, config, option_prefix):
		pass

	def getPositive(self, selector):
		raise AbstractError

	def parseSelector(self, selector):
		return [selector]

	def matcher(self, value, selector):
		raise AbstractError

	def matchWith(self, selector):
		matcher = self.matcher
		class FunctionObject(MatcherHolder):
			def match(self, value):
				return matcher(value, self._selector)
		return getFixedFunctionObject(self, FunctionObject, selector)

	def __repr__(self):
		return '%s(...)' % self.__class__.__name__

class BasicMatcher(Matcher):
	def matchFunction(value, selector):
		raise AbstractError
	matchFunction = staticmethod(matchFunction)

	def getPositive(self, selector):
		return [selector]

	def matcher(self, value, selector):
		return QM(self.__class__.matchFunction(value, selector), 1, -1)

	def matchWith(self, selector):
		matcher = self.__class__.matchFunction
		class FunctionObject(MatcherHolder):
			def match(self, value):
				return QM(matcher(value, self._selector), 1, -1)
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


class ExprMatcher(Matcher):
	alias = ['expr', 'eval']

	def getExpr(selector):
		return eval('lambda value: (%s) == True' % selector) # pylint:disable=eval-used
	getExpr = staticmethod(getExpr)

	def getPositive(self, selector):
		return None

	def matcher(self, value, selector):
		return QM(ExprMatcher.getExpr(selector)(value), 1, -1)

	def matchWith(self, selector):
		class FunctionObject(MatcherHolder):
			def init(self, fixedSelector):
				self._matcher = ExprMatcher.getExpr(fixedSelector)
			def match(self, value):
				return QM(self._matcher(value), 1, -1)
		return getFixedFunctionObject(self, FunctionObject, selector)


class RegExMatcher(Matcher):
	alias = ['regex']

	def getPositive(self, selector):
		return None

	def matcher(self, value, selector):
		return QM(re.search(selector, value) is not None, 1, -1)

	def matchWith(self, selector):
		class FunctionObject(MatcherHolder):
			def init(self, fixedSelector):
				self._regex = re.compile(fixedSelector)
			def match(self, value):
				return QM(self._regex.search(value) is not None, 1, -1)
		return getFixedFunctionObject(self, FunctionObject, selector)


class BlackWhiteMatcher(Matcher):
	alias = ['blackwhite']

	def __init__(self, config, option_prefix):
		Matcher.__init__(self, config, option_prefix)
		self._baseMatcher = config.getPlugin(appendOption(option_prefix, 'mode'), 'start',
			cls = Matcher, pargs = (option_prefix,))

	def getPositive(self, selector):
		return lfilter(lambda p: not p.startswith('-'), selector.split())

	def parseSelector(self, selector):
		return selector.split()

	def matcher(self, value, selector):
		result = 0
		for idx, subselector in enumerate(selector.split()):
			if subselector.startswith('-') and (self._baseMatcher.matcher(value, subselector[1:]) > 0):
				result = -(idx + 1)
			elif self._baseMatcher.matcher(value, subselector) > 0:
				result = idx + 1
		return result


ListOrder = makeEnum(['source', 'matcher'])

class ListFilter(Plugin):
	def __init__(self, selector, matcher, order):
		(self._matchFunction, self._positive, self._selector, self._order) = (None, None, None, order)
		if selector:
			self._selector = matcher.parseSelector(selector)
			self._matchFunction = matcher.matchWith(selector)
			self._positive = matcher.getPositive(selector)

	def getSelector(self):
		return self._selector

	def filterList(self, entries):
		if entries is None:
			return self._positive
		if not self._matchFunction:
			return entries
		if self._order == ListOrder.matcher:
			entries = sorted(entries, key = self._matchFunction.match)
		return self._filterListImpl(entries)

	def _filterListImpl(self, entries):
		raise AbstractError

	def __repr__(self):
		return '%s(matcher = %r, positive = %r, order = %r)' % (self.__class__.__name__,
			self._matchFunction, self._positive, ListOrder.enum2str(self._order))


class StrictListFilter(ListFilter):
	alias = ['strict', 'require']

	def _filterListImpl(self, entries):
		return lfilter(lambda entry: self._matchFunction.match(entry) > 0, entries)


class MediumListFilter(ListFilter):
	alias = ['try_strict']

	def _filterListImpl(self, entries):
		strict_result = lfilter(lambda entry: self._matchFunction.match(entry) > 0, entries)
		if strict_result:
			return strict_result
		return lfilter(lambda entry: self._matchFunction.match(entry) >= 0, entries)


class WeakListFilter(ListFilter):
	alias = ['weak', 'prefer']

	def _filterListImpl(self, entries):
		return lfilter(lambda entry: self._matchFunction.match(entry) >= 0, entries)


class DictLookup(Plugin):
	def __init__(self, values, order, matcher, only_first, always_default):
		(self._values, self._only_first, self._always_default) = (values, only_first, always_default)
		(self._matcher, self._order) = (matcher, order)

	def empty(self):
		return not self._values

	def __repr__(self):
		return '%s(values = {%s}, matcher = %r, only_first = %r, always_default = %r)' % (
			self.__class__.__name__, strDict(self._values, self._order), self._matcher,
			self._only_first, self._always_default)

	def _lookup(self, value):
		for key in self._order:
			if self._matcher.matcher(key, value) > 0:
				yield self._values[key]

	def lookup(self, value, default = noDefault):
		result = list(self._lookup(value))
		if (None in self._values) and (self._always_default or not result):
			result.append(self._values[None])
		if (default != noDefault) and not result:
			result.append(default)
		if not self._only_first:
			return result
		elif result:
			return result[0]
