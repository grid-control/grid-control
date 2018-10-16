# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

import re, fnmatch, logging
from grid_control.config.config_entry import join_config_locations
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.data_structures import make_enum
from grid_control.utils.parsing import str_dict_linear
from hpfwk import AbstractError, Plugin
from python_compat import lfilter, sorted, unspecified


ListOrder = make_enum(['source', 'matcher'])  # pylint: disable=invalid-name


class MatcherHolder(object):
	def __init__(self, selector, case):
		self._case = case
		self._selector = selector
		self._assign_selector(selector)

	def __repr__(self):
		if self._case:
			return self.__class__.__name__ + '(%s)' % repr(self._selector)
		return self.__class__.__name__ + '_ci(%s)' % repr(self._selector)

	def match(self, value):
		raise AbstractError

	def _assign_selector(self, selector_fixed):
		pass


class Matcher(ConfigurablePlugin):
	def __init__(self, config, option_prefix, case_override=None, **kwargs):
		ConfigurablePlugin.__init__(self, config)
		self._case = case_override
		if case_override is None:
			self._case = config.get_bool(join_config_locations(option_prefix, 'case sensitive'),
				default=True, **kwargs)
		self._log = logging.getLogger('matcher.%s.%s' % (self.__class__.__name__,
			option_prefix[0].replace(' ', '_')))
		if not self._log.isEnabledFor(logging.DEBUG1):
			self._log = None

	def __repr__(self):
		return '%s(case sensitive = %r)' % (self.__class__.__name__, self._case)

	def create_matcher(self, selector):
		matcher = self.matcher

		class FunctionObject(MatcherHolder):
			def match(self, value):
				return matcher(value, _get_case(self._case, self._selector))
		return _get_fixed_matcher_object(self, FunctionObject, selector, self._case)

	def get_positive_selector(self, selector):
		raise AbstractError

	def matcher(self, value, selector):
		raise AbstractError

	def parse_selector(self, selector):
		return [selector]


class DictLookup(Plugin):
	def __init__(self, values, order, matcher, only_first, always_default):
		(self._values, self._only_first, self._always_default) = (values, only_first, always_default)
		(self._matcher, self._order) = (matcher, order)

	def __repr__(self):
		return '%s(values = {%s}, matcher = %r, only_first = %r, always_default = %r)' % (
			self.__class__.__name__, str_dict_linear(self._values, self._order), self._matcher,
			self._only_first, self._always_default)

	def empty(self):
		return not self._values

	def get_values(self):
		return self._values.values()

	def lookup(self, value, default=unspecified, is_selector=True):
		result = list(self._lookup(value, is_selector))
		if (None in self._values) and (self._always_default or not result):
			result.append(self._values[None])
		if not (result or unspecified(default)):
			result.append(default)
		if not self._only_first:
			return result
		elif result:
			return result[0]

	def _lookup(self, value, is_selector):
		if value is not None:
			for key in self._order:
				if is_selector:
					match = self._matcher.matcher(key, value)
				else:
					match = self._matcher.matcher(value, key)
				if match > 0:
					yield self._values[key]


class ListFilter(Plugin):
	def __init__(self, selector, matcher, order, negate):
		(self._matcher, self._match_function) = (matcher, None)
		(self._positive, self._selector, self._order, self._negate) = (None, None, order, negate)
		if selector:
			self._selector = matcher.parse_selector(selector)
			self._positive = matcher.get_positive_selector(selector)
			match_obj = matcher.create_matcher(selector)
			if negate:
				def _match_fun(item):
					return -match_obj.match(item)
				self._match_function = _match_fun
			else:
				self._match_function = match_obj.match

	def __repr__(self):
		return '%s(matcher = %r, positive = %r, order = %r, negate = %r)' % (self.__class__.__name__,
			self._matcher, self._positive, ListOrder.enum2str(self._order), self._negate)

	def filter_list(self, entries, key=None):
		if entries is None:
			return self._positive
		if not self._match_function:
			return entries
		if key is None:
			_match_function = self._match_function
		else:
			def _match_function(item):
				return self._match_function(key(item))
		if self._order == ListOrder.matcher:
			entries = sorted(entries, key=_match_function)
		return self._filter_list(entries, _match_function)

	def get_selector(self):
		return self._selector

	def _filter_list(self, entries, match_function):
		raise AbstractError


class AlwaysMatcher(Matcher):
	alias_list = ['always']

	def create_matcher(self, selector):
		class AlwaysSelector(MatcherHolder):
			def match(self, value):
				return 1
		return AlwaysSelector('', '')

	def get_positive_selector(self, selector):
		return [selector]

	def matcher(self, value, selector):
		return 1


class BaseMatcher(Matcher):
	def create_matcher(self, selector):
		matcher = self.__class__.match_function

		class FunctionObject(MatcherHolder):
			def match(self, value):
				return _match_result(matcher(_get_case(self._case, value), self._selector))
		return _get_fixed_matcher_object_case(self, FunctionObject, selector, self._case)

	def get_positive_selector(self, selector):
		return [selector]

	def match_function(value, selector):
		raise AbstractError
	match_function = staticmethod(match_function)

	def matcher(self, value, selector):
		return _match_result(self.__class__.match_function(
			_get_case(self._case, value), _get_case(self._case, selector)))


class BlackWhiteMatcher(Matcher):
	alias_list = ['blackwhite']

	def __init__(self, config, option_prefix, case_override=None, **kwargs):
		Matcher.__init__(self, config, option_prefix, case_override, **kwargs)
		self._base_matcher = config.get_plugin(join_config_locations(option_prefix, 'mode'), 'start',
			cls=Matcher, pargs=(option_prefix, self._case), pkwargs=kwargs, **kwargs)

	def __repr__(self):
		return '%s(base matcher = %r)' % (self.__class__.__name__, self._base_matcher)

	def get_positive_selector(self, selector):
		return lfilter(lambda p: not p.startswith('-'), selector.split())

	def matcher(self, value, selector):
		result = 0
		for idx, subselector in enumerate(selector.split()):
			if subselector.startswith('-') and (self._base_matcher.matcher(value, subselector[1:]) > 0):
				result = -(idx + 1)
			elif self._base_matcher.matcher(value, subselector) > 0:
				result = idx + 1
			if self._log:
				self._log.log(logging.DEBUG1, 'matching value %s against selector %s (%s): %s',
					value, subselector, self._base_matcher, result)
		return result

	def parse_selector(self, selector):
		return selector.split()


class ExprMatcher(Matcher):
	alias_list = ['expr', 'eval']

	def create_matcher(self, selector):
		class FunctionObject(MatcherHolder):
			def match(self, value):
				return _match_result(self._matcher(_get_case(self._case, value)))

			def _assign_selector(self, selector_fixed):
				self._matcher = _get_fun_from_expr(selector_fixed)
		return _get_fixed_matcher_object_case(self, FunctionObject, selector, self._case)

	def get_positive_selector(self, selector):
		return None

	def matcher(self, value, selector):
		match_fun = _get_fun_from_expr(_get_case(self._case, selector))
		return _match_result(match_fun(_get_case(self._case, value)))


class RegExMatcher(Matcher):
	alias_list = ['regex']

	def __init__(self, config, option_prefix, case_override=None, **kwargs):
		Matcher.__init__(self, config, option_prefix, case_override=None, **kwargs)
		self._case_regex = self._case

	def create_matcher(self, selector):
		class FunctionObject(MatcherHolder):
			def match(self, value):
				return _match_result(self._regex.search(_get_case(self._case, value)) is not None)

			def _assign_selector(self, selector_fixed):
				self._regex = re.compile(selector_fixed)
		selector_case = _get_case(self._case_regex, selector)
		return _get_fixed_matcher_object(self, FunctionObject, selector_case, self._case)

	def get_positive_selector(self, selector):
		return None

	def matcher(self, value, selector):
		re_match = re.search(_get_case(self._case_regex, selector), _get_case(self._case, value))
		return _match_result(re_match is not None)


class MediumListFilter(ListFilter):
	alias_list = ['try_strict']

	def _filter_list(self, entries, match_function):
		strict_result = lfilter(lambda entry: match_function(entry) > 0, entries)
		if strict_result:
			return strict_result
		return lfilter(lambda entry: match_function(entry) >= 0, entries)


class StrictListFilter(ListFilter):
	alias_list = ['strict', 'require']

	def _filter_list(self, entries, match_function):
		return lfilter(lambda entry: match_function(entry) > 0, entries)


class WeakListFilter(ListFilter):
	alias_list = ['weak', 'prefer']

	def _filter_list(self, entries, match_function):
		return lfilter(lambda entry: match_function(entry) >= 0, entries)


class EndMatcher(BaseMatcher):
	alias_list = ['end']
	match_function = str.endswith


class EqualMatcher(BaseMatcher):
	alias_list = ['equal']
	match_function = str.__eq__


class ShellStyleMatcher(BaseMatcher):
	alias_list = ['shell']
	match_function = staticmethod(fnmatch.fnmatch)


class StartMatcher(BaseMatcher):
	alias_list = ['start']
	match_function = str.startswith


def _get_case(case, value):
	if not case:
		return value.lower()
	return value


def _get_fixed_matcher_object(instance, matcher_class, selector, case):
	matcher_class.__name__ = instance.__class__.__name__ + '_FixedSelector'
	return matcher_class(selector, case)


def _get_fixed_matcher_object_case(instance, matcher_class, selector, case):
	selector_case = _get_case(case, selector)
	return _get_fixed_matcher_object(instance, matcher_class, selector_case, case)


def _get_fun_from_expr(selector):
	return eval('lambda value: (%s) == True' % selector)  # pylint:disable=eval-used


def _match_result(value):
	if value:
		return 1
	return -1
