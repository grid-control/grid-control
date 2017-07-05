# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

import random
from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import WMS
from grid_control.config import ListOrder
from grid_control.utils.parsing import parse_list
from hpfwk import AbstractError
from python_compat import imap, lchain, lfilter, lmap, set, sorted


class FinalBroker(Broker):
	alias_list = ['final']

	def __init__(self, config, name, broker_prefix, **kwargs):
		Broker.__init__(self, config, name, broker_prefix, **kwargs)
		self._finalizer = dict.fromkeys([WMS.SITES, WMS.QUEUES,
			WMS.WN, WMS.ENDPOINT, WMS.STORAGE], _list_union)
		self._finalizer.update(dict.fromkeys([WMS.WALLTIME, WMS.CPUTIME,
			WMS.MEMORY, WMS.CPUS, WMS.DISKSPACE, WMS.ARRAY_SIZE], _req_max))

	def process(self, req_list):
		return list(self._process(req_list))

	def _process(self, req_list):
		tmp = {}
		for (req_type, req_value) in req_list:
			if req_value is not None:
				if req_type in self._finalizer:
					tmp.setdefault(req_type, []).append(req_value)
				else:
					yield (req_type, req_value)
		for (req_type, req_value_list) in tmp.items():
			req_value = self._finalizer[req_type](req_value_list)
			if req_value is not None:
				yield (req_type, req_value)


class SingleBroker(Broker):
	def __init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs):
		Broker.__init__(self, config, name, broker_prefix, **kwargs)
		(self._req_type, self._discovery_fun) = (req_type, discovery_fun)

	def process(self, req_list):
		found_req = False
		req_list = list(req_list)
		for (req_type, req_value) in req_list:
			if (req_type == self._req_type) and (req_value is not None):
				found_req = True
				yield (req_type, self._process(req_list, req_value))
			else:
				yield (req_type, req_value)
		if not found_req:
			yield (self._req_type, self._process_missing(req_list))

	def _process(self, req_list, req_value):  # process existing entries
		raise AbstractError

	def _process_missing(self, req_list):  # called when no matching requirements exist
		return self._process(req_list, self._discovery_fun())


class CoverageBroker(SingleBroker):
	alias_list = ['coverage']

	def __init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs):
		SingleBroker.__init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs)
		self._idx = 0

	def _process(self, req_list, req_value):
		req_value = lfilter(lambda item: not item.startswith('-'), req_value or [])
		if req_value:
			req_value = [sorted(req_value)[self._idx % len(req_value)]]
			self._idx = self._idx + 1
			return req_value


class DiscoveryBroker(SingleBroker):
	alias_list = ['discover']

	def enabled(self):
		return self._discovery_fun() is not None

	def _process(self, req_list, req_value):
		return req_value


class FilterBroker(SingleBroker):
	alias_list = ['filter']

	def __init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs):
		SingleBroker.__init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs)
		self._user_filter = config.get_filter(broker_prefix, '',
			default_matcher='BlackWhiteMatcher', default_filter='StrictListFilter',
			default_order=ListOrder.source)

	def __repr__(self):
		return self._repr_base(repr(self._user_filter))

	def enabled(self):
		return self._user_filter.get_selector()

	def _process(self, req_list, req_value):
		return self._user_filter.filter_list(req_value)

	def _process_missing(self, req_list):
		return self._user_filter.get_selector() or None


class LimitBroker(SingleBroker):
	alias_list = ['limit']

	def __init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs):
		SingleBroker.__init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs)
		self._num_entries = config.get_int('%s entries' % broker_prefix, 1, on_change=None)

	def __repr__(self):
		return self._repr_base('limit=%d' % self._num_entries)

	def _process(self, req_list, req_value):
		if self._num_entries > 0:
			req_value = req_value[:self._num_entries]
		return req_value


class RandomBroker(SingleBroker):
	alias_list = ['random']

	def __init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs):
		SingleBroker.__init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs)
		self._randomize = config.get_bool('%s randomize' % broker_prefix, False, on_change=None)

	def enabled(self):
		return self._randomize

	def _process(self, req_list, req_value):
		req_value = list(req_value)
		random.shuffle(req_value)
		return req_value


class SimpleBroker(SingleBroker):
	alias_list = ['simple']

	def __init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs):
		SingleBroker.__init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs)
		self._item_dict = discovery_fun()

		if self._item_dict is not None:  # Sort discovered items according to requirements
			item_prop_type_list = self._get_common_item_prop_type_map()
			none_value = {float: 1e10, str: chr(127), tuple: tuple()}

			def _key_fun(item_prop_tuple):
				item_prop_key, item_prop_value = item_prop_tuple

				def _enforce_type(key):
					value = item_prop_value.get(key)
					if value is None:
						return none_value[item_prop_type_list[key]]
					return item_prop_type_list[key](value)
				return (tuple(imap(_enforce_type, sorted(item_prop_type_list))), item_prop_key)
			self._item_list_sorted = lmap(lambda k_v: k_v[0], sorted(self._item_dict.items(), key=_key_fun))

	def process(self, req_list):
		if not self._item_dict:
			return req_list
		return SingleBroker.process(self, req_list)

	def _get_common_item_prop_type_map(self):  # find conversion methods to access properties uniformly
		mapped_item_prop_type_dict = {int: float, float: float, str: str, list: tuple, tuple: tuple}
		item_prop_type_dict = {}
		for item_prop_dict in self._item_dict.values():
			for item_prop_key, item_prop_value in item_prop_dict.items():
				for (item_prop_type, mapped_item_prop_type) in mapped_item_prop_type_dict.items():
					if isinstance(item_prop_value, item_prop_type):
						item_prop_type_dict.setdefault(item_prop_key, []).append(mapped_item_prop_type)
						break
		for item_prop_key, item_prop_type_list in list(item_prop_type_dict.items()):
			item_prop_type_dict[item_prop_key] = item_prop_type_list[0]
		return item_prop_type_dict

	def _process(self, req_list, req_value):
		# Match items which fulfill the requirements
		def _matcher(props):
			for req_type, req_value in req_list:
				if props.get(req_type) is None:
					continue
				if req_value >= props[req_type]:
					return False
			return True
		# Apply sort order and give matching entries as preselection to FilterBroker
		return lfilter(lambda x: _matcher(self._item_dict[x]), self._item_list_sorted)


class StorageBroker(SingleBroker):
	alias_list = ['storage']

	def __init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs):
		SingleBroker.__init__(self, config, name, broker_prefix, req_type, discovery_fun, **kwargs)
		self._storage_lookup = config.get_lookup('%s storage access' % broker_prefix, {}, on_change=None,
			parser=lambda x: parse_list(x, ' '), strfun=lambda x: str.join(' ', x))

	def process(self, req_list):
		matched_list = []
		for (req_type, req_value) in req_list:
			if (req_type == WMS.STORAGE) and req_value:
				for req_item in req_value:
					matched_list.extend(self._storage_lookup.lookup(req_item))
			yield (req_type, req_value)
		if matched_list:
			yield (self._req_type, matched_list)


def _list_union(*args):
	return sorted(set(lchain(*args)))


def _req_max(args):
	result = max(args)
	if result >= 0:
		return result
