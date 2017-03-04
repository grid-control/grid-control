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

from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import WMS
from grid_control.config import ListOrder
from grid_control.utils.parsing import parse_list
from python_compat import imap, lfilter, lmap, sorted


class CoverageBroker(Broker):
	def __init__(self, config, name, broker_prefix, item_name, discover_fun):
		Broker.__init__(self, config, name, broker_prefix, item_name, discover_fun)
		user_filter = config.get_filter(broker_prefix, '',
			default_matcher='blackwhite', default_filter='try_strict',
			default_order=ListOrder.matcher)
		self._item_list_start = user_filter.filter_list(None)
		item_list_discovered = list(self._discover(discover_fun).keys())
		if item_list_discovered:
			self._item_list_start = user_filter.filter_list(item_list_discovered)
		self._item_idx = 0

	def _broker(self, reqs, items):
		items = Broker._broker(self, reqs, items)
		if items:
			items = [sorted(items)[self._item_idx % len(items)]]
			self._item_idx += 1
		return items


class FilterBroker(Broker):
	def __init__(self, config, name, broker_prefix, item_name, discover_fun):
		Broker.__init__(self, config, name, broker_prefix, item_name, discover_fun)
		user_filter = config.get_filter(broker_prefix, '',
			default_matcher='blackwhite', default_filter='try_strict',
			default_order=ListOrder.matcher)
		self._item_list_start = user_filter.get_selector()
		if self._item_list_start:
			self._discover(discover_fun)
		if self._item_list_discovered:
			self._item_list_start = user_filter.filter_list(self._item_list_discovered)


class RandomBroker(Broker):
	def __init__(self, config, name, broker_prefix, item_name, discover_fun):
		Broker.__init__(self, config, name, broker_prefix, item_name, discover_fun)
		self._item_list_start = self._discover(discover_fun)


class StorageBroker(Broker):
	def __init__(self, config, name, broker_prefix, item_name, discover_fun):
		Broker.__init__(self, config, name, broker_prefix, item_name, discover_fun)
		self._storage_lookup = config.get_lookup('%s storage access' % broker_prefix, {}, on_change=None,
			parser=lambda x: parse_list(x, ' '), strfun=lambda x: str.join(' ', x))

	def _broker(self, reqs, items):
		result = Broker._broker(self, reqs, items)
		for (req_enum, req_value) in reqs:
			if (req_enum == WMS.STORAGE) and req_value:
				if result is None:
					result = []
				for rval in req_value:
					result.extend(self._storage_lookup.lookup(rval))
		return result


class UserBroker(Broker):
	def __init__(self, config, name, broker_prefix, item_name, discover_fun):
		Broker.__init__(self, config, name, broker_prefix, item_name, discover_fun)
		self._item_list_start = config.get_list(broker_prefix, [], on_change=None)
		if not self._item_list_start:
			self._item_list_start = None


class SimpleBroker(FilterBroker):
	def __init__(self, config, name, broker_prefix, item_name, discover_fun):
		FilterBroker.__init__(self, config, name, broker_prefix, item_name, discover_fun)
		self._discover(discover_fun)

		if self._item_list_discovered:  # Sort discovered items according to requirements
			item_prop_type_list = self._get_common_item_prop_type_map()
			none_value = {float: 1e10, str: chr(127), tuple: tuple()}

			def _key_fun(item_prop_tuple):
				item_prop_key, item_prop_value = item_prop_tuple

				def enforce_type(key):
					value = item_prop_value.get(key)
					if value is None:
						return none_value[item_prop_type_list[key]]
					return item_prop_type_list[key](value)
				return (tuple(imap(enforce_type, sorted(item_prop_type_list))), item_prop_key)
			self._item_list_sorted = lmap(lambda k_v: k_v[0],
				sorted(self._item_list_discovered.items(), key=_key_fun))

	def _broker(self, reqs, items):
		if not self._item_list_discovered:
			return FilterBroker._broker(self, reqs, self._item_list_start)  # Use user constrained items

		# Match items which fulfill the requirements
		def _matcher(props):
			for key, value in reqs:
				if props.get(key) is None:
					continue
				if value >= props[key]:
					return False
			return True
		# Apply sort order and give matching entries as preselection to FilterBroker
		items = lfilter(lambda x: _matcher(self._item_list_discovered[x]),
			self._item_list_start or self._item_list_sorted)
		return FilterBroker._broker(self, reqs, lfilter(items.__contains__, self._item_list_sorted))

	def _get_common_item_prop_type_map(self):  # find conversion method to access properties uniformly
		mapped_item_prop_type_dict = {int: float, float: float, str: str, list: tuple, tuple: tuple}
		item_prop_type_dict = {}
		for item_prop_dict in self._item_list_discovered.values():
			for item_prop_key, item_prop_value in item_prop_dict.items():
				for (item_prop_type, mapped_item_prop_type) in mapped_item_prop_type_dict.items():
					if isinstance(item_prop_value, item_prop_type):
						item_prop_type_dict.setdefault(item_prop_key, []).append(mapped_item_prop_type)
						break
		for item_prop_key, item_prop_type_list in list(item_prop_type_dict.items()):
			item_prop_type_dict[item_prop_key] = item_prop_type_list[0]
		return item_prop_type_dict
