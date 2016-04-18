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

from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import WMS
from grid_control.config import ListOrder
from grid_control.utils.parsing import parseList
from python_compat import imap, lfilter, lmap, sorted

class RandomBroker(Broker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		Broker.__init__(self, config, name, userOpt, itemName, discoverFun)
		self._itemsStart = self._discover(discoverFun)


class UserBroker(Broker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		Broker.__init__(self, config, name, userOpt, itemName, discoverFun)
		self._itemsStart = config.getList(userOpt, [], onChange = None)
		if not self._itemsStart:
			self._itemsStart = None


class FilterBroker(Broker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		Broker.__init__(self, config, name, userOpt, itemName, discoverFun)
		userFilter = config.getFilter(userOpt, '',
			defaultMatcher = 'blackwhite', defaultFilter = 'try_strict',
			defaultOrder = ListOrder.matcher)
		self._itemsStart = userFilter.getSelector()
		if self._itemsStart:
			self._discover(discoverFun)
		if self._itemsDiscovered:
			self._itemsStart = userFilter.filterList(self._itemsDiscovered)


class CoverageBroker(Broker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		Broker.__init__(self, config, name, userOpt, itemName, discoverFun)
		userFilter = config.getFilter(userOpt, '',
			defaultMatcher = 'blackwhite', defaultFilter = 'try_strict',
			defaultOrder = ListOrder.matcher)
		self._itemsStart = userFilter.filterList(None)
		itemsDiscover = list(self._discover(discoverFun).keys())
		if itemsDiscover:
			self._itemsStart = userFilter.filterList(itemsDiscover)
		self._nIndex = 0

	def _broker(self, reqs, items):
		items = Broker._broker(self, reqs, items)
		if items:
			items = [sorted(items)[self._nIndex % len(items)]]
			self._nIndex += 1
		return items


class SimpleBroker(FilterBroker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		FilterBroker.__init__(self, config, name, userOpt, itemName, discoverFun)
		self._discover(discoverFun)

		if self._itemsDiscovered: # Sort discovered items according to requirements
			itemPropTypes = self._getPropTypes()
			none_value = {float: 1e10, str: chr(127), tuple: tuple()}
			def keyFun(x):
				def enforce_type(key):
					value = x[1].get(key)
					if value is None:
						return none_value[itemPropTypes[key]]
					return itemPropTypes[key](value)
				return (tuple(imap(enforce_type, sorted(itemPropTypes))), x[0])
			self._itemsSorted = lmap(lambda k_v: k_v[0], sorted(self._itemsDiscovered.items(), key = keyFun))

	def _getPropTypes(self): # find conversion method to access properties uniformly
		itemPropTypesMap = {int: float, float: float, str: str, list: tuple, tuple: tuple}
		itemPropTypes = {}
		for itemPropDict in self._itemsDiscovered.values():
			for itemPropKey, itemPropValue in itemPropDict.items():
				for (iptype, ipmapped) in itemPropTypesMap.items():
					if isinstance(itemPropValue, iptype):
						itemPropTypes.setdefault(itemPropKey, []).append(ipmapped)
						break
		for itemPropKey, itemPropTypeList in list(itemPropTypes.items()):
			itemPropTypes[itemPropKey] = itemPropTypeList[0]
		return itemPropTypes

	def _broker(self, reqs, items):
		if not self._itemsDiscovered:
			return FilterBroker._broker(self, reqs, self._itemsStart) # Use user constrained items

		# Match items which fulfill the requirements
		def matcher(props):
			for key, value in reqs:
				if key not in props or (props[key] is None):
					continue
				if not (value < props[key]):
					return False
			return True
		# Apply sort order and give matching entries as preselection to FilterBroker
		items = lfilter(lambda x: matcher(self._itemsDiscovered[x]), self._itemsStart or self._itemsSorted)
		return FilterBroker._broker(self, reqs, lfilter(lambda x: x in items, self._itemsSorted))


class StorageBroker(Broker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		Broker.__init__(self, config, name, userOpt, itemName, discoverFun)
		self._storageDict = config.getLookup('%s storage access' % userOpt, {}, onChange = None,
			parser = lambda x: parseList(x, ' '), strfun = lambda x: str.join(' ', x))

	def _broker(self, reqs, items):
		result = Broker._broker(self, reqs, items)
		for (rType, rValue) in reqs:
			if (rType == WMS.STORAGE) and rValue:
				if result is None:
					result = []
				for rval in rValue:
					result.extend(self._storageDict.lookup(rval))
		return result
