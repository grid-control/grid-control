#-#  Copyright 2012-2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.backends.broker import Broker
from grid_control.backends.wms import WMS
from grid_control.utils.gc_itertools import ichain
from grid_control.utils.parsing import parseList
from python_compat import imap, lfilter, lmap, set, sorted

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


class FilterBroker(UserBroker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		UserBroker.__init__(self, config, name, userOpt, itemName, discoverFun)
		if self._itemsStart:
			self._discover(discoverFun)
		if self._itemsDiscovered:
			self._itemsStart = utils.filterBlackWhite(self._itemsDiscovered, self._itemsStart, addUnmatched = True)


class CoverageBroker(Broker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		Broker.__init__(self, config, name, userOpt, itemName, discoverFun)
		itemsUser = config.getList(userOpt, [], onChange = None)
		if not itemsUser:
			itemsUser = None
		itemsDisc = self._discover(discoverFun).keys()
		self._itemsStart = itemsDisc
		if itemsDisc and itemsUser:
			self._itemsStart = utils.filterBlackWhite(itemsDisc, itemsUser)
		elif not itemsDisc:
			self._itemsStart = utils.filterBlackWhite(itemsUser, itemsUser)
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
			allItemPropKeys = sorted(set(ichain(self._itemsDiscovered.values())))
			itemPropTypes = {}
			for itemPropDict in self._itemsDiscovered.values():
				for itemPropKey, itemPropValue in itemPropDict.items():
					itemPropTypesList = itemPropTypes.setdefault(itemPropKey, [])
					if isinstance(itemPropValue, (int, float)):
						itemPropTypesList.append(float)
					elif isinstance(itemPropValue, str):
						itemPropTypesList.append(str)
					elif isinstance(itemPropValue, (list, tuple)):
						itemPropTypesList.append(tuple)
			for itemPropKey, itemPropTypeList in list(itemPropTypes.items()):
				if len(set(itemPropTypeList)) != 1: # multiple types or none
					allItemPropKeys.remove(itemPropKey)
				else:
					itemPropTypes[itemPropKey] = itemPropTypeList[0]
			none_value = {float: 1e10, str: chr(127), tuple: tuple()}
			def keyFun(x):
				def enforce_type(key):
					value = x[1].get(key)
					if value is None:
						return none_value[itemPropTypes[key]]
					return itemPropTypes[key](value)
				return (tuple(imap(enforce_type, allItemPropKeys)), x[0])
			self._itemsSorted = lmap(lambda k_v: k_v[0], sorted(self._itemsDiscovered.items(), key = keyFun))

	def _broker(self, reqs, items):
		if not self._itemsDiscovered:
			return FilterBroker._broker(self, reqs, self._itemsStart) # Use user constrained items
		items = utils.QM(self._itemsStart is not None, self._itemsStart, self._itemsSorted) # or discovered items

		# Match items which fulfill the requirements
		def matcher(props):
			for key, value in reqs:
				if key not in props or (props[key] is None):
					continue
				if not (value < props[key]):
					return False
			return True
		# Apply sort order and give matching entries as preselection to FilterBroker
		items = lfilter(lambda x: matcher(self._itemsDiscovered[x]), items)
		return FilterBroker._broker(self, reqs, lfilter(lambda x: x in items, self._itemsSorted))


class StorageBroker(Broker):
	def __init__(self, config, name, userOpt, itemName, discoverFun):
		Broker.__init__(self, config, name, userOpt, itemName, discoverFun)
		self._storageDict = config.getDict('%s storage access' % userOpt, {}, onChange = None,
			parser = lambda x: parseList(x, ' '), strfun = lambda x: str.join(' ', x))[0]

	def _broker(self, reqs, items):
		result = Broker._broker(self, reqs, items)
		for (rType, rValue) in reqs:
			if (rType == WMS.STORAGE) and rValue:
				if result is None:
					result = []
				for rval in rValue:
					result.extend(self._storageDict.get(rval, []))
		return result
