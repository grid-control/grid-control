import random
from grid_control import QM, AbstractObject, AbstractError, utils

class Broker(AbstractObject):
	def __init__(self, config, section, userOpt, itemName, discoverFun):
		(self._itemsStart, self._itemsDiscovered, self._itemName) = (None, False, itemName)
		self._nEntries = config.getInt(section, '%s entries' % userOpt, 0, onChange = None)
		self._nRandom = config.getBool(section, '%s randomize' % userOpt, False, onChange = None)

	def _discover(self, discoverFun, cached = True):
		if not cached or (self._itemsDiscovered == False):
			self._itemsDiscovered = discoverFun()
			msg = 'an unknown number of'
			if self._itemsDiscovered != None:
				msg = str(len(self._itemsDiscovered))
			utils.vprint('Broker discovered %s %s' % (msg, self._itemName))
		return self._itemsDiscovered

	def _broker(self, reqs, items):
		if items and self._nRandom:
			return random.sample(items, QM(self._nEntries, self._nEntries, len(items)))
		elif items and self._nEntries:
			return items[:self._nEntries]
		return items

	def brokerAdd(self, reqs, reqEntry):
		result = self._broker(reqs, self._itemsStart)
		if result != None:
			reqs.append((reqEntry, result))
		return reqs
Broker.dynamicLoaderPath()
