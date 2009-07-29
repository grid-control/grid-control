import sys, os, random
from grid_control import AbstractObject
from wms import WMS

class Broker(AbstractObject):
	def __init__(self, config, queues):
		self.config = config
		self.queues = queues
		self.userQueue = config.get('local', 'queue', '').split()

	def matchQueue(self, reqs):
		return reqs

	def addQueueReq(self, reqs, queues):
		if len(queues) > 0:
			rIdx = random.randint(0, len(queues) - 1)
			reqs.append((WMS.SITES, queues[rIdx]))


class DummyBroker(Broker):
	def __init__(self, config, queues):
		Broker.__init__(self, config, queues)

	def matchQueue(self, reqs):
		self.addQueueReq(reqs, self.userQueue)
		return reqs


class SimpleBroker(Broker):
	def __init__(self, config, queues):
		Broker.__init__(self, config, queues)

	def matchQueue(self, reqs):
		def matcher(props):
			for key, value in reqs:
				if not props.has_key(key):
					continue
				if value >= props[key]:
					return False
			return True

		if self.queues:
			match = dict(filter(lambda (x, y): matcher(y), self.queues.items())).keys()
			if len(match) == 0:
				match = self.userQueue
		else:
			match = self.userQueue

		if len(self.userQueue) > 0:
			self.addQueueReq(reqs, filter(lambda x: x in self.userQueue, match))
		else:
			self.addQueueReq(reqs, match)
		return reqs
