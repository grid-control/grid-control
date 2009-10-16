import sys, os, random
from grid_control import AbstractObject
from wms import WMS

class Broker(AbstractObject):
	def __init__(self, config, queues):
		self.config = config
		self.queues = queues
		self.userQueue = config.get('local', 'queue', '', volatile=True).split()

	def matchQueue(self, reqs):
		return reqs

	def addQueueReq(self, reqs, queues, randomize=False):
		if len(queues) > 0:
			rIdx = 0
			if randomize:
				rIdx = random.randint(0, len(queues) - 1)
			reqs.append((WMS.SITES, queues[rIdx]))


class DummyBroker(Broker):
	def __init__(self, config, queues):
		Broker.__init__(self, config, queues)

	def matchQueue(self, reqs):
		self.addQueueReq(reqs, self.userQueue, True)
		return reqs


class SimpleBroker(Broker):
	def __init__(self, config, queues):
		Broker.__init__(self, config, queues)

	def matchQueue(self, reqs):
		def item(index):
			def helper(lst):
				return lst[index]
			return helper

		def matcher(props):
			for key, value in reqs:
				if key not in props:
					continue
				if value >= props[key]:
					return False
			return True

		def queue_cmp(a, b):
			diffs = set()
			for key in set(a.keys() + b.keys()):
				if key in a and key in b:
					diffs.add(cmp(a[key], b[key]))
				elif key in a:
					diffs.add(-1)
				else:
					diffs.add(1)
			if len(diffs) > 1:
				if diffs == set(0, 1):
					return 1
				elif diffs == set(0, -1):
					return -1
				return 0
			return diffs.pop()

		sorted_queues = sorted(self.queues.items(), queue_cmp, item(1))

		if self.queues:
			match = map(item(0), (filter(lambda (x, y): matcher(y), sorted_queues)))
			if len(match) == 0:
				match = self.userQueue
		else:
			match = self.userQueue

		if len(self.userQueue) > 0:
			# Submit job with the first queue that matches the requirements
			# and the user specified in the configuration file
			self.addQueueReq(reqs, filter(lambda x: x in match, self.userQueue))
		else:
			self.addQueueReq(reqs, match)
		return reqs
