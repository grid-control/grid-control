import sys, os, random
from grid_control import AbstractObject
from grid_control.utils import sorted
from wms import WMS

class Broker(AbstractObject):
	def __init__(self, config, queues):
		self.config = config
		# Queue info format: {'queue1': {WMS.MEMORY: 123, ...}, 'queue2': {...}}
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
			# From the doc of cmp: "Return negative if a<b, zero if
			# a==b, positive if a>b"
			diff = 0
			for key in a.keys() + b.keys():
				current_diff = 0
				if key in a and key in b:
					current_diff = cmp(a[key], b[key])
				elif key in a:
					current_diff = -1
				else:
					current_diff = 1

				if diff == 0 and diff != current_diff:
					diff = current_diff
				elif diff != current_diff and current_diff != 0:
					return 0
			return diff

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
