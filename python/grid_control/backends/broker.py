import sys, os, random
from python_compat import *
from grid_control import AbstractObject, utils
from wms import WMS

class Broker(AbstractObject):
	def __init__(self, config, queues, nodes):
		self.config = config
		self.allnodes = nodes
		# Queue info format: {'queue1': {WMS.MEMORY: 123, ...}, 'queue2': {...}}
		self.queues = queues
		self.userQueue = config.get('local', 'queue', '', volatile=True).split()
		self.userNodes = config.get('local', 'sites', '', volatile=True).split()
		if (len(self.userNodes) == 0) or (nodes == None):
			self.nodes = None
		else:
			self.nodes = utils.doBlackWhiteList(nodes, self.userNodes)

	def matchQueue(self, reqs):
		return reqs

	def addQueueReq(self, reqs, queues, randomize=False, nodes=None):
		if len(queues) > 0:
			rIdx = 0
			if randomize:
				rIdx = random.randint(0, len(queues) - 1)
			if nodes == None:
				nodes = self.nodes
			reqs.append((WMS.SITES, (queues[rIdx], nodes)))


class DummyBroker(Broker):
	def matchQueue(self, reqs):
		self.addQueueReq(reqs, self.userQueue, randomize=True)
		return reqs


class CoverageBroker(Broker):
	def __init__(self, config, queues, nodes):
		Broker.__init__(self, config, queues, nodes)
		self.counter = 0

	def matchQueue(self, reqs):
		node = None
		if self.allnodes:
			node = self.allnodes[self.counter % len(self.allnodes)]
			self.counter += 1
		self.addQueueReq(reqs, self.userQueue, nodes = node)
		return reqs


class SimpleBroker(Broker):
	def matchQueue(self, reqs):
		def matcher(props):
			for key, value in reqs:
				if key not in props:
					continue
				if value >= props[key]:
					return False
			return True

		def queue_cmp(a, b):
			# From the doc of cmp: 'Return negative if a<b, zero if
			# a==b, positive if a>b'
			diff = 0
			for key in a.keys() + b.keys():
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

		sorted_queues = sorted(self.queues.items(), queue_cmp, lambda x: x[1])

		if self.queues:
			match = map(lambda x: x[0], filter(lambda (x, y): matcher(y), sorted_queues))
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

Broker.dynamicLoaderPath()
