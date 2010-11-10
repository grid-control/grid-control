import sys, os, random, itertools
from python_compat import *
from grid_control import QM, AbstractObject, utils
from wms import WMS

class Broker(AbstractObject):
	def __init__(self, config, section, bic): # backend information class
		(self.config, nodes, self.queues) = (config, bic.getNodes(), bic.getQueues())
		# Queue info format: {'queue1': {WMS.MEMORY: 123, ...}, 'queue2': {...}}
		userNodes = config.getList(section, 'sites', [], volatile=True)
		self.userQueue = config.getList(section, 'queue', [], volatile=True)
		self.nodes = None
		if nodes and userNodes:
			self.nodes = utils.doBlackWhiteList(nodes, userNodes)

	def matchQueue(self, reqs):
		return reqs

	# Add request for first / random queue on given nodes to job requirements
	def _addQueueReq(self, reqs, queues, reqNodes, randomize=False):
		reqQueue = None
		if queues:
			reqQueue = queues[QM(randomize, random.randint(0, len(queues) - 1), 0)]
		if reqQueue or reqNodes:
			reqs.append((WMS.SITES, (reqQueue, reqNodes)))
		return reqs


class DummyBroker(Broker):
	def matchQueue(self, reqs):
		return self._addQueueReq(reqs, self.userQueue, self.nodes, True)


class CoverageBroker(Broker):
	def __init__(self, config, section, bic):
		Broker.__init__(self, config, section, bic)
		self.cover = itertools.cycle(QM(self.nodes, self.nodes, []))

	def matchQueue(self, reqs):
		reqNode = QM(self.nodes, [next(self.cover, None)], None)
		return self._addQueueReq(reqs, self.userQueue, reqNode, True)


class SimpleBroker(Broker):
	def matchQueue(self, reqs):
		# Sort queues according to requirements
		def queue_cmp(a, b):
			# Return negative if a<b, zero if a==b, positive if a>b
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
		sorted_queues = []
		if self.queues:
			sorted_queues = sorted(self.queues.items(), queue_cmp, lambda x: x[1])

		# Match queues which fulfill the requirements (still sorted)
		def matcher(props):
			for key, value in reqs:
				if key not in props:
					continue
				if value >= props[key]:
					return False
			return True
		match = map(lambda x: x[0], filter(lambda (x, y): matcher(y), sorted_queues))

		# Combine matched queues and user supplied queues
		selected = self.userQueue
		if match:
			selected = QM(self.userQueue, filter(lambda x: x in match, self.userQueue), match)
		return self._addQueueReq(reqs, selected, self.nodes)

Broker.dynamicLoaderPath()
