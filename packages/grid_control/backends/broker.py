import sys, os, random, itertools
from python_compat import *
from grid_control import QM, AbstractObject, AbstractError, utils
import wms

class Broker(AbstractObject):
	def __init__(self, config, section, bic, forceQ = False, forceN = False): # backend information class
		(self.config, self.bic, self.queues, self.nodes) = (config, bic, None, None)
		# Queue info format: {'queue1': {WMS.MEMORY: 123, ...}, 'queue2': {...}}
		self.userNodes = config.getList(section, 'sites', None, volatile=True)
		self.userQueues = config.getList(section, 'queue', None, volatile=True)
		if bic:
			self.discover(self.userQueues or forceQ, self.userNodes or forceN)

	def discover(self, doQueues, doNodes):
		def doDiscover(fun, msgOK = '%d', msgFail = 'an unknown number of'):
			result = fun()
			if result != None:
				return (result, msgOK % len(result))
			return (result, msgFail)
		(msgQ, msgN) = ('an unknown number of', 'an unknown number of')
		if doQueues:
			(self.queues, msgQ) = doDiscover(self.bic.getQueues)
		if doNodes:
			(self.nodes, msgN) = doDiscover(self.bic.getNodes)
		if doQueues or doNodes:
			msgMid = QM(msgQ and msgN, ' servicing ', '')
			utils.vprint('Broker discovered %s%s%s' % (msgN + ' nodes', msgMid, msgQ + ' queues'))

	# apply selection to available resources, fall back to user input
	def filterList(self, pool, userReq, brokerReq):
		valid = utils.doBlackWhiteList(pool, userReq)
		valid = QM(valid == pool, None, valid)
		valid = utils.doBlackWhiteList(valid, brokerReq)
		ubOverlap = utils.doBlackWhiteList(brokerReq, userReq)
		if brokerReq and ubOverlap: # Use sort order of brokerReq
			valid = utils.doBlackWhiteList(brokerReq, valid)
		if brokerReq:
			valid = utils.doBlackWhiteList(valid, userReq)
		return QM(pool and brokerReq and not ubOverlap, [], valid)

	def brokerSites(self, reqs):
		raise AbstractError()

	# Add request for first / random queue on given nodes to job requirements
	def addSiteReq(self, reqs, queues = None, nodes = None, randomize=False):
		reqQueue = self.filterList(self.queues, self.userQueues, queues)
		if reqQueue:
			reqQueue = reqQueue[QM(randomize, random.randint(0, len(reqQueue) - 1), 0)]
		reqNodes = self.filterList(self.nodes, self.userNodes, nodes)
		if reqQueue != None or reqNodes != None:
			reqs.append((wms.WMS.SITES, (QM(reqQueue == [], '', reqQueue), reqNodes)))
		return reqs


class DummyBroker(Broker):
	def __init__(self, config, section, bic):
		Broker.__init__(self, config, section, None)

	def brokerSites(self, reqs):
		if self.userNodes:
			reqs.append((wms.WMS.SITES, (None, self.userNodes)))
		return reqs


class RandomBroker(Broker):
	def brokerSites(self, reqs):
		return self.addSiteReq(reqs, randomize = True)


class CoverageBroker(Broker):
	def __init__(self, config, section, bic):
		Broker.__init__(self, config, section, bic)
		self.cover = itertools.cycle(QM(self.nodes, self.nodes, []))

	def brokerSites(self, reqs):
		reqNode = QM(self.nodes, [next(self.cover, None)], None)
		return self.addSiteReq(reqs, None, reqNode, True)


class SimpleBroker(Broker):
	def __init__(self, config, section, bic):
		Broker.__init__(self, config, section, bic, forceQ = True)

	def brokerSites(self, reqs):
		# Sort queues according to requirements
		def queue_cmp(a, b):
			diff = 0 # Return negative if a<b, zero if a==b, positive if a>b
			for key in a.keys() + b.keys():
				current_diff = 1
				if key in a and key in b:
					current_diff = cmp(a[key], b[key])
				elif key in a:
					current_diff = -1
				if (diff != current_diff) and (diff == 0):
					diff = current_diff
				elif (diff != current_diff) and (current_diff != 0):
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
		return self.addSiteReq(reqs, match, None, randomize = not self.queues)

Broker.dynamicLoaderPath()
