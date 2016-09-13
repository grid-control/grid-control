#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
import random
from testFwk import create_config, run_test
from grid_control.backends import WMS
from grid_control.backends.broker_base import Broker
from python_compat import sorted


random.shuffle = lambda x: sorted(x)
random.sample = lambda x, n: sorted(x)[:n]

req_base = [(WMS.WALLTIME, 123), (WMS.CPUTIME, 134), (WMS.MEMORY, 321), (WMS.CPUS, 3)]
queues_base = {
	'long1': {WMS.CPUTIME: 432000},
	'long2': {WMS.CPUTIME: 432000},
}

def createBroker(name, config_dict, items = queues_base):
	random.seed(123)
	config = create_config(config_dict={'broker': config_dict})
	broker = Broker.create_instance(name, config, '', 'items', 'items', lambda: items)
	return broker

def checkReq(broker, reqIn, reqOut = []):
	value = broker.brokerAdd(list(reqIn), WMS.BACKEND)
	return value == reqIn + reqOut

class Test_Broker_UserBroker:
	"""
	>>> broker = createBroker('UserBroker', {})
	>>> checkReq(broker, req_base, [])
	True
	>>> broker = createBroker('UserBroker', {'items': 'a b c'})
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b', 'c'])])
	True
	>>> broker = createBroker('UserBroker', {'items': 'a b c'}, {})
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b', 'c'])])
	True
	>>> broker = createBroker('UserBroker', {'items': 'a -b -c'}, {})
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', '-b', '-c'])])
	True
	>>> broker = createBroker('UserBroker', {'items': 'a b c'}, queues_base)
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b', 'c'])])
	True
	>>> broker = createBroker('UserBroker', {'items': 'a -b -c'}, queues_base)
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', '-b', '-c'])])
	True

	>>> broker = createBroker('UserBroker', {'items': 'a b c', 'items entries': '2'})
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b'])])
	True
	>>> broker = createBroker('UserBroker', {'items': 'a b c', 'items entries': '2', 'items randomize': 'True'})
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b'])])
	True
	>>> broker = createBroker('UserBroker', {'items': 'a b c', 'items randomize': 'True'})
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b', 'c'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b', 'c'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['a', 'b', 'c'])])
	True
	"""

class Test_Broker_CoverageBroker:
	"""
	>>> broker = createBroker('CoverageBroker', {}, {})
	Broker discovered 0 items
	>>> checkReq(broker, req_base, [])
	True

	>>> broker = createBroker('CoverageBroker', {}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long2'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True

	>>> broker = createBroker('CoverageBroker', {'items': 'ekpplus001 ekpplus002 ekpplus003 ekpplus004'}, {})
	Broker discovered 0 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['ekpplus001'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['ekpplus002'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['ekpplus003'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['ekpplus004'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['ekpplus001'])])
	True

	>>> broker = createBroker('CoverageBroker', {'items': 'long1 -long2 long3'}, {})
	Broker discovered 0 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long3'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True

	>>> broker = createBroker('CoverageBroker', {'items': 'long1 long2 long3'}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long2'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True

	>>> broker = createBroker('CoverageBroker', {'items': 'long1 long3'}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True

	>>> broker = createBroker('CoverageBroker', {'items': '-long2'}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True
	"""

class Test_Broker_FilterBroker:
	"""
	>>> broker = createBroker('FilterBroker', {}, {})
	>>> checkReq(broker, req_base, [])
	True
	>>> broker = createBroker('FilterBroker', {}, queues_base)
	>>> checkReq(broker, req_base, [])
	True

	>>> broker = createBroker('FilterBroker', {'items': '-long1'}, None)
	Broker discovered an unknown number of items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['-long1'])])
	True
	>>> broker = createBroker('FilterBroker', {'items': '-long1'}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long2'])])
	True
	>>> broker = createBroker('FilterBroker', {'items': '-long1 -long2'}, None)
	Broker discovered an unknown number of items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['-long1', '-long2'])])
	True
	>>> broker = createBroker('FilterBroker', {'items': '-long1 -long2'}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, [])])
	True

	>>> broker = createBroker('FilterBroker', {'items': 'long1 long2'}, None)
	Broker discovered an unknown number of items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1', 'long2'])])
	True
	>>> broker = createBroker('FilterBroker', {'items': 'long2 long1'}, {})
	Broker discovered 0 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long2', 'long1'])])
	True
	>>> broker = createBroker('FilterBroker', {'items': 'long1 long2'}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1', 'long2'])])
	True
	>>> broker = createBroker('FilterBroker', {'items': 'long2 long1'}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long2', 'long1'])])
	True

	>>> broker = createBroker('FilterBroker', {'items': 'long1 -long2 long3'}, None)
	Broker discovered an unknown number of items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1', '-long2', 'long3'])])
	True
	>>> broker = createBroker('FilterBroker', {'items': 'long1 -long2 long3'}, {})
	Broker discovered 0 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1', '-long2', 'long3'])])
	True

	>>> broker = createBroker('FilterBroker', {'items': 'long1 -long2 long3'}, queues_base)
	Broker discovered 2 items
	>>> checkReq(broker, req_base, [(WMS.BACKEND, ['long1'])])
	True

	"""

req0 = []
req1 = [(WMS.WALLTIME, 423), (WMS.CPUTIME, 234), (WMS.MEMORY, 321), (WMS.CPUS, 3)]
req2 = [(WMS.WALLTIME, 42300), (WMS.CPUTIME, 23400), (WMS.MEMORY, 321), (WMS.CPUS, 3)]
req3 = [(WMS.WALLTIME, 122300), (WMS.CPUTIME, 93400), (WMS.MEMORY, 321), (WMS.CPUS, 3)]
req4 = [(WMS.WALLTIME, 1223000), (WMS.CPUTIME, 934000), (WMS.MEMORY, 321), (WMS.CPUS, 3)]
req4 = [(WMS.WALLTIME, 1223000), (WMS.CPUTIME, 934000), (WMS.MEMORY, 321), (WMS.CPUS, 3)]
req5 = [(WMS.STORAGE, None)]
req6 = [(WMS.STORAGE, [])]
req7 = [(WMS.STORAGE, ['a', 'b', 'c'])]

queues1 = {
	'longl': {WMS.CPUTIME: 432000},
	'longs': {WMS.CPUTIME: 430000},
}
queues2 = {
	'long1': {WMS.WALLTIME: 987, WMS.CPUTIME: 123},
	'long2': {WMS.WALLTIME: 123, WMS.CPUTIME: 987},
}
queues3 = {
	'medium': {WMS.WALLTIME: 86700, WMS.CPUTIME: 28740},
	'long': {WMS.CPUTIME: 432000},
	'infinite': {},
	'short': {WMS.WALLTIME: 11100, WMS.CPUTIME: 3600},
	'io_only': {WMS.CPUTIME: 432000},
	'test': {WMS.WALLTIME: 86700, WMS.CPUTIME: 28740}
}
queues4 = {
	'q_all1': {},
	'q_all2': {WMS.STORAGE: None},
	'q_ab': {WMS.STORAGE: ['a', 'b']},
	'q_cd': {WMS.STORAGE: ['c', 'd']},
	'q_xy': {WMS.STORAGE: ['x', 'y']},
}

class Test_Broker_SimpleBroker:
	"""
	>>> broker = createBroker('SimpleBroker', {}, {})
	Broker discovered 0 items
	>>> checkReq(broker, req0, [])
	True
	>>> checkReq(broker, req1, [])
	True
	>>> checkReq(broker, req2, [])
	True
	>>> checkReq(broker, req3, [])
	True
	>>> checkReq(broker, req4, [])
	True
	>>> checkReq(broker, req5, [])
	True
	>>> checkReq(broker, req6, [])
	True
	>>> checkReq(broker, req7, [])
	True

	>>> broker = createBroker('SimpleBroker', {}, queues1)
	Broker discovered 2 items
	>>> checkReq(broker, req0, [(WMS.BACKEND, ['longs', 'longl'])])
	True
	>>> checkReq(broker, req1, [(WMS.BACKEND, ['longs', 'longl'])])
	True
	>>> checkReq(broker, req2, [(WMS.BACKEND, ['longs', 'longl'])])
	True
	>>> checkReq(broker, req3, [(WMS.BACKEND, ['longs', 'longl'])])
	True
	>>> checkReq(broker, req4, [(WMS.BACKEND, [])])
	True
	>>> checkReq(broker, req5, [(WMS.BACKEND, ['longs', 'longl'])])
	True
	>>> checkReq(broker, req6, [(WMS.BACKEND, ['longs', 'longl'])])
	True
	>>> checkReq(broker, req7, [(WMS.BACKEND, ['longs', 'longl'])])
	True

	>>> broker = createBroker('SimpleBroker', {}, queues2)
	Broker discovered 2 items
	>>> checkReq(broker, req0, [(WMS.BACKEND, ['long2', 'long1'])])
	True
	>>> checkReq(broker, req1, [(WMS.BACKEND, [])])
	True
	>>> checkReq(broker, req2, [(WMS.BACKEND, [])])
	True
	>>> checkReq(broker, req3, [(WMS.BACKEND, [])])
	True
	>>> checkReq(broker, req4, [(WMS.BACKEND, [])])
	True
	>>> checkReq(broker, req5, [(WMS.BACKEND, ['long2', 'long1'])])
	True
	>>> checkReq(broker, req6, [(WMS.BACKEND, ['long2', 'long1'])])
	True
	>>> checkReq(broker, req7, [(WMS.BACKEND, ['long2', 'long1'])])
	True

	>>> broker = createBroker('SimpleBroker', {}, queues3)
	Broker discovered 6 items
	>>> checkReq(broker, req0, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long', 'infinite'])])
	True
	>>> checkReq(broker, req1, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long', 'infinite'])])
	True
	>>> checkReq(broker, req2, [(WMS.BACKEND, ['medium', 'test', 'io_only', 'long', 'infinite'])])
	True
	>>> checkReq(broker, req3, [(WMS.BACKEND, ['io_only', 'long', 'infinite'])])
	True
	>>> checkReq(broker, req4, [(WMS.BACKEND, ['infinite'])])
	True
	>>> checkReq(broker, req5, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long', 'infinite'])])
	True
	>>> checkReq(broker, req6, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long', 'infinite'])])
	True
	>>> checkReq(broker, req7, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long', 'infinite'])])
	True

	>>> broker = createBroker('SimpleBroker', {'items': '-infinite'}, queues3)
	Broker discovered 6 items
	>>> checkReq(broker, req0, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long'])])
	True
	>>> checkReq(broker, req1, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long'])])
	True
	>>> checkReq(broker, req2, [(WMS.BACKEND, ['medium', 'test', 'io_only', 'long'])])
	True
	>>> checkReq(broker, req3, [(WMS.BACKEND, ['io_only', 'long'])])
	True
	>>> checkReq(broker, req4, [(WMS.BACKEND, [])])
	True
	>>> checkReq(broker, req5, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long'])])
	True
	>>> checkReq(broker, req6, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long'])])
	True
	>>> checkReq(broker, req7, [(WMS.BACKEND, ['short', 'medium', 'test', 'io_only', 'long'])])
	True

	>>> broker = createBroker('StorageBroker', {'items storage access': '1 2 3\\na=>x\\nb=>y -z'}, [])
	>>> checkReq(broker, req0)
	True
	>>> checkReq(broker, req5)
	True
	>>> checkReq(broker, req6)
	True
	>>> checkReq(broker, req7, [(WMS.BACKEND, ['x', 'y', '-z', '1', '2', '3'])])
	True
	"""

run_test()
