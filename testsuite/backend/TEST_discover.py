#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import os
from testfwk import create_config, run_test, str_dict_testsuite, testfwk_set_path
from grid_control.backends.backend_tools import BackendDiscovery
from python_compat import imap, sorted


testfwk_set_path('../bin')

def test_discover(plugin, src='/dev/null', wms_id_list=None, **kwargs):
	config = create_config()
	executor = BackendDiscovery.create_instance(plugin, config, **kwargs)
	os.environ['GC_TEST_FILE'] = src
	for entry in executor.discover():
		entry = dict(imap(lambda k_v: (str(k_v[0]), k_v[1]), entry.items()))
		keys = sorted(entry.keys())
		keys.remove('name')
		print(str_dict_testsuite(entry, ['name'] + keys))

class Test_Base:
	"""
	"""

class Test_ARC:
	"""
	"""

class Test_Condor:
	"""
	"""

class Test_CREAM:
	"""
	"""

class Test_GliteWMS:
	"""
	"""

class Test_GridEngine:
	"""
	>>> test_discover('GridEngineDiscoverQueues')
	{'name': '12h.q', '62558': 44100, '62559': 43500}
	{'name': '15m.q', '62558': 1200, '62559': 930}
	{'name': '1h.q', '62558': 3900, '62559': 3900}
	{'name': '48h.q', '62558': 194400, '62559': 173100}
	{'name': '7d.q', '62558': 616500, '62559': 605700}
	{'name': 'multicore.q', '62558': 173100, '62559': 173100}
	{'name': 'proof.q', '62558': 21900, '62559': 21900}

	>>> test_discover('GridEngineDiscoverNodes')
	{'name': '@12core'}
	{'name': '@1week'}
	{'name': '@24core'}
	{'name': '@48core'}
	{'name': '@8core'}
	{'name': '@allhosts'}
	{'name': '@hyperthreading'}
	{'name': '@maintenance'}
	{'name': 'tcx140.naf.desy.de'}
	{'name': 'tcx142.naf.desy.de'}
	{'name': 'tcx143.naf.desy.de'}
	{'name': 'tcx144.naf.desy.de'}
	{'name': 'tcx145.naf.desy.de'}
	"""

class Test_Host:
	"""
	"""

class Test_JMS:
	"""
	"""

class Test_LSF:
	"""
	"""

class Test_PBS:
	"""
	>>> test_discover('PBSDiscoverQueues', src='test.PBS.queues')
	{'name': 'medium', '62558': 86700, '62559': 28740}
	{'name': 'long', '62559': 432000}
	{'name': 'short', '62558': 11100, '62559': 3600}
	{'name': 'io_only', '62559': 432000}
	{'name': 'test', '62558': 86700, '62559': 28740}
	{'name': 'infinite'}

	>>> test_discover('PBSDiscoverNodes')
	{'name': 'ekpplus010'}
	{'name': 'ekpplus012'}
	{'name': 'ekpplus013'}
	{'name': 'ekpplus015'}
	{'name': 'ekpplus016'}
	{'name': 'ekpplus022'}
	{'name': 'ekpplus023'}
	{'name': 'ekpplus027'}
	{'name': 'ekpplus028'}
	"""

class Test_SLURM:
	"""
	"""


run_test()
