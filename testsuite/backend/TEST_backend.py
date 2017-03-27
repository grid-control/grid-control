#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test, testfwk_remove_files, try_catch
from grid_control.backends import WMS
from grid_control.job_db import Job


def create_wms(name):
	wms = WMS.create_instance('Host', create_config(), name)
	return (wms, wms.get_access_token(None))

def display_jobs(wms, gc_id_list):
	for (gc_id, job_state, job_info) in wms.check_jobs(gc_id_list):
		print('%s %s' % (gc_id, Job.enum2str(job_state)))

class Test_WMS:
	"""
	>>> wms = WMS.create_instance('WMS', create_config(), 'wms')
	>>> try_catch(lambda: wms.can_submit(60, True), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: wms.get_access_token(None), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: wms.deploy_task(task=None, monitor=None, transfer_se=None, transfer_sb=None), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: wms.submit_jobs([100, 200], task=None), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: wms.check_jobs(['WMSID.HOST.100', 'WMSID.HOST.200']), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: wms.cancel_jobs(['WMSID.HOST.100', 'WMSID.HOST.200']), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: wms.retrieve_jobs(['WMSID.HOST.100', 'WMSID.HOST.200']), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_MultiWMS:
	"""
	>>> testfwk_remove_files(['work/*', 'work'])
	>>> (h1, at1) = create_wms('HOST1')
	Using batch system: ---
	>>> (h2, at2) = create_wms('HOST2')
	Using batch system: ---
	>>> (h3, at3) = create_wms('HOST3')
	Using batch system: ---

	>>> hm = WMS.create_instance('MultiWMS', create_config(), 'multiwms', [h1, h2, h3])
	Broker discovered 3 wms
	>>> (at1 == at2, at1 == at3)
	(False, False)
	>>> hm.get_access_token('WMSID.HOST1.123') == at1
	True
	>>> hm.get_access_token('WMSID.HOST2.123') == at2
	True
	>>> hm.get_access_token('WMSID.HOST3.123') == at3
	True
	>>> hm.get_access_token('WMSID.HOSTx.123') == at1
	True
	>>> display_jobs(h1, ['WMSID.HOST1.100000', 'WMSID.HOST1.200000'])
	WMSID.HOST1.100000 DONE
	WMSID.HOST1.200000 DONE

	>>> display_jobs(hm, ['WMSID.HOST1.100000', 'WMSID.HOST1.200000', 'WMSID.HOST2.300000', 'WMSID.HOST3.400000', 'WMSID.HOST2.500000', 'WMSID.HOST3.600000'])
	WMSID.HOST1.100000 DONE
	WMSID.HOST1.200000 DONE
	WMSID.HOST2.300000 DONE
	WMSID.HOST2.500000 DONE
	WMSID.HOST3.400000 DONE
	WMSID.HOST3.600000 DONE

	>>> testfwk_remove_files(['work/*', 'work'])
	"""

run_test()
