#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import os, logging
from testfwk import create_config, run_test, testfwk_set_path, try_catch
from grid_control.backends.aspect_cancel import CancelJobs
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments


testfwk_set_path('../bin')

def test_cancel(plugin, src='/dev/null', wms_id_list=None, **kwargs):
	config = create_config()
	executor = CancelJobs.create_instance(plugin, config, **kwargs)
	executor.setup(logging.getLogger())
	os.environ['GC_TEST_FILE'] = src
	for (wms_id,) in executor.execute(wms_id_list or [], 'wms_name'):
		print(wms_id)

class Test_Base:
	"""
	>>> try_catch(lambda: test_cancel('CancelJobs'), 'AbstractError', 'is an abstract function')
	caught

	>>> try_catch(lambda: test_cancel('CancelJobsWithProcess', proc_factory=ProcessCreatorAppendArguments(create_config(), 'ls')), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_ARC:
	"""
	"""

class Test_Condor:
	"""
	>>> test_cancel('CondorCancelJobs', 'test.Condor.cancel')
	133662.0
	133662.1
	133662.2
	133662.3
	"""

class Test_CREAM:
	"""
	"""

class Test_GliteWMS:
	"""
	>>> test_cancel('GridCancelJobs', cancel_exec='glite-wms-job-cancel')
	>>> test_cancel('GridCancelJobs', wms_id_list=['https://lb-3-fzk.gridka.de:9000/i_dont_exist'], cancel_exec='glite-wms-job-cancel')

	>>> test_cancel('GridCancelJobs', 'test.GliteWMS.cancel1', wms_id_list=['https://lb-3-fzk.gridka.de:9000/i_dont_exist'], cancel_exec='glite-wms-job-cancel')
	https://lb-3-fzk.gridka.de:9000/GgeNd6MD5REG2KsbN5dmEg

	>>> test_cancel('GridCancelJobs', 'test.GliteWMS.cancel2', wms_id_list=['https://lb-3-fzk.gridka.de:9000/i_dont_exist'], cancel_exec='glite-wms-job-cancel')
	https://lb-1-fzk.gridka.de:9000/yrCkbMwQjvEyFj3561Yung
	https://lb-1-fzk.gridka.de:9000/g6Sii-xhtPruRVRm5lyJBg

	>>> test_cancel('GridCancelJobs', 'test.GliteWMS.cancel3', wms_id_list=['https://lb-3-fzk.gridka.de:9000/i_dont_exist'], cancel_exec='glite-wms-job-cancel')
	Process '<testsuite dir>/bin/glite-wms-job-cancel' '--noint' '--logfile' '/dev/stderr' '-i' '/dev/stdin' finished with exit code 2
	"""

class Test_GridEngine:
	"""
	"""

class Test_Host:
	"""
	"""

class Test_JMS:
	"""
	"""

class Test_LSF:
	"""
	>>> test_cancel('LSFCancelJobs', 'test.LSF.cancel', wms_id_list=[])

	>>> test_cancel('LSFCancelJobs', 'test.LSF.cancel', wms_id_list=['827766504', '827766508', '827766509'])
	827766504
	827766508
	827766509
	"""

class Test_PBS:
	"""
	"""

class Test_SLURM:
	"""
	"""

run_test()
