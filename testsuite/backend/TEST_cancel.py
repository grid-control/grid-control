#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
import os, logging
from testFwk import create_config, run_test, setPath, try_catch
from grid_control.backends.aspect_cancel import CancelJobs
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments


setPath('../bin')

def test_cancel(plugin, src = '/dev/null', wmsIDs = None, **kwargs):
	config = create_config()
	executor = CancelJobs.create_instance(plugin, config, **kwargs)
	executor.setup(logging.getLogger())
	os.environ['GC_TEST_FILE'] = src
	for (wmsID,) in executor.execute(wmsIDs or [], 'wms_name'):
		print(wmsID)

class Test_Base:
	"""
	>>> try_catch(lambda: test_cancel('CancelJobs'), 'AbstractError', 'is an abstract function')
	caught

	>>> try_catch(lambda: test_cancel('CancelJobsWithProcess', proc_factory = ProcessCreatorAppendArguments(create_config(), 'ls')), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_ARC:
	"""
	"""

class Test_Condor:
	"""
	>>> test_cancel('Condor_CancelJobs', 'test.Condor.cancel')
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
	>>> test_cancel('Grid_CancelJobs', cancel_exec = 'glite-wms-job-cancel')
	>>> test_cancel('Grid_CancelJobs', wmsIDs = ['https://lb-3-fzk.gridka.de:9000/i_dont_exist'], cancel_exec = 'glite-wms-job-cancel')

	>>> test_cancel('Grid_CancelJobs', 'test.GliteWMS.cancel1', wmsIDs = ['https://lb-3-fzk.gridka.de:9000/i_dont_exist'], cancel_exec = 'glite-wms-job-cancel')
	https://lb-3-fzk.gridka.de:9000/GgeNd6MD5REG2KsbN5dmEg

	>>> test_cancel('Grid_CancelJobs', 'test.GliteWMS.cancel2', wmsIDs = ['https://lb-3-fzk.gridka.de:9000/i_dont_exist'], cancel_exec = 'glite-wms-job-cancel')
	https://lb-1-fzk.gridka.de:9000/yrCkbMwQjvEyFj3561Yung
	https://lb-1-fzk.gridka.de:9000/g6Sii-xhtPruRVRm5lyJBg

	>>> test_cancel('Grid_CancelJobs', 'test.GliteWMS.cancel3', wmsIDs = ['https://lb-3-fzk.gridka.de:9000/i_dont_exist'], cancel_exec = 'glite-wms-job-cancel')
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
	>>> test_cancel('LSF_CancelJobs', 'test.LSF.cancel', wmsIDs = [])

	>>> test_cancel('LSF_CancelJobs', 'test.LSF.cancel', wmsIDs = ['827766504', '827766508', '827766509'])
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
