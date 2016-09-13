#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
import os, logging
from testFwk import create_config, run_test, setPath, str_dict_testsuite, try_catch
from grid_control.backends.aspect_status import CheckInfo, CheckJobs, CheckStatus
from grid_control.backends.backend_tools import ProcessCreatorAppendArguments
from grid_control.job_db import Job


setPath('../bin')

def test_check(plugin, src = '/dev/null', src_err = '/dev/null', src_ret = 0, wmsIDs = None, overwrite_exec = None, do_print = True, config_dict = None, **kwargs):
	config = create_config(config_dict = config_dict or {})
	executor = CheckJobs.create_instance(plugin, config, **kwargs)
	executor.setup(logging.getLogger())
	if overwrite_exec:
		executor._proc_factory._cmd = overwrite_exec
	os.environ['GC_TEST_FILE'] = src
	os.environ['GC_TEST_ERR'] = src_err
	os.environ['GC_TEST_RET'] = str(src_ret)
	for wmsID, jobStatus, jobInfo in executor.execute(wmsIDs or []):
		for key in list(jobInfo):
			if isinstance(key, int):
				jobInfo['<%s>' % CheckInfo.enum2str(key)] = jobInfo.pop(key)
		if do_print:
			print('%s %s %s' % (wmsID, Job.enum2str(jobStatus), str_dict_testsuite(jobInfo)))
	print('STATUS=%s' % CheckStatus.enum2str(executor.get_status()))

class Test_Base:
	"""
	>>> try_catch(lambda: test_check('CheckJobs'), 'AbstractError', 'is an abstract function')
	caught

	>>> try_catch(lambda: test_check('CheckJobsWithProcess', proc_factory = ProcessCreatorAppendArguments(create_config(), 'ls')), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_ARC:
	"""
	>>> test_check('ARC_CheckJobs', wmsIDs = ['gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m'])
	STATUS=OK

	>>> test_check('ARC_CheckJobs', 'test.ARC.status1')
	gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m QUEUED {'<RAW_STATUS>': 'queuing', 'entry valid for': '20 minutes', 'entry valid from': '2016-06-18 18:20:24', 'id on service': 'U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m', 'job management url': 'gsiftp://grid-arc0.desy.de:2811/jobs (org.nordugrid.gridftpjob)', 'job status url': 'ldap://grid-arc0.desy.de:2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=gsiftp:\\\\2f\\\\2fgrid-arc0.desy.de:2811\\\\2fjobs\\\\2fU3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m) (org.nordugrid.ldapng)', 'name': 'ARC', 'other messages': 'SubmittedVia=org.nordugrid.gridftpjob', 'owner': '/C=DE/O=GermanGrid/OU=uni-hamburg/CN=Fred Markus Stober', 'queue': 'grid', 'requested slots': '1', 'service information url': 'ldap://grid-arc0.desy.de:2135/Mds-Vo-Name=local,o=grid??sub?(objectClass=*) (org.nordugrid.ldapng)', 'session directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m', 'stagein directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m', 'stageout directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m', 'state': 'Queuing (INLRMS:Q)', 'stderr': 'err', 'stdin': '/dev/null', 'stdout': 'out', 'submitted': '2016-06-18 18:01:23', 'submitted from': '131.169.168.65:56263', 'waiting position': '1'}
	gsiftp://grid-arc0.desy.de:2811/jobs/xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn QUEUED {'<RAW_STATUS>': 'queuing', 'entry valid for': '20 minutes', 'entry valid from': '2016-06-18 18:20:24', 'id on service': 'xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn', 'job management url': 'gsiftp://grid-arc0.desy.de:2811/jobs (org.nordugrid.gridftpjob)', 'job status url': 'ldap://grid-arc0.desy.de:2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=gsiftp:\\\\2f\\\\2fgrid-arc0.desy.de:2811\\\\2fjobs\\\\2fxBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn) (org.nordugrid.ldapng)', 'name': 'ARC', 'other messages': 'SubmittedVia=org.nordugrid.gridftpjob', 'owner': '/C=DE/O=GermanGrid/OU=uni-hamburg/CN=Fred Markus Stober', 'queue': 'grid', 'requested slots': '1', 'service information url': 'ldap://grid-arc0.desy.de:2135/Mds-Vo-Name=local,o=grid??sub?(objectClass=*) (org.nordugrid.ldapng)', 'session directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn', 'stagein directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn', 'stageout directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn', 'state': 'Queuing (INLRMS:Q)', 'stderr': 'err', 'stdin': '/dev/null', 'stdout': 'out', 'submitted': '2016-06-18 18:06:17', 'submitted from': '131.169.168.65:56391', 'waiting position': '2'}
	STATUS=OK

	>>> test_check('ARC_CheckJobs', 'test.ARC.status2')
	gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m DONE {'<RAW_STATUS>': 'finished', 'end time': '2016-06-18 19:14:05', 'entry valid for': '20 minutes', 'entry valid from': '2016-06-18 20:30:10', 'exit code': '0', 'id on service': 'U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m', 'job management url': 'gsiftp://grid-arc0.desy.de:2811/jobs (org.nordugrid.gridftpjob)', 'job status url': 'ldap://grid-arc0.desy.de:2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=gsiftp:\\\\2f\\\\2fgrid-arc0.desy.de:2811\\\\2fjobs\\\\2fU3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m) (org.nordugrid.ldapng)', 'name': 'ARC', 'other messages': 'SubmittedVia=org.nordugrid.gridftpjob', 'owner': '/C=DE/O=GermanGrid/OU=uni-hamburg/CN=Fred Markus Stober', 'queue': 'grid', 'requested slots': '1', 'results must be retrieved before': '2016-06-25 18:21:58', 'service information url': 'ldap://grid-arc0.desy.de:2135/Mds-Vo-Name=local,o=grid??sub?(objectClass=*) (org.nordugrid.ldapng)', 'session directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m', 'stagein directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m', 'stageout directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/U3pLDmfrXYonXlDnJp9YSwCoABFKDmABFKDmWpJKDmABFKDmoDon2m', 'state': 'Finished (FINISHED)', 'stderr': 'err', 'stdin': '/dev/null', 'stdout': 'out', 'submitted': '2016-06-18 18:01:23', 'submitted from': '131.169.168.65:56263'}
	gsiftp://grid-arc0.desy.de:2811/jobs/xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn DONE {'<RAW_STATUS>': 'finished', 'end time': '2016-06-18 19:15:27', 'entry valid for': '20 minutes', 'entry valid from': '2016-06-18 20:30:10', 'exit code': '0', 'id on service': 'xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn', 'job management url': 'gsiftp://grid-arc0.desy.de:2811/jobs (org.nordugrid.gridftpjob)', 'job status url': 'ldap://grid-arc0.desy.de:2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=gsiftp:\\\\2f\\\\2fgrid-arc0.desy.de:2811\\\\2fjobs\\\\2fxBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn) (org.nordugrid.ldapng)', 'name': 'ARC', 'other messages': 'SubmittedVia=org.nordugrid.gridftpjob', 'owner': '/C=DE/O=GermanGrid/OU=uni-hamburg/CN=Fred Markus Stober', 'queue': 'grid', 'requested slots': '1', 'results must be retrieved before': '2016-06-25 18:33:07', 'service information url': 'ldap://grid-arc0.desy.de:2135/Mds-Vo-Name=local,o=grid??sub?(objectClass=*) (org.nordugrid.ldapng)', 'session directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn', 'stagein directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn', 'stageout directory url': 'gsiftp://grid-arc0.desy.de:2811/jobs/xBBMDmPwXYonXlDnJp9YSwCoABFKDmABFKDmlqLKDmABFKDmycOlCn', 'state': 'Finished (FINISHED)', 'stderr': 'err', 'stdin': '/dev/null', 'stdout': 'out', 'submitted': '2016-06-18 18:06:17', 'submitted from': '131.169.168.65:56391'}
	STATUS=OK
	"""

class Test_Condor:
	"""
	>>> test_check('Condor_CheckJobs', wmsIDs = ['78905.0'])
	STATUS=OK

	>>> test_check('Condor_CheckJobs', 'test.Condor.status_long1')
	78905.0 RUNNING {'<RAW_STATUS>': 2, '<WN>': 'slot1@moab-vm-702139.bwforcluster', 'CompletionDate': '0', 'GlobalJobId': 'ekpcms6.physik.uni-karlsruhe.de#78905.0#1466285011', 'JobCurrentStartDate': '1466285012', 'JobStartDate': '1466285012', 'QDate': '1466285011'}
	78905.1 RUNNING {'<RAW_STATUS>': 2, '<WN>': 'slot2@moab-vm-702139.bwforcluster', 'CompletionDate': '0', 'GlobalJobId': 'ekpcms6.physik.uni-karlsruhe.de#78905.1#1466285011', 'JobCurrentStartDate': '1466285012', 'JobStartDate': '1466285012', 'QDate': '1466285011'}
	STATUS=OK

	>>> test_check('Condor_CheckJobs', src_err = 'test.Condor.status_long2', src_ret = 123)
	Process '<testsuite dir>/bin/condor_q' '-long' finished with exit code 123
	STATUS=ERROR
	"""

class Test_CREAM:
	"""
	>>> test_check('CREAM_CheckJobs', wmsIDs = ['https://grid-cr0.desy.de:8443/CREAM052718106'])
	STATUS=OK

	>>> test_check('CREAM_CheckJobs', 'test.CREAM.status')
	https://grid-cr0.desy.de:8443/CREAM052718106 ABORTED {'<RAW_STATUS>': 'ABORTED', 'description': 'submission to BLAH failed [retry count=3]', 'failurereason': 'BLAH error: submission command failed (exit code = 1) (stdout:) (stderr:qsub: Unauthorized Request  MSG=group ACL is not satisfied: user cmsger030@grid-cr0.desy.de, queue desy-) N/A (jobId = CREAM052718106)'}
	https://grid-cr0.desy.de:8443/CREAM910144555 ABORTED {'<RAW_STATUS>': 'ABORTED', 'description': 'submission to BLAH failed [retry count=3]', 'failurereason': 'BLAH error: submission command failed (exit code = 1) (stdout:) (stderr:qsub: Unauthorized Request  MSG=group ACL is not satisfied: user cmsger030@grid-cr0.desy.de, queue desy-) N/A (jobId = CREAM910144555)'}
	STATUS=OK

	>>> test_check('CREAM_CheckJobs', 'test.CREAM.status', src_ret = 1)
	https://grid-cr0.desy.de:8443/CREAM052718106 ABORTED {'<RAW_STATUS>': 'ABORTED', 'description': 'submission to BLAH failed [retry count=3]', 'failurereason': 'BLAH error: submission command failed (exit code = 1) (stdout:) (stderr:qsub: Unauthorized Request  MSG=group ACL is not satisfied: user cmsger030@grid-cr0.desy.de, queue desy-) N/A (jobId = CREAM052718106)'}
	https://grid-cr0.desy.de:8443/CREAM910144555 ABORTED {'<RAW_STATUS>': 'ABORTED', 'description': 'submission to BLAH failed [retry count=3]', 'failurereason': 'BLAH error: submission command failed (exit code = 1) (stdout:) (stderr:qsub: Unauthorized Request  MSG=group ACL is not satisfied: user cmsger030@grid-cr0.desy.de, queue desy-) N/A (jobId = CREAM910144555)'}
	Process '<testsuite dir>/bin/glite-ce-job-status' '--level' '0' '--logfile' '/dev/stderr' finished with exit code 1
	STATUS=OK
	"""

class Test_GliteWMS:
	"""
	>>> test_check('Grid_CheckJobs', wmsIDs = ['https://lb-3-fzk.gridka.de:9000/GgeNd6MD5REG2KsbN5dmEg'], check_exec = 'glite-wms-job-status')
	STATUS=OK

	>>> test_check('Grid_CheckJobs', 'test.GliteWMS.status1', check_exec = 'glite-wms-job-status')
	https://lb-3-fzk.gridka.de:9000/GgeNd6MD5REG2KsbN5dmEg QUEUED {'<QUEUE>': 'jobmanager-pbspro-cmsXS', '<RAW_STATUS>': 'scheduled', '<SITE>': 'ce-2-fzk.gridka.de:2119', 'reason': 'Job successfully submitted to Globus', 'timestamp': 1289766838}
	STATUS=OK

	>>> test_check('Grid_CheckJobs', 'test.GliteWMS.status2', check_exec = 'glite-wms-job-status')
	https://grid-lb0.desy.de:9000/gI6QdIJdvkCj3V2nqRQInw DONE {'<QUEUE>': 'cream-pbs-cms', '<RAW_STATUS>': 'done', '<SITE>': 'cream02.athena.hellasgrid.gr:8443', 'exit code': '0', 'reason': 'Job Terminated Successfully', 'timestamp': 1289772322}
	https://lb-3-fzk.gridka.de:9000/h261HCD0QRxIn1gr-8Q8VA RUNNING {'<QUEUE>': 'jobmanager-lcglsf-grid_2nh_cms', '<RAW_STATUS>': 'running', '<SITE>': 'ce126.cern.ch:2119', 'reason': 'Job successfully submitted to Globus', 'timestamp': 1289772334}
	STATUS=OK

	>>> test_check('Grid_CheckJobs', 'test.GliteWMS.status3', check_exec = 'glite-wms-job-status')
	https://lb-1-fzk.gridka.de:9000/Cr-LicZeP8kiaLG77_JXyQ DONE {'<QUEUE>': 'jobmanager-lcgpbs-cms', '<RAW_STATUS>': 'done', '<SITE>': 't2-ce-02.to.infn.it:2119', 'exit code': '0', 'reason': 'Job terminated successfully', 'timestamp': 1289772799}
	https://lb-3-fzk.gridka.de:9000/h261HCD0QRxIn1gr-8Q8VA DONE {'<QUEUE>': 'jobmanager-lcglsf-grid_2nh_cms', '<RAW_STATUS>': 'done', '<SITE>': 'ce126.cern.ch:2119', 'exit code': '0', 'reason': 'Job terminated successfully', 'timestamp': 1289772334}
	STATUS=OK
	"""

class Test_GridEngine:
	"""
	>>> test_check('GridEngine_CheckJobs', 'test.GridEngine.status1')
	6323 RUNNING {'<QUEUE>': 'short', '<RAW_STATUS>': 'r', '<WN>': 'ekpplus008.ekpplus.cluster', 'JAT_prio': '0.55077', 'JAT_start_time': '2010-11-04T20:28:38', 'JB_name': 'GC11e8a12f.12', 'JB_owner': 'stober', 'queue_name': 'short@ekpplus008.ekpplus.cluster', 'slots': '1'}
	6324 RUNNING {'<QUEUE>': 'short', '<RAW_STATUS>': 't', '<WN>': 'ekpplus001.ekpplus.cluster', 'JAT_prio': '0.55071', 'JAT_start_time': '2010-11-04T20:28:38', 'JB_name': 'GC11e8a12f.13', 'JB_owner': 'stober', 'queue_name': 'short@ekpplus001.ekpplus.cluster', 'slots': '1'}
	6350 UNKNOWN {'<RAW_STATUS>': 'Eqw', 'JAT_prio': '0.55025', 'JB_name': 'GC11e8a12f.38', 'JB_owner': 'stober', 'JB_submission_time': '2010-11-04T20:28:34', 'slots': '1'}
	6352 QUEUED {'<RAW_STATUS>': 'qw', 'JAT_prio': '0.00000', 'JB_name': 'GC11e8a12f.40', 'JB_owner': 'stober', 'JB_submission_time': '2010-11-04T20:28:38', 'slots': '1'}
	STATUS=OK

	>>> test_check('GridEngine_CheckJobs', 'test.GridEngine.status1', config_dict = {'backend': {'user': ''}})
	6323 RUNNING {'<QUEUE>': 'short', '<RAW_STATUS>': 'r', '<WN>': 'ekpplus008.ekpplus.cluster', 'JAT_prio': '0.55077', 'JAT_start_time': '2010-11-04T20:28:38', 'JB_name': 'GC11e8a12f.12', 'JB_owner': 'stober', 'queue_name': 'short@ekpplus008.ekpplus.cluster', 'slots': '1'}
	6324 RUNNING {'<QUEUE>': 'short', '<RAW_STATUS>': 't', '<WN>': 'ekpplus001.ekpplus.cluster', 'JAT_prio': '0.55071', 'JAT_start_time': '2010-11-04T20:28:38', 'JB_name': 'GC11e8a12f.13', 'JB_owner': 'stober', 'queue_name': 'short@ekpplus001.ekpplus.cluster', 'slots': '1'}
	6350 UNKNOWN {'<RAW_STATUS>': 'Eqw', 'JAT_prio': '0.55025', 'JB_name': 'GC11e8a12f.38', 'JB_owner': 'stober', 'JB_submission_time': '2010-11-04T20:28:34', 'slots': '1'}
	6352 QUEUED {'<RAW_STATUS>': 'qw', 'JAT_prio': '0.00000', 'JB_name': 'GC11e8a12f.40', 'JB_owner': 'stober', 'JB_submission_time': '2010-11-04T20:28:38', 'slots': '1'}
	STATUS=OK

	>>> test_check('GridEngine_CheckJobs', 'test.GridEngine.status2')
	STATUS=OK

	>>> try_catch(lambda: test_check('GridEngine_CheckJobs', 'test.GridEngine.status3'), 'BackendError', 'parse qstat XML output')
	caught

	>>> try_catch(lambda: test_check('GridEngine_CheckJobs', 'test.GridEngine.status4'), 'BackendError', 'Error reading job info')
	caught

	>>> test_check('GridEngine_CheckJobs', 'test.GridEngine.status5')
	6352 READY {'<RAW_STATUS>': 'xxx', 'JAT_prio': '0.00000', 'JB_name': 'GC11e8a12f.40', 'JB_owner': 'stober', 'JB_submission_time': '2010-11-04T20:28:38', 'slots': '1'}
	STATUS=OK
	"""

class Test_Host:
	"""
	>>> test_check('Host_CheckJobs', wmsIDs = ['1036587'])
	STATUS=OK

	>>> test_check('Host_CheckJobs', 'test.Host.status', overwrite_exec = '../bin/ps')
	12862 RUNNING {'<QUEUE>': 'localqueue', '<RAW_STATUS>': 'SN', '<WN>': 'localhost', 'command': '/bin/bash /usr/users/stober/grid-control/share/local.sh', 'cpu': '0.0', 'mem': '0.0', 'rss': '1244', 'start': '20:33', 'time': '0:00', 'tty': 'pts/62', 'user': 'stober', 'vsz': '63792'}
	12868 RUNNING {'<QUEUE>': 'localqueue', '<RAW_STATUS>': 'SN', '<WN>': 'localhost', 'command': '/bin/bash /usr/users/stober/grid-control/share/local.sh', 'cpu': '0.0', 'mem': '0.0', 'rss': '1244', 'start': '20:33', 'time': '0:00', 'tty': 'pts/62', 'user': 'stober', 'vsz': '63792'}
	12878 UNKNOWN {'<QUEUE>': 'localqueue', '<RAW_STATUS>': 'Z', '<WN>': 'localhost', 'command': '/bin/bash /usr/users/stober/grid-control/share/local.sh', 'cpu': '0.0', 'mem': '0.0', 'rss': '1244', 'start': '20:33', 'time': '0:00', 'tty': 'pts/62', 'user': 'stober', 'vsz': '63792'}
	STATUS=OK
	"""

class Test_JMS:
	"""
	>>> test_check('JMS_CheckJobs', wmsIDs = ['1036587'])
	STATUS=OK

	>>> test_check('JMS_CheckJobs', 'test.JMS.status')
	456808 RUNNING {'<QUEUE>': 'b', '<RAW_STATUS>': 'r', '<WN>': '1*003', 'cpu_time': '60', 'group': 'bd00', 'job_name': 'GCdc509df1.0', 'kill_time': '4/20:43', 'memory': '1000', 'nodes': '1/1/1', 'partition': 't', 'queue_time': '4/19:43', 'start_time': '4/19:43', 'user': 'bd105', 'wall_time': '60'}
	456809 RUNNING {'<QUEUE>': 'b', '<RAW_STATUS>': 'r', '<WN>': '1*030', 'cpu_time': '60', 'group': 'bd00', 'job_name': 'GCdc509df1.1', 'kill_time': '4/20:43', 'memory': '1000', 'nodes': '1/1/1', 'partition': 't', 'queue_time': '4/19:43', 'start_time': '4/19:43', 'user': 'bd105', 'wall_time': '60'}
	456810 WAITING {'<QUEUE>': 'b', '<RAW_STATUS>': 'w', 'cpu_time': '60', 'group': 'bd00', 'job_name': 'GCdc509df1.2', 'memory': '1000', 'nodes': '1/1/1', 'partition': 't', 'queue_time': '4/19:43', 'user': 'bd105', 'wall_time': '60'}
	456818 WAITING {'<QUEUE>': 'b', '<RAW_STATUS>': 'w', 'cpu_time': '60', 'group': 'bd00', 'job_name': 'GCdc509df1.10', 'memory': '1000', 'nodes': '1/1/1', 'partition': 't', 'queue_time': '4/19:43', 'user': 'bd105', 'wall_time': '60'}
	456819 WAITING {'<QUEUE>': 'b', '<RAW_STATUS>': 'w', 'cpu_time': '60', 'group': 'bd00', 'job_name': 'GCdc509df1.11', 'memory': '1000', 'nodes': '1/1/1', 'partition': 't', 'queue_time': '4/19:43', 'user': 'bd105', 'wall_time': '60'}
	STATUS=OK
	"""

class Test_LSF:
	"""
	>>> test_check('LSF_CheckJobs', wmsIDs = ['1036587'])
	STATUS=OK

	>>> test_check('LSF_CheckJobs', 'test.LSF.status1')
	103506916 QUEUED {'<QUEUE>': '8nm', '<RAW_STATUS>': 'PEND', '<WN>': '-', 'from': 'lxplus235', 'job_name': 'GC81e0208d.0', 'submit_time': 'Nov 4 20:07', 'user': 'stober'}
	103506921 QUEUED {'<QUEUE>': '8nm', '<RAW_STATUS>': 'PEND', '<WN>': '-', 'from': 'lxplus235', 'job_name': 'GC81e0208d.3', 'submit_time': 'Nov 4 20:07', 'user': 'stober'}
	103506923 QUEUED {'<QUEUE>': '8nm', '<RAW_STATUS>': 'PEND', '<WN>': '-', 'from': 'lxplus235', 'job_name': 'GC81e0208d.4', 'submit_time': 'Nov 4 20:07', 'user': 'stober'}
	103606918 RUNNING {'<QUEUE>': '8nm', '<RAW_STATUS>': 'RUN', '<WN>': 'lxbsu0606', 'from': 'lxplus235', 'job_name': 'GC81e0208d.7', 'submit_time': 'Nov 4 20:07', 'user': 'stober'}
	103606919 RUNNING {'<QUEUE>': '8nm', '<RAW_STATUS>': 'RUN', '<WN>': 'lxbsu0647', 'from': 'lxplus235', 'job_name': 'GC81e0208d.8', 'submit_time': 'Nov 4 20:07', 'user': 'stober'}
	103606921 RUNNING {'<QUEUE>': '8nm', '<RAW_STATUS>': 'RUN', '<WN>': 'lxbsq1446', 'from': 'lxplus235', 'job_name': 'GC81e0208d.9', 'submit_time': 'Nov 4 20:07', 'user': 'stober'}
	103606923 DONE {'<QUEUE>': '8nm', '<RAW_STATUS>': 'DONE', '<WN>': 'lxbsq0620', 'from': 'lxplus235', 'job_name': 'GC81e0208d.5', 'submit_time': 'Nov 4 20:07', 'user': 'stober'}
	103507052 QUEUED {'<QUEUE>': '8nm', '<RAW_STATUS>': 'PEND', '<WN>': '-', 'from': 'lxplus235', 'job_name': 'GC81e0208d.10', 'submit_time': 'Nov 4 20:09', 'user': 'stober'}
	103507054 QUEUED {'<QUEUE>': '8nm', '<RAW_STATUS>': 'PEND', '<WN>': '-', 'from': 'lxplus235', 'job_name': 'GC81e0208d.11', 'submit_time': 'Nov 4 20:09', 'user': 'stober'}
	103507056 QUEUED {'<QUEUE>': '8nm', '<RAW_STATUS>': 'PEND', '<WN>': '-', 'from': 'lxplus235', 'job_name': 'GC81e0208d.13', 'submit_time': 'Nov 4 20:09', 'user': 'stober'}
	STATUS=OK

	>>> try_catch(lambda: test_check('LSF_CheckJobs', 'test.LSF.status2'), 'BackendError', 'Unable to parse job info')
	caught
	"""

class Test_PBS:
	"""
	>>> test_check('PBS_CheckJobs', wmsIDs = ['1036587'])
	STATUS=OK

	>>> test_check('PBS_CheckJobs', 'test.PBS.status')
	1036587 QUEUED {'<QUEUE>': 'VM_SLC5', '<RAW_STATUS>': 'Q', 'checkpoint': 'u', 'ctime': 'Thu Nov  4 19:40:24 2010', 'error_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0000.qbNZnY/gc.stderr', 'etime': 'Thu Nov  4 19:40:24 2010', 'fault_tolerant': 'False', 'hold_types': 'n', 'job_name': 'GCdc509df1.0', 'job_owner': 'bd105@ic1n991.localdomain', 'join_path': 'n', 'keep_files': 'n', 'mail_points': 'a', 'mtime': 'Thu Nov  4 19:40:24 2010', 'output_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0000.qbNZnY/gc.stdout', 'priority': 0, 'qtime': 'Thu Nov  4 19:40:24 2010', 'rerunable': 'True', 'resource_list.nice': 0, 'server': 'ic-pbs.localdomain', 'submit_args': '-N GCdc509df1.0 -q VM_SLC5 -v GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0000.qbNZnY -o /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0000.qbNZnY/gc.stdout -e /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0000.qbNZnY/gc.stderr /pfs/data/home/kit/bd00/bd105/grid-control/share/local.sh', 'variable_list': 'PBS_O_HOME=/home/ws/bd105,PBS_O_LANG=en_US.UTF-8,PBS_O_LOGNAME=bd105,PBS_O_PATH=/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/all/bin:/opt/intel/Compiler/11.1/073/bin/intel64:/opt/intel/Compiler/11.1/073/bin:/jms/bin:/home/ws/bd105/bin:/usr/local/bin:/usr/bin:/bin:/usr/bin/X11:/usr/X11R6/bin:/usr/games:/opt/kde3/bin:/usr/lib64/jvm/jre/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:.:/opt/openmpi/1.4.3/bin:/software/ssck/bin,PBS_O_MAIL=/var/mail/bd105,PBS_O_SHELL=/bin/bash,PBS_O_HOST=ic1n991.localdomain,PBS_SERVER=ic-pbs.localdomain,PBS_O_WORKDIR=/pfs/data/home/kit/bd00/bd105/grid-control/docs,GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0000.qbNZnY,PBS_O_QUEUE=VM_SLC5'}
	1036571 RUNNING {'<QUEUE>': 'VM_SLC5', '<RAW_STATUS>': 'R', '<WN>': 'ic1n027.ic-pbs.localdomain', 'checkpoint': 'u', 'ctime': 'Thu Nov  4 19:40:07 2010', 'error_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0001.__tx2d/gc.stderr', 'etime': 'Thu Nov  4 19:40:07 2010', 'fault_tolerant': 'False', 'hold_types': 'n', 'job_name': 'GCdc509df1.1', 'job_owner': 'bd105@ic1n991.localdomain', 'join_path': 'n', 'keep_files': 'n', 'mail_points': 'a', 'mtime': 'Thu Nov  4 19:40:10 2010', 'output_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0001.__tx2d/gc.stdout', 'priority': 0, 'qtime': 'Thu Nov  4 19:40:07 2010', 'rerunable': 'True', 'resource_list.nice': 0, 'server': 'ic-pbs.localdomain', 'session_id': 9107, 'start_count': 1, 'start_time': 'Thu Nov  4 19:40:07 2010', 'submit_args': '-N GCdc509df1.1 -q VM_SLC5 -v GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0001.__tx2d -o /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0001.__tx2d/gc.stdout -e /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0001.__tx2d/gc.stderr /pfs/data/home/kit/bd00/bd105/grid-control/share/local.sh', 'variable_list': 'PBS_O_HOME=/home/ws/bd105,PBS_O_LANG=en_US.UTF-8,PBS_O_LOGNAME=bd105,PBS_O_PATH=/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/all/bin:/opt/intel/Compiler/11.1/073/bin/intel64:/opt/intel/Compiler/11.1/073/bin:/jms/bin:/home/ws/bd105/bin:/usr/local/bin:/usr/bin:/bin:/usr/bin/X11:/usr/X11R6/bin:/usr/games:/opt/kde3/bin:/usr/lib64/jvm/jre/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:.:/opt/openmpi/1.4.3/bin:/software/ssck/bin,PBS_O_MAIL=/var/mail/bd105,PBS_O_SHELL=/bin/bash,PBS_O_HOST=ic1n991.localdomain,PBS_SERVER=ic-pbs.localdomain,PBS_O_WORKDIR=/pfs/data/home/kit/bd00/bd105/grid-control/docs,GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0001.__tx2d,PBS_O_QUEUE=VM_SLC5'}
	1036589 QUEUED {'<QUEUE>': 'VM_SLC5', '<RAW_STATUS>': 'Q', 'checkpoint': 'u', 'ctime': 'Thu Nov  4 19:40:24 2010', 'error_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0017.8gGyXF/gc.stderr', 'etime': 'Thu Nov  4 19:40:24 2010', 'fault_tolerant': 'False', 'hold_types': 'n', 'job_name': 'GCdc509df1.17', 'job_owner': 'bd105@ic1n991.localdomain', 'join_path': 'n', 'keep_files': 'n', 'mail_points': 'a', 'mtime': 'Thu Nov  4 19:40:24 2010', 'output_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0017.8gGyXF/gc.stdout', 'priority': 0, 'qtime': 'Thu Nov  4 19:40:24 2010', 'rerunable': 'True', 'resource_list.nice': 0, 'server': 'ic-pbs.localdomain', 'submit_args': '-N GCdc509df1.17 -q VM_SLC5 -v GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0017.8gGyXF -o /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0017.8gGyXF/gc.stdout -e /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0017.8gGyXF/gc.stderr /pfs/data/home/kit/bd00/bd105/grid-control/share/local.sh', 'variable_list': 'PBS_O_HOME=/home/ws/bd105,PBS_O_LANG=en_US.UTF-8,PBS_O_LOGNAME=bd105,PBS_O_PATH=/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/all/bin:/opt/intel/Compiler/11.1/073/bin/intel64:/opt/intel/Compiler/11.1/073/bin:/jms/bin:/home/ws/bd105/bin:/usr/local/bin:/usr/bin:/bin:/usr/bin/X11:/usr/X11R6/bin:/usr/games:/opt/kde3/bin:/usr/lib64/jvm/jre/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:.:/opt/openmpi/1.4.3/bin:/software/ssck/bin,PBS_O_MAIL=/var/mail/bd105,PBS_O_SHELL=/bin/bash,PBS_O_HOST=ic1n991.localdomain,PBS_SERVER=ic-pbs.localdomain,PBS_O_WORKDIR=/pfs/data/home/kit/bd00/bd105/grid-control/docs,GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0017.8gGyXF,PBS_O_QUEUE=VM_SLC5'}
	1036590 QUEUED {'<QUEUE>': 'VM_SLC5', '<RAW_STATUS>': 'Q', 'checkpoint': 'u', 'ctime': 'Thu Nov  4 19:40:24 2010', 'error_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0018.fJtP_K/gc.stderr', 'etime': 'Thu Nov  4 19:40:24 2010', 'fault_tolerant': 'False', 'hold_types': 'n', 'job_name': 'GCdc509df1.18', 'job_owner': 'bd105@ic1n991.localdomain', 'join_path': 'n', 'keep_files': 'n', 'mail_points': 'a', 'mtime': 'Thu Nov  4 19:40:24 2010', 'output_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0018.fJtP_K/gc.stdout', 'priority': 0, 'qtime': 'Thu Nov  4 19:40:24 2010', 'rerunable': 'True', 'resource_list.nice': 0, 'server': 'ic-pbs.localdomain', 'submit_args': '-N GCdc509df1.18 -q VM_SLC5 -v GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0018.fJtP_K -o /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0018.fJtP_K/gc.stdout -e /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0018.fJtP_K/gc.stderr /pfs/data/home/kit/bd00/bd105/grid-control/share/local.sh', 'variable_list': 'PBS_O_HOME=/home/ws/bd105,PBS_O_LANG=en_US.UTF-8,PBS_O_LOGNAME=bd105,PBS_O_PATH=/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/all/bin:/opt/intel/Compiler/11.1/073/bin/intel64:/opt/intel/Compiler/11.1/073/bin:/jms/bin:/home/ws/bd105/bin:/usr/local/bin:/usr/bin:/bin:/usr/bin/X11:/usr/X11R6/bin:/usr/games:/opt/kde3/bin:/usr/lib64/jvm/jre/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:.:/opt/openmpi/1.4.3/bin:/software/ssck/bin,PBS_O_MAIL=/var/mail/bd105,PBS_O_SHELL=/bin/bash,PBS_O_HOST=ic1n991.localdomain,PBS_SERVER=ic-pbs.localdomain,PBS_O_WORKDIR=/pfs/data/home/kit/bd00/bd105/grid-control/docs,GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0018.fJtP_K,PBS_O_QUEUE=VM_SLC5'}
	1036536 RUNNING {'<QUEUE>': 'short', '<RAW_STATUS>': 'R', '<WN>': 'ic1n006.ic-pbs.localdomain', 'checkpoint': 'u', 'ctime': 'Thu Nov  4 19:38:27 2010', 'error_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0010.gXQPSk/gc.stderr', 'etime': 'Thu Nov  4 19:38:27 2010', 'fault_tolerant': 'False', 'hold_types': 'n', 'job_name': 'GCdc509df1.10', 'job_owner': 'bd105@ic1n991.localdomain', 'join_path': 'n', 'keep_files': 'n', 'mail_points': 'a', 'mtime': 'Thu Nov  4 19:38:28 2010', 'output_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0010.gXQPSk/gc.stdout', 'priority': 0, 'qtime': 'Thu Nov  4 19:38:27 2010', 'rerunable': 'True', 'resource_list.cput': '01:00:00', 'resource_list.nice': 0, 'resource_list.walltime': '03:05:00', 'server': 'ic-pbs.localdomain', 'session_id': 21361, 'start_count': 1, 'start_time': 'Thu Nov  4 19:38:28 2010', 'submit_args': '-N GCdc509df1.10 -q short -v GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0010.gXQPSk -o /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0010.gXQPSk/gc.stdout -e /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0010.gXQPSk/gc.stderr /pfs/data/home/kit/bd00/bd105/grid-control/share/local.sh', 'variable_list': 'PBS_O_HOME=/home/ws/bd105,PBS_O_LANG=en_US.UTF-8,PBS_O_LOGNAME=bd105,PBS_O_PATH=/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/all/bin:/opt/intel/Compiler/11.1/073/bin/intel64:/opt/intel/Compiler/11.1/073/bin:/jms/bin:/home/ws/bd105/bin:/usr/local/bin:/usr/bin:/bin:/usr/bin/X11:/usr/X11R6/bin:/usr/games:/opt/kde3/bin:/usr/lib64/jvm/jre/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:.:/opt/openmpi/1.4.3/bin:/software/ssck/bin,PBS_O_MAIL=/var/mail/bd105,PBS_O_SHELL=/bin/bash,PBS_O_HOST=ic1n991.localdomain,PBS_SERVER=ic-pbs.localdomain,PBS_O_WORKDIR=/pfs/data/home/kit/bd00/bd105/grid-control/docs,GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0010.gXQPSk,PBS_O_QUEUE=short', 'walltime.remaining': 1108}
	1036537 RUNNING {'<QUEUE>': 'short', '<RAW_STATUS>': 'R', '<WN>': 'ic1n006.ic-pbs.localdomain', 'checkpoint': 'u', 'ctime': 'Thu Nov  4 19:38:28 2010', 'error_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0011.NBfyUT/gc.stderr', 'etime': 'Thu Nov  4 19:38:28 2010', 'fault_tolerant': 'False', 'hold_types': 'n', 'job_name': 'GCdc509df1.11', 'job_owner': 'bd105@ic1n991.localdomain', 'join_path': 'n', 'keep_files': 'n', 'mail_points': 'a', 'mtime': 'Thu Nov  4 19:38:28 2010', 'output_path': 'ic1n991:/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0011.NBfyUT/gc.stdout', 'priority': 0, 'qtime': 'Thu Nov  4 19:38:28 2010', 'rerunable': 'True', 'resource_list.cput': '01:00:00', 'resource_list.nice': 0, 'resource_list.walltime': '03:05:00', 'server': 'ic-pbs.localdomain', 'session_id': 21363, 'start_count': 1, 'start_time': 'Thu Nov  4 19:38:28 2010', 'submit_args': '-N GCdc509df1.11 -q short -v GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0011.NBfyUT -o /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0011.NBfyUT/gc.stdout -e /pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0011.NBfyUT/gc.stderr /pfs/data/home/kit/bd00/bd105/grid-control/share/local.sh', 'variable_list': 'PBS_O_HOME=/home/ws/bd105,PBS_O_LANG=en_US.UTF-8,PBS_O_LOGNAME=bd105,PBS_O_PATH=/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/kit/bd00/pbs/torque/bin:/software/kit/bd00/tools/bin:/home/ws/bd105/bin:/software/all/bin:/opt/intel/Compiler/11.1/073/bin/intel64:/opt/intel/Compiler/11.1/073/bin:/jms/bin:/home/ws/bd105/bin:/usr/local/bin:/usr/bin:/bin:/usr/bin/X11:/usr/X11R6/bin:/usr/games:/opt/kde3/bin:/usr/lib64/jvm/jre/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:.:/opt/openmpi/1.4.3/bin:/software/ssck/bin,PBS_O_MAIL=/var/mail/bd105,PBS_O_SHELL=/bin/bash,PBS_O_HOST=ic1n991.localdomain,PBS_SERVER=ic-pbs.localdomain,PBS_O_WORKDIR=/pfs/data/home/kit/bd00/bd105/grid-control/docs,GC_SANDBOX=/pfs/data/home/kit/bd00/bd105/grid-control/docs/work.USERMOD-parameter/sandbox/GCdc509df15ea2.0011.NBfyUT,PBS_O_QUEUE=short', 'walltime.remaining': 1108}
	STATUS=OK
	"""

class Test_SLURM:
	"""
	"""

run_test()
