#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testfwk').setup(__file__)
# - prolog marker
import os, time, signal, logging
from testfwk import TestsuiteStream, create_config, run_test, str_dict_testsuite, testfwk_create_workflow, testfwk_remove_files, testsuite_display_job_obj_list, try_catch
from grid_control.datasets import DataProvider
from grid_control.report import Report
from grid_control.utils.activity import Activity
from testDS import create_source
from python_compat import irange


class ReportFilter(logging.Filter):
	def filter(self, record):
		return 'job' in record.msg.lower()


logging.Filter(name='')
logging.getLogger('console.report').addFilter(ReportFilter())
Activity.root.finish()


def checkTask(workflow):
	workflow.job_manager.check(workflow.task, None)
	task = workflow.task
	print('max: %s' % task.get_job_len())
	def displayJobConfig(jobnum):
		msg = '%3d' % jobnum
		if not task.can_submit(jobnum):
			msg += '! '
		else:
			msg += ': '
		job_config = task.get_job_dict(jobnum)
		for key in ['DATASETBLOCK', 'DATASETNICK', 'DATASETPATH']:
			job_config.pop(key, None)
		print(msg + str_dict_testsuite(job_config))
	if task.get_job_len() is None:
		displayJobConfig(0)
		displayJobConfig(123)
	else:
		for jobnum in irange(task.get_job_len()):
			displayJobConfig(jobnum)

files_list = ['data_resync*.dbs', 'work/jobs/*', 'work/*', 'work']

def local_testfwk_create_workflow(settings):
	return testfwk_create_workflow(user_config_dict=settings)

def display_report(workflow):
	stream = TestsuiteStream()
	report = Report.create_instance('BasicReport', create_config(), 'test', workflow.job_manager.job_db, workflow.task)
	report.show_report(workflow.job_manager.job_db, workflow.job_manager.job_db.get_job_list())

def do_resync_test(settings, resync_time=0, report=True):
	workflow = local_testfwk_create_workflow(settings)
	print('____')

	def display():
		checkTask(workflow)
		print('____')
		testsuite_display_job_obj_list(workflow.job_manager.job_db)
		if report:
			print('____')
			display_report(workflow)

	display()
	if resync_time:
		time.sleep(resync_time)
		print('****')
		display()
	return workflow

class Test_TaskResync:
	"""
	>>> testfwk_remove_files(files_list)
	>>> dummy = do_resync_test({'jobs': {'jobs': 1}})
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	max: None
	  0: {'GC_JOB_ID': 0, 'GC_PARAM': 0}
	123: {'GC_JOB_ID': 123, 'GC_PARAM': 123}
	____
	0 <no stored data>
	____
	Total number of jobs:        1     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       1  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%

	>>> DataProvider.save_to_file('data_resync.dbs', create_source([('/PRIVATE/DS#1', {'file1': 1, 'file2': 1})]).get_block_list_cached(show_stats=False))
	>>> dummy = do_resync_test({'task': {'dataset': 'data_resync.dbs', 'files per job': 1}})
	 * Dataset 'data_resync.dbs':
	  contains 1 block with 2 files with 2 entries
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	max: 2
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	____
	Total number of jobs:        2     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       2  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%

	>>> DataProvider.save_to_file('data_resync.dbs', create_source([('/PRIVATE/DS#1', {'file1': 1, 'file2': 1, 'file3': 1})]).get_block_list_cached(show_stats=False))
	>>> dummy = do_resync_test({'task': {'dataset': 'data_resync.dbs', 'files per job': 1, 'dataset refresh': '0:00:01', 'dataset default query interval': '00:00:01'}}, 1)
	Dataset source 'dataset' will be queried every 0h 00min 01sec
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	max: 2
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	____
	Total number of jobs:        2     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       2  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%
	****
	Finished resync of parameter source (XX:XX:XX)
	0000-00-00 00:00:00 - The task module has requested changes to the job database
	0000-00-00 00:00:00 - Number of jobs changed from 2 to 3
	0000-00-00 00:00:00 - All requested changes are applied
	max: 3
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  2: {'DATASETSPLIT': 2, 'FILE_NAMES': 'file3', 'GC_JOB_ID': 2, 'GC_PARAM': 2, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	2 <no stored data>
	____
	Total number of jobs:        3     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       3  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%

	>>> DataProvider.save_to_file('data_resync.dbs', create_source([('/PRIVATE/DS#1', {'file1': 1, 'file2': 1})]).get_block_list_cached(show_stats=False))
	>>> dummy = do_resync_test({'task': {'dataset': 'data_resync.dbs', 'files per job': 1, 'dataset refresh': '0:00:01', 'dataset default query interval': '00:00:01'}}, 2)
	Dataset source 'dataset' will be queried every 0h 00min 01sec
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	max: 3
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  2: {'DATASETSPLIT': 2, 'FILE_NAMES': 'file3', 'GC_JOB_ID': 2, 'GC_PARAM': 2, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	2 <no stored data>
	____
	Total number of jobs:        3     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       3  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%
	****
	Finished resync of parameter source (XX:XX:XX)
	0000-00-00 00:00:00 - The task module has requested changes to the job database
	0000-00-00 00:00:00 - Job 2 state changed from INIT to DISABLED
	0000-00-00 00:00:00 - All requested changes are applied
	max: 3
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  2! {'DATASETSPLIT': 2, 'FILE_NAMES': '', 'GC_JOB_ID': 2, 'GC_PARAM': 2, 'MAX_EVENTS': '', 'SKIP_EVENTS': ''}
	____
	0 <no stored data>
	1 <no stored data>
	2 DISABLED <no id>
	____
	Total number of jobs:        3     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       2   67%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       1   33%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%

	>>> DataProvider.save_to_file('data_resync.dbs', create_source([('/PRIVATE/DS#1', {'file1': 1, 'file2': 1, 'file4': 1})]).get_block_list_cached(show_stats=False))
	>>> workflow = do_resync_test({'task': {'dataset': 'data_resync.dbs', 'files per job': 1, 'dataset refresh': '0:00:01', 'dataset default query interval': '00:00:01'}})
	Dataset source 'dataset' will be queried every 0h 00min 01sec
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	Stopped reading job infos at job #2 out of 1 available job files, since the limit of 2 jobs is reached
	____
	max: 2
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	____
	Total number of jobs:        2     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       2  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%
	>>> os.kill(os.getpid(), signal.SIGUSR2)
	External signal triggered resync of datasource 'data'
	>>> checkTask(workflow)
	Finished resync of parameter source (XX:XX:XX)
	0000-00-00 00:00:00 - The task module has requested changes to the job database
	0000-00-00 00:00:00 - Number of jobs changed from 2 to 3
	0000-00-00 00:00:00 - All requested changes are applied
	max: 3
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  2: {'DATASETSPLIT': 2, 'FILE_NAMES': 'file4', 'GC_JOB_ID': 2, 'GC_PARAM': 2, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	>>> display_report(workflow)
	Total number of jobs:        3     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       3  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%
	"""

class Test_TaskResyncDataset:
	"""
	>>> testfwk_remove_files(files_list)
	>>> DataProvider.save_to_file('data_resync0.dbs', create_source([]).get_block_list_cached(show_stats=False))
	>>> DataProvider.save_to_file('data_resync1.dbs', create_source([('/PRIVATE/DSA#1', {'file1a': 1, 'file2a': 1})]).get_block_list_cached(show_stats=False))
	>>> DataProvider.save_to_file('data_resync2.dbs', create_source([('/PRIVATE/DSB#1', {'file1b': 1, 'file2b': 1})]).get_block_list_cached(show_stats=False))

	>>> dummy = do_resync_test({'jobs': {'jobs': 1}}, report=False)
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	max: None
	  0: {'GC_JOB_ID': 0, 'GC_PARAM': 0}
	123: {'GC_JOB_ID': 123, 'GC_PARAM': 123}
	____
	0 <no stored data>

	>>> try_catch(lambda: local_testfwk_create_workflow({'task': {'dataset': 'data_resync0.dbs', 'files per job': 1}}), 'UserError', 'does not provide jobs to process')
	 * Dataset 'data_resync0.dbs':
	  contains nothing!
	caught

	>>> workflow_store = do_resync_test({'task': {'dataset': 'data_resync0.dbs', 'files per job': 1, 'dataset refresh': '1:00'}})
	Dataset source 'dataset' will be queried every 1h 00min 00sec
	Dataset source 'dataset' does not provide jobs to process
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	max: 0
	____
	____
	Total number of jobs:        0     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       0    0%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%
	>>> workflow_store.testsuite_config.factory.freeze()

	>>> dummy = do_resync_test({'task': {'dataset': 'data_resync1.dbs', 'files per job': 1, 'dataset refresh': '0:01'}})
	The config option '[task] dataset' was changed
	Triggering resync of datasets, parameters
	The configuration was changed - triggering storage of new config options
	Dataset source 'dataset' will be queried every 0h 01min 00sec
	Dataset source 'dataset' does not provide jobs to process
	Parameter source requested resync
	Finished resync of parameter source (XX:XX:XX)
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	0000-00-00 00:00:00 - The task module has requested changes to the job database
	max: 2
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1a', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2a', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	____
	Total number of jobs:        2     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       2  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%

	>>> dummy = do_resync_test({'task': {'dataset': 'data_resync1.dbs\\ndata_resync2.dbs', 'files per job': 1, 'dataset processor +': 'sort', 'dataset file sort': True, 'dataset block sort': True}})
	The config option '[task] dataset' was changed
	Triggering resync of datasets, parameters
	The configuration was changed - triggering storage of new config options
	The config option '<task> dataset processor' was changed
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	0000-00-00 00:00:00 - The task module has requested changes to the job database
	max: 4
	  0: {'DATASETSPLIT': 0, 'FILE_NAMES': 'file1a', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATASETSPLIT': 1, 'FILE_NAMES': 'file2a', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  2: {'DATASETSPLIT': 2, 'FILE_NAMES': 'file1b', 'GC_JOB_ID': 2, 'GC_PARAM': 2, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  3: {'DATASETSPLIT': 3, 'FILE_NAMES': 'file2b', 'GC_JOB_ID': 3, 'GC_PARAM': 3, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	2 <no stored data>
	3 <no stored data>
	____
	Total number of jobs:        4     Successful jobs:       0    0%
	Jobs being processed:        0        Failing jobs:       0    0%
	Jobs       INIT:       4  100%     Jobs  SUBMITTED:       0    0%
	Jobs   DISABLED:       0    0%     Jobs      READY:       0    0%
	Jobs    WAITING:       0    0%     Jobs     QUEUED:       0    0%
	Jobs    ABORTED:       0    0%     Jobs    RUNNING:       0    0%
	Jobs     CANCEL:       0    0%     Jobs    UNKNOWN:       0    0%
	Jobs  CANCELLED:       0    0%     Jobs       DONE:       0    0%
	Jobs     FAILED:       0    0%     Jobs    SUCCESS:       0    0%
	"""

class Test_TaskResyncMultiDataset:
	"""
	>>> testfwk_remove_files(files_list)
	>>> DataProvider.save_to_file('data_resync1.dbs', create_source([('/PRIVATE/DSA#1', {'file1a': 1})]).get_block_list_cached(show_stats=False))
	>>> DataProvider.save_to_file('data_resync2.dbs', create_source([('/PRIVATE/DSB#1', {'file2a': 1})]).get_block_list_cached(show_stats=False))

	>>> dummy = do_resync_test({'task': {'files per job': 1, 'datasource names': 'data1 data2', 'data1': 'data_resync1.dbs', 'data2': 'data_resync2.dbs'}}, report=False)
	 * Dataset 'data_resync1.dbs':
	  contains 1 block with 1 file with 1 entry
	 * Dataset 'data_resync2.dbs':
	  contains 1 block with 1 file with 1 entry
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	max: 2
	  0: {'DATA1BLOCK': '1', 'DATA1NICK': 'DSA', 'DATA1PATH': '/PRIVATE/DSA', 'DATA1SPLIT': 'data1:0', 'DATA2BLOCK': '', 'DATA2NICK': '', 'DATA2PATH': '', 'DATA2SPLIT': '', 'FILE_NAMES': 'file1a', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATA1BLOCK': '', 'DATA1NICK': '', 'DATA1PATH': '', 'DATA1SPLIT': '', 'DATA2BLOCK': '1', 'DATA2NICK': 'DSB', 'DATA2PATH': '/PRIVATE/DSB', 'DATA2SPLIT': 'data2:0', 'FILE_NAMES': 'file2a', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>

	>>> DataProvider.save_to_file('data_resync1.dbs', create_source([('/PRIVATE/DSA#1', {'file1a': 1, 'file1b': 1})]).get_block_list_cached(show_stats=False))
	>>> DataProvider.save_to_file('data_resync2.dbs', create_source([('/PRIVATE/DSB#1', {'file2a': 1, 'file2b': 1})]).get_block_list_cached(show_stats=False))
	>>> dummy = do_resync_test({'task': {'files per job': 1, 'datasource names': 'data1 data2', 'data1': 'data_resync1.dbs', 'data2': 'data_resync2.dbs', 'data1 refresh': '0:00:01', 'data1 default query interval': '00:00:01'}}, 1, report=False)
	Dataset source 'data1' will be queried every 0h 00min 01sec
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	____
	max: 2
	  0: {'DATA1BLOCK': '1', 'DATA1NICK': 'DSA', 'DATA1PATH': '/PRIVATE/DSA', 'DATA1SPLIT': 'data1:0', 'DATA2BLOCK': '', 'DATA2NICK': '', 'DATA2PATH': '', 'DATA2SPLIT': '', 'FILE_NAMES': 'file1a', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATA1BLOCK': '', 'DATA1NICK': '', 'DATA1PATH': '', 'DATA1SPLIT': '', 'DATA2BLOCK': '1', 'DATA2NICK': 'DSB', 'DATA2PATH': '/PRIVATE/DSB', 'DATA2SPLIT': 'data2:0', 'FILE_NAMES': 'file2a', 'GC_JOB_ID': 1, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	****
	Finished resync of parameter source (XX:XX:XX)
	0000-00-00 00:00:00 - The task module has requested changes to the job database
	0000-00-00 00:00:00 - Number of jobs changed from 2 to 3
	0000-00-00 00:00:00 - All requested changes are applied
	max: 3
	  0: {'DATA1BLOCK': '1', 'DATA1NICK': 'DSA', 'DATA1PATH': '/PRIVATE/DSA', 'DATA1SPLIT': 'data1:0', 'DATA2BLOCK': '', 'DATA2NICK': '', 'DATA2PATH': '', 'DATA2SPLIT': '', 'FILE_NAMES': 'file1a', 'GC_JOB_ID': 0, 'GC_PARAM': 0, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  1: {'DATA1BLOCK': '', 'DATA1NICK': '', 'DATA1PATH': '', 'DATA1SPLIT': '', 'DATA2BLOCK': '1', 'DATA2NICK': 'DSB', 'DATA2PATH': '/PRIVATE/DSB', 'DATA2SPLIT': 'data2:0', 'FILE_NAMES': 'file2a', 'GC_JOB_ID': 1, 'GC_PARAM': 2, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	  2: {'DATA1BLOCK': '1', 'DATA1NICK': 'DSA', 'DATA1PATH': '/PRIVATE/DSA', 'DATA1SPLIT': 'data1:1', 'DATA2BLOCK': '', 'DATA2NICK': '', 'DATA2PATH': '', 'DATA2SPLIT': '', 'FILE_NAMES': 'file1b', 'GC_JOB_ID': 2, 'GC_PARAM': 1, 'MAX_EVENTS': 1, 'SKIP_EVENTS': 0}
	____
	0 <no stored data>
	1 <no stored data>
	2 <no stored data>
	"""

run_test(exit_fun=lambda: testfwk_remove_files(files_list))
