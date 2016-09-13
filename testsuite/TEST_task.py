#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import create_config, remove_files_testsuite, run_test, try_catch
from hpfwk import Plugin


config_dict = {'jobs': {'wall time': '1'}, 'global': {'executable': 'TEST_task.py', }}
config = create_config(config_dict = config_dict)
task = Plugin.create_instance('UserTask', config, 'mytask')
task.taskID = 'GC12345678'
task.taskDate = '2099-01-01'

create_config(config_dict = {'global': {'job name': '@GC_TASK_DATE@ job_@GC_JOB_ID@'}})
class Test_JobName:
	"""
	>>> try_catch(lambda: Plugin.create_instance('JobNamePlugin', config).getName(task, 12), 'AbstractError', 'is an abstract function')
	caught
	>>> Plugin.create_instance('DefaultJobName', config).getName(task, 12)
	'GC12345678.12'
	>>> Plugin.create_instance('ConfigurableJobName', create_config(config_dict = {'global': {'job name': '@GC_TASK_DATE@ job_@GC_JOB_ID@'}})).getName(task, 12)
	'2099-01-01 job_12'
	>>> try_catch(lambda: Plugin.create_instance('ConfigurableJobName', create_config(config_dict = {'global': {'job name': '@GC_UNKNOWN@'}})).getName(task, 12), 'ConfigError', 'references unknown variables')
	caught
	"""

class Test_Task:
	"""
	>>> task1 = Plugin.create_instance('TaskModule', config, 'mytask')
	>>> try_catch(task1.get_command, 'AbstractError', 'is an abstract function')
	caught
	"""

run_test(cleanup_fun = lambda: remove_files_testsuite(['work/params.dat.gz', 'work/params.map.gz', 'work']))
