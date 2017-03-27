#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test, str_dict_testsuite, testfwk_create_workflow
from grid_control.job_db import Job
from grid_control.monitoring import Monitoring, MultiMonitor


class TestMonitor(Monitoring):
	def _display(self, args):
		print(repr(args))

	def on_job_submit(self, wms, job_obj, jobnum):
		self._display(['on_job_submit', jobnum, Job.enum2str(job_obj.state)])

	def on_job_update(self, wms, job_obj, jobnum, data):
		self._display(['on_job_update', jobnum, Job.enum2str(job_obj.state), data])

	def on_job_output(self, wms, job_obj, jobnum, exit_code):
		self._display(['on_job_output', jobnum, Job.enum2str(job_obj.state), exit_code])

	def on_task_finish(self, job_len):
		self._display(['on_task_finish', job_len])

	def on_workflow_finish(self):
		self._display(['on_workflow_finish'])

	def get_script(self):
		return ['mon.test1.sh', 'mon.test2.sh']

	def get_task_dict(self):
		return {'key': 'value'}

	def get_file_list(self):
		return ['mon.support.dat']


class Test_Monitoring:
	"""
	>>> config = create_config()
	>>> workflow = testfwk_create_workflow({'jobs': {'jobs': 1}})
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: ---
	>>> m1 = TestMonitor(config, 'm1', workflow.task)
	>>> m2 = Monitoring(config, 'm2', workflow.task)
	>>> m = MultiMonitor(config, 'mon', [m1, m2], workflow.task)
	>>> m.on_job_submit(None, Job(), 123)
	['on_job_submit', 123, 'INIT']
	>>> m.on_job_update(None, Job(), 123, {'key': 'value'})
	['on_job_update', 123, 'INIT', {'key': 'value'}]
	>>> m.on_job_output(None, Job(), 123, 321)
	['on_job_output', 123, 'INIT', 321]
	>>> m.on_task_finish(42)
	['on_task_finish', 42]
	>>> m.on_workflow_finish()
	['on_workflow_finish']
	>>> m.get_script()
	['mon.test1.sh', 'mon.test2.sh']
	>>> print(str_dict_testsuite(m.get_task_dict()))
	{'GC_MONITORING': 'mon.test1.sh mon.test2.sh', 'key': 'value'}
	>>> m.get_file_list()
	['mon.support.dat', 'mon.test1.sh', 'mon.test2.sh']
	"""

run_test()
