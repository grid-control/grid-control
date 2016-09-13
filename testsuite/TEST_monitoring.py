#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import create_config, create_workflow, run_test, str_dict_testsuite
from grid_control.job_db import Job
from grid_control.monitoring import Monitoring, MultiMonitor


class TestMonitor(Monitoring):
	def _display(self, args):
		print(repr(args))

	def onJobSubmit(self, wms, jobObj, jobnum):
		self._display(['onJobSubmit', jobnum, Job.enum2str(jobObj.state)])

	def onJobUpdate(self, wms, jobObj, jobnum, data):
		self._display(['onJobUpdate', jobnum, Job.enum2str(jobObj.state), data])

	def onJobOutput(self, wms, jobObj, jobnum, retCode):
		self._display(['onJobOutput', jobnum, Job.enum2str(jobObj.state), retCode])

	def onTaskFinish(self, nJobs):
		self._display(['onTaskFinish', nJobs])

	def onFinish(self):
		self._display(['onFinish'])

	def getScript(self):
		return ['mon.test1.sh', 'mon.test2.sh']

	def get_task_dict(self):
		return {'key': 'value'}

	def getFiles(self):
		return ['mon.support.dat']


class Test_Monitoring:
	"""
	>>> config = create_config()
	>>> workflow = create_workflow({'jobs': {'jobs': 1}}, abort = 'task')
	>>> m1 = TestMonitor(config, 'm1', workflow.task)
	>>> m2 = Monitoring(config, 'm2', workflow.task)
	>>> m = MultiMonitor(config, 'mon', [m1, m2], workflow.task)
	>>> m.onJobSubmit(None, Job(), 123)
	['onJobSubmit', 123, 'INIT']
	>>> m.onJobUpdate(None, Job(), 123, {'key': 'value'})
	['onJobUpdate', 123, 'INIT', {'key': 'value'}]
	>>> m.onJobOutput(None, Job(), 123, 321)
	['onJobOutput', 123, 'INIT', 321]
	>>> m.onTaskFinish(42)
	['onTaskFinish', 42]
	>>> m.onFinish()
	['onFinish']
	>>> m.getScript()
	['mon.test1.sh', 'mon.test2.sh']
	>>> print(str_dict_testsuite(m.get_task_dict()))
	{'GC_MONITORING': 'mon.test1.sh mon.test2.sh', 'key': 'value'}
	>>> m.getFiles()
	['mon.support.dat', 'mon.test1.sh', 'mon.test2.sh']
	"""

run_test()
