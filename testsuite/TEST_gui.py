#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
import sys
from testFwk import cleanup_workflow, create_config, create_workflow, run_test, try_catch
from grid_control.gui import GUI
from grid_control.report import Report
from grid_control_gui.ansi import Console
from python_compat import StringBuffer, lfilter


class Test_GUI:
	"""
	>>> config = create_config()
	>>> workflow = create_workflow({'jobs': {'jobs': 1}}, abort = 'jobmanager')
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: HOST
	>>> gui = GUI.create_instance('GUI', config, workflow)
	>>> try_catch(lambda: gui.displayWorkflow(workflow), 'AbstractError', 'is an abstract function')
	caught
	"""

def test_report(*args):
	old_stdout = sys.stdout
	buffer = StringBuffer()
	sys.stdout = buffer
	Report.create_instance('BasicReport', *args).show_report(args[0])
	sys.stdout = old_stdout
	return lfilter(lambda l: l.startswith('REPORT SUMMARY'), buffer.getvalue().splitlines())

class Test_Report:
	"""
	>>> workflow = create_workflow({'jobs': {'jobs': 1}}, abort = 'jobmanager')
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: HOST

	>>> try_catch(lambda: Report.create_instance('Report', workflow.jobManager.jobDB).show_report(workflow.jobManager.jobDB), 'AbstractError', 'is an abstract function')
	caught

	>>> test_report(workflow.jobManager.jobDB)
	['REPORT SUMMARY:                                                  ']

	>>> test_report(workflow.jobManager.jobDB, workflow.task)
	['REPORT SUMMARY:                          unnamed / GC000000000000']

	>>> workflow = create_workflow({'jobs': {'jobs': 1}, 'global': {'config id': 'very long and descriptive config name'}}, abort = 'jobmanager')
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: HOST
	>>> test_report(workflow.jobManager.jobDB, workflow.task)
	['REPORT SUMMARY:             very long and descriptive config name']

	>>> workflow = create_workflow({'jobs': {'jobs': 1}, 'global': {'config id': 'incredibly and way too long and descriptive config name'}}, abort = 'jobmanager')
	Current task ID: GC000000000000
	Task started on: 0000-00-00
	Using batch system: HOST
	>>> test_report(workflow.jobManager.jobDB, workflow.task)
	['REPORT SUMMARY:                                    GC000000000000']
	"""


class Test_ANSIGUI:
	"""
	>>> Console.fmt('Hello', [Console.COLOR_BLUE])
	'Hello'
	>>> len(Console.fmt('Hello', [Console.COLOR_BLUE], force_ansi = True))
	5
	>>> len(str(Console.fmt('Hello', [Console.COLOR_BLUE], force_ansi = True)))
	16
	>>> tmp = str(Console.fmt('Hello', [Console.COLOR_BLUE], force_ansi = True))
	>>> tmp
	'\\x1b[0;34mHello\\x1b[0m'
	>>> Console.fmt_strip(tmp)
	'Hello'
	"""

run_test(cleanup_fun = cleanup_workflow)
