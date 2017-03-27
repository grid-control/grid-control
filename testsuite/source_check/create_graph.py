#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import shutil
from testfwk import create_config, run_test, testfwk_create_workflow
from grid_control.workflow import Workflow
from grid_control_gui.plugin_graph import get_graph_image, get_workflow_graph


config_dict = {
	'global': {'task': 'UserTask', 'backend': 'Host'},
	'jobs': {'nseeds': 1},
	'task': {'wall time': '1', 'executable': 'create_graph.py', 'dataset': '../datasets/dataA.dbs', 'files per job': 1},
}

workflow = testfwk_create_workflow(config_dict)
del workflow.testsuite_config

open('devtool.dot', 'w').write(get_workflow_graph(workflow))
shutil.rmtree('work')

run_test()
