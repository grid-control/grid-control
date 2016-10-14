#!/usr/bin/env python

import logging

# Throw exceptions if the file is executed in the wrong way or grid-control is incorrectly installed
if 'Settings' in locals():
	raise Exception('This file is supposed to be run directly by python - not by go.py!')
try:
	from grid_control_api import gc_create_config, gc_create_workflow
	from grid_control_settings import Settings
except ImportError:
	raise Exception('grid-control is not correctly installed ' +
		'or the gc package directory is not part of the PYTHONPATH.')

setup = Settings()
setup.Global.report = 'null'
setup.Global.backend = 'Host'
setup.Global.task = 'UserTask'
setup.Global.duration = -1
setup.jobs.jobs = 1
setup.jobs.wall_time = 1
setup.task.executable = 'Example02_local.sh'
setup.interactive.default = False

config = gc_create_config(config_dict=setup.get_config_dict())
logging.getLogger().setLevel(logging.CRITICAL)
logging.getLogger('jobs').setLevel(logging.INFO)
workflow = gc_create_workflow(config)
workflow.run()
