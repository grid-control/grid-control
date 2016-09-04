#!/usr/bin/env python

import logging

# Throw exceptions if the file is executed in the wrong way or grid-control is not correctly installed
if 'Settings' in locals():
	raise Exception('This file is supposed to be run directly by python - not by go.py!')
try:
	from gcSettings import Settings
	from gcTool import gc_create_config, gc_create_workflow
except ImportError:
	raise Exception('grid-control is not correctly installed or the gc package directory is not part of the PYTHONPATH.')

setup = Settings()
setup.Global.report = 'null'
setup.Global.backend = 'Host'
setup.Global.task = 'UserTask'
setup.Global.duration = -1
setup.jobs.jobs = 1
setup.jobs.wall_time = 1
setup.task.executable = 'Example02_local.sh'
setup.interactive.default = False

config = gc_create_config(configDict = setup.getConfigDict())
logging.getLogger().setLevel(logging.CRITICAL)
logging.getLogger('jobs').setLevel(logging.INFO)
workflow = gc_create_workflow(config)
workflow.run()
