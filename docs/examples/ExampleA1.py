#!/usr/bin/env python

if 'Settings' in locals():  # Throw exceptions if the file is executed in the wrong way
	raise Exception('This file is supposed to be run directly by python - not by go.py!')
try:
	from grid_control_api import gc_create_config, gc_create_workflow
except ImportError:  # .. or if grid-control is incorrectly installed
	raise Exception('grid-control is not correctly installed ' +
		'or the gc package directory is not part of the PYTHONPATH.')

# Setup workflow
config = gc_create_config(config_dict={
	'interactive': {'default': False},
	'global': {'backend': 'Host', 'task': 'UserTask'},
	'task': {'executable': 'Example02_local.sh'},
	'jobs': {'wall time': '1:00', 'jobs': 2},
})

# Create and run workflow with GUI output
workflow = gc_create_workflow(config)
workflow.run(duration=-1)
