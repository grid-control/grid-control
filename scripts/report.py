#!/usr/bin/env python
import gcSupport, sys, os, optparse
from grid_control import *
import grid_control.proxy

# big try... except block to catch exceptions and print error message
try:
	# try to open config file
	try:
		open(sys.argv[1], 'r')
		config = Config(sys.argv[1])
	except:
		raise ConfigError("Error while reading configuration file!")

	config.opts = config
	config.opts.seed = None
	config.opts.init = False
	config.opts.resync = False
	config.opts.maxRetry = 0

	# Check work dir validity (default work directory is the config file name)
	config.workDir = config.getPath('global', 'workdir', config.workDirDefault)

	# Initialise application module
	module = config.get('global', 'module')
	module = Module.open(module, config)

	# Initialise job database
	jobs = JobDB(config, module, None)
	Report(jobs, jobs).summary()

except GridError, e:
	e.showMessage()
