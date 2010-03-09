#!/usr/bin/env python
import gcSupport, sys, os, optparse

# big try... except block to catch exceptions and print error message
try:
	# try to open config file
	try:
		open(args[0], 'r')
		config = Config(args[0])
	except:
		raise ConfigError("Error while reading configuration file!")

	config.opts = config
	config.opts.seed = '0'

	# Check work dir validity (default work directory is the config file name)
	config.workDir = config.getPath('global', 'workdir', config.workDirDefault)
	os.chdir(config.workDir)

	# Initialise application module
	module = config.get('global', 'module')
	module = Module.open(module, config, TrivialProxy())

	# Initialise job database
	jobs = JobDB(config, module)
	Report(jobs, jobs).summary()

except GridError, e:
	e.showMessage()
