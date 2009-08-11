#!/usr/bin/env python
import sys, os, signal, optparse, gcSupport

# and include grid_control python module
from grid_control import *
from time import sleep
utils.verbosity.setting = 0

def main(args):
	# big try... except block to catch exceptions and print error message
	try:
		# try to open config file
		try:
			open(args[0], 'r')
			config = Config(args[0])
		except IOError, e:
			raise ConfigError("Error while reading configuration file '%s'!" % args[0])

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
		return 1

	return 0

# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
