#!/usr/bin/env python
#-#  Copyright 2007-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import sys, os, signal, optparse, time, logging

# Load grid-control package
sys.path.insert(1, os.path.join(sys.path[0], 'packages'))
from gcPackage import *

usage = 'Syntax: %s [OPTIONS] <config file>\n' % sys.argv[0]
def print_help(*args):
	utils.eprint('%s\n%s' % (usage, open(utils.pathShare('help.txt'), 'r').read()))
	sys.exit(0)

if __name__ == '__main__':
	global log, handler
	log = None

	# set up signal handler for interrupts
	def interrupt(sig, frame):
		global log, handler
		utils.abort(True)
		log = utils.ActivityLog('Quitting grid-control! (This can take a few seconds...)')
		signal.signal(signal.SIGINT, handler)
	handler = signal.signal(signal.SIGINT, interrupt)

	# display the 'grid-control' logo and version
	utils.vprint(open(utils.pathShare('logo.txt'), 'r').read(), -1)
	utils.vprint('Revision: %s' % utils.getVersion(), -1)
	pyver = sys.version_info[0] + sys.version_info[1] / 10.0
	if pyver < 2.3:
		utils.deprecated('This python version (%.1f) is not supported anymore!' % pyver)

	parser = optparse.OptionParser(add_help_option=False)
	parser.add_option('-h', '--help',          action='callback', callback=print_help)
	parser.add_option('',   '--help-conf',     dest='help_cfg',   default=False, action='store_true')
	parser.add_option('',   '--help-confmin',  dest='help_scfg',  default=False, action='store_true')
	parser.add_option('-i', '--init',          dest='init',       default=False, action='store_true')
	parser.add_option('-q', '--resync',        dest='resync',     default=False, action='store_true')
	parser.add_option('-P', '--python',        dest='python',     default=False, action='store_true')
	parser.add_option('-s', '--no-submission', dest='submission', default=True,  action='store_false')
	parser.add_option('-c', '--continuous',    dest='continuous', default=None,  action='store_true')
	parser.add_option('-o', '--override',      dest='override',   default=[],    action='append')
	parser.add_option('-d', '--delete',        dest='delete',     default=None)
	parser.add_option('',   '--reset',         dest='reset',      default=None)
	parser.add_option('-a', '--action',        dest='action',     default=None)
	parser.add_option('-J', '--job-selector',  dest='selector',   default=None)
	parser.add_option('-m', '--max-retry',     dest='maxRetry',   default=None,  type='int')
	parser.add_option('-v', '--verbose',       dest='verbosity',  default=0,     action='count')
	parser.add_option('-G', '--gui',           dest='gui',        action='store_const', const = 'ANSIGUI')
	parser.add_option('-W', '--webserver',     dest='gui',        action='store_const', const = 'CPWebserver')
	# Deprecated options - refer to new report script instead
	parser.add_option('-r', '--report',        dest='old_report', default=False, action='store_true')
	parser.add_option('-R', '--site-report',   dest='old_report', default=False, action='store_true')
	parser.add_option('-T', '--time-report',   dest='old_report', default=False, action='store_true')
	parser.add_option('-M', '--task-report',   dest='old_report', default=False, action='store_true')
	parser.add_option('-D', '--detail-report', dest='old_report', default=False, action='store_true')
	parser.add_option('',   '--help-vars',     dest='old_report', default=False, action='store_true')
	(opts, args) = parser.parse_args()

	utils.verbosity(opts.verbosity)
	logging.getLogger().setLevel(logging.DEFAULT_VERBOSITY - opts.verbosity)

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		utils.exitWithUsage(usage, 'Config file not specified!')
	if opts.old_report:
		utils.deprecated('Please use the more versatile report tool in the scripts directory!')

	# Config filler which collects data from command line arguments
	class OptsConfigFiller(ConfigFiller):
		def __init__(self, optParser):
			self._optParser = optParser

		def fill(self, container):
			defaultCmdLine = container.getEntry('global', 'cmdargs', '').value
			(opts, args) = self._optParser.parse_args(args = defaultCmdLine.split() + sys.argv[1:])
			def setConfigFromOpt(section, option, value):
				if value != None:
					container.setEntry(section, option, str(value), '<cmdline>')
			for (option, value) in {'max retry': opts.maxRetry, 'action': opts.action,
					'continuous': opts.continuous, 'selected': opts.selector}.items():
				setConfigFromOpt('jobs', option, value)
			setConfigFromOpt('global', 'gui', opts.gui)
			setConfigFromOpt('global', '#init', opts.init)
			setConfigFromOpt('global', '#resync', opts.resync)
			setConfigFromOpt('global', 'submission', opts.submission)
			StringConfigFiller(opts.override).fill(container)

	# big try... except block to catch exceptions and print error message
	def main():
		if opts.python:
			fcf = PythonConfigFiller(args[0])
		else:
			fcf = FileConfigFiller([args[0]])
		config = CompatConfig([DefaultFilesConfigFiller(), fcf, OptsConfigFiller(parser)], args[0])
			 # Apply override command line options
		config.opts = opts
		logging_setup(config.addSections(['logging']))

		# Check work dir validity (default work directory is the config file name)
		if not os.path.exists(config.getWorkPath()):
			if not config.getState():
				utils.vprint('Will force initialization of %s if continued!' % config.getWorkPath(), -1)
				config.setState(True)
			if utils.getUserBool('Do you want to create the working directory %s?' % config.getWorkPath(), True):
				utils.ensureDirExists(config.getWorkPath(), 'work directory')

		# Create workflow and freeze config settings
		workflow = config.getClass('global', 'workflow', 'Workflow:global', cls = Workflow).getInstance()
		config.freezeConfig(writeConfig = config.getState(detail = 'config'))

		# Give config help
		if opts.help_cfg or opts.help_scfg:
			config.write(sys.stdout, printDefault = opts.help_cfg, printUnused = False)
			sys.exit(0)

		# Check if user requested deletion / reset of jobs
		if opts.delete:
			workflow.jobManager.delete(workflow.wms, opts.delete)
			sys.exit(0)
		if opts.reset:
			workflow.jobManager.reset(workflow.wms, opts.reset)
			sys.exit(0)
		# Run the configured workflow
		workflow.run()

	handleException(main)
