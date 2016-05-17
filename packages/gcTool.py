#!/usr/bin/env python
# | Copyright 2007-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, sys, signal, logging
from grid_control import utils
from grid_control.config import createConfig
from grid_control.gc_exceptions import gc_excepthook
from grid_control.logging_setup import logging_setup
from grid_control.utils.cmd_options import Options
from hpfwk import ExceptionFormatter, Plugin, handle_debug_interrupt

# grid-control command line parser
def gc_cmd_line_parser(cmd_line_args):
	parser = Options(usage = '%s [OPTIONS] <config file>', add_help_option = False)
	parser.addBool(None, ' ', 'debug',         default = False)
	parser.addBool(None, ' ', 'help-conf',     default = False)
	parser.addBool(None, ' ', 'help-confmin',  default = False)
	parser.addBool(None, 'c', 'continuous',    default = False)
	parser.addBool(None, 'h', 'help',          default = False)
	parser.addBool(None, 'i', 'init',          default = False)
	parser.addBool(None, 'q', 'resync',        default = False)
	parser.addBool(None, 's', 'no-submission', default = True,  dest = 'submission')
	parser.addBool(None, 'G', 'gui',           default = False, dest = 'gui_ansi')
	parser.addBool(None, 'W', 'webserver',     default = False, dest = 'gui_cp')
	parser.addAccu(None, 'v', 'verbose')
	parser.addList(None, 'l', 'logging')
	parser.addList(None, 'o', 'override')
	parser.addText(None, ' ', 'action')
	parser.addText(None, 'd', 'delete')
	parser.addText(None, 'J', 'job-selector')
	parser.addText(None, 'm', 'max-retry')
	parser.addText(None, ' ', 'reset')
	# Deprecated options - refer to new report script instead
	for (sopt, lopt) in [('-r', 'report'), ('-R', 'site-report'), ('-T', 'time-report'),
			('-M', 'task-report'), ('-D', 'detail-report'), ('', 'help-vars')]:
		parser.addBool(None, sopt, lopt, default = False, dest = 'old_report')

	(opts, args, _) = parser.parse(args = cmd_line_args)
	opts.gui = None
	if opts.gui_ansi:
		opts.gui = 'ANSIGUI'
	elif opts.gui_cp:
		opts.gui = 'CPWebserver'
	opts.continuous = opts.continuous or None # either True or None
	# Display help
	if opts.help:
		sys.stderr.write('%s\n\n%s\n' % (parser.usage(), open(utils.pathShare('help.txt'), 'r').read()))
		sys.exit(os.EX_USAGE)
	# Require single config file argument
	if len(args) == 0:
		utils.exitWithUsage(parser.usage(), 'Config file not specified!')
	elif len(args) > 1:
		utils.exitWithUsage(parser.usage(), 'Invalid command line arguments: %r' % cmd_line_args)
	# Warn about deprecated report options
	if opts.old_report:
		utils.deprecated('Please use the more versatile report tool in the scripts directory!')
	# Configure preliminary logging
	utils.verbosity(opts.verbose)
	logging.getLogger().setLevel(max(1, logging.DEFAULT - opts.verbose))
	if opts.debug: # Setup initial debug handler before it is reconfigured by logging_setup
		handler = logging.StreamHandler(sys.stdout)
		handler.setFormatter(ExceptionFormatter(showCodeContext = 2, showVariables = 1, showFileStack = 1))
		logging.getLogger('exception').addHandler(handler)

	return (opts, args)

# Config filler which collects data from command line arguments
class OptsConfigFiller(Plugin.getClass('ConfigFiller')):
	def __init__(self, cmd_line_args):
		self._cmd_line_args = cmd_line_args

	def fill(self, container):
		combinedEntry = container.getEntry('cmdargs', lambda entry: entry.section == 'global')
		newCmdLine = self._cmd_line_args
		if combinedEntry:
			newCmdLine = combinedEntry.value.split() + self._cmd_line_args
		(opts, _) = gc_cmd_line_parser(newCmdLine)
		def setConfigFromOpt(section, option, value):
			if value is not None:
				self._addEntry(container, section, option, str(value), '<cmdline>')
		cmd_line_config_map = {
			'state!': { '#init': opts.init, '#resync': opts.resync,
				'#display config': opts.help_conf, '#display minimal config': opts.help_confmin },
			'action': { 'delete': opts.delete, 'reset': opts.reset },
			'global': { 'gui': opts.gui, 'submission': opts.submission },
			'jobs': { 'max retry': opts.max_retry, 'selected': opts.job_selector },
			'logging': { 'debug mode': opts.debug },
		}
		for section in cmd_line_config_map:
			for (option, value) in cmd_line_config_map[section].items():
				setConfigFromOpt(section, option, value)
		for entry in opts.logging:
			tmp = entry.replace(':', '=').split('=')
			if len(tmp) == 1:
				tmp.append('DEBUG')
			setConfigFromOpt('logging', tmp[0] + ' level', tmp[1])
		if opts.action is not None:
			setConfigFromOpt('workflow', 'action', opts.action.replace(',', ' '))
		if opts.continuous:
			setConfigFromOpt('workflow', 'duration', -1)
		Plugin.createInstance('StringConfigFiller', opts.override).fill(container)

# create config instance
def gc_create_config(cmd_line_args):
	(_, args) = gc_cmd_line_parser(cmd_line_args)
	return createConfig(configFile = args[0], additional = [OptsConfigFiller(cmd_line_args)])

# set up signal handler for interrupts
def handle_abort_interrupt(sig, frame):
	utils.abort(True)
	handle_abort_interrupt.log = utils.ActivityLog('Quitting grid-control! (This can take a few seconds...)')
	signal.signal(signal.SIGINT, signal.SIG_DFL)

# create workflow from config and do initial processing steps
def gc_create_workflow(config):
	# Configure logging settings
	logging_setup(config.changeView(setSections = ['logging']))

	global_config = config.changeView(setSections = ['global'])
	# Check work dir validity (default work directory is the config file name)
	if not os.path.exists(global_config.getWorkPath()):
		if not global_config.getState('init'):
			logging.getLogger('user').warning('Starting initialization of %s!', global_config.getWorkPath())
			global_config.setState(True, 'init')
		if global_config.getChoiceYesNo('workdir create', True,
				interactive = 'Do you want to create the working directory %s?' % global_config.getWorkPath()):
			utils.ensureDirExists(global_config.getWorkPath(), 'work directory')

	# Query config settings before config is frozen
	help_cfg = global_config.getState('display', detail = 'config')
	help_scfg = global_config.getState('display', detail = 'minimal config')

	action_config = config.changeView(setSections = ['action'])
	action_delete = action_config.get('delete', '', onChange = None)
	action_reset = action_config.get('reset', '', onChange = None)

	# Create workflow and freeze config settings
	workflow = global_config.getPlugin('workflow', 'Workflow:global', cls = 'Workflow')
	config.factory.freezeConfig(writeConfig = config.getState('init', detail = 'config'))

	# Give config help
	if help_cfg or help_scfg:
		config.write(sys.stdout, printDefault = help_cfg, printUnused = False,
			printMinimal = help_scfg, printSource = help_cfg)
		sys.exit(os.EX_OK)

	# Check if user requested deletion / reset of jobs
	if action_delete:
		workflow.jobManager.delete(workflow.wms, action_delete)
		sys.exit(os.EX_OK)
	if action_reset:
		workflow.jobManager.reset(workflow.wms, action_reset)
		sys.exit(os.EX_OK)

	return workflow

def run():
	# set up signal handler for interrupts and debug session requests
	signal.signal(signal.SIGINT, handle_abort_interrupt)
	signal.signal(signal.SIGURG, handle_debug_interrupt)

	# display the 'grid-control' logo and version
	sys.stdout.write(open(utils.pathShare('logo.txt'), 'r').read())
	sys.stdout.write('Revision: %s\n' % utils.getVersion())
	pyver = (sys.version_info[0], sys.version_info[1])
	if pyver < (2, 3):
		utils.deprecated('This python version (%d.%d) is not supported anymore!' % pyver)

	# main try... except block to catch exceptions and show error message
	try:
		config = gc_create_config(sys.argv[1:])
		workflow = gc_create_workflow(config)
		sys.exit(workflow.run())
	except SystemExit:
		pass
	except Exception: # coverage overrides sys.excepthook
		gc_excepthook(*sys.exc_info())
		sys.exit(os.EX_SOFTWARE)

if __name__ == '__main__':
	run()
