# | Copyright 2007-2017 Karlsruhe Institute of Technology
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
from grid_control.config import create_config
from grid_control.gc_exceptions import gc_excepthook
from grid_control.logging_setup import logging_setup, parse_logging_args
from grid_control.utils.activity import Activity
from grid_control.utils.cmd_options import Options
from grid_control.utils.file_objects import SafeFile
from hpfwk import Plugin, handle_debug_interrupt, init_hpf_plugins


# grid-control command line parser
def parse_cmd_line(cmd_line_args):
	parser = Options(usage='%s [OPTIONS] <config file>', add_help_option=False)
	parser.add_bool(None, ' ', 'debug', default=False)
	parser.add_bool(None, ' ', 'help-conf', default=False)
	parser.add_bool(None, ' ', 'help-confmin', default=False)
	parser.add_bool(None, 'c', 'continuous', default=False)
	parser.add_bool(None, 'h', 'help', default=False)
	parser.add_bool(None, 'i', 'init', default=False)
	parser.add_bool(None, 'q', 'resync', default=False)
	parser.add_bool(None, 's', 'no-submission', default=True, dest='submission')
	parser.add_bool(None, 'G', 'gui', default=False, dest='gui_ansi')
	parser.add_accu(None, 'v', 'verbose')
	parser.add_list(None, 'l', 'logging')
	parser.add_list(None, 'o', 'override')
	parser.add_text(None, ' ', 'action')
	parser.add_text(None, 'd', 'delete')
	parser.add_text(None, 'J', 'job-selector')
	parser.add_text(None, 'm', 'max-retry')
	parser.add_text(None, ' ', 'reset')
	# Deprecated options - refer to new report script instead
	for (sopt, lopt) in [('-r', 'report'), ('-R', 'site-report'), ('-T', 'time-report'),
			('-M', 'task-report'), ('-D', 'detail-report'), ('', 'help-vars')]:
		parser.add_bool(None, sopt, lopt, default=False, dest='old_report')

	(opts, args, _) = parser.parse(args=cmd_line_args)
	opts.gui = None
	if opts.gui_ansi:
		opts.gui = 'ANSIGUI'
	opts.continuous = opts.continuous or None  # either True or None
	# Display help
	if opts.help:
		utils.exit_with_usage(parser.usage(),
			SafeFile(utils.get_path_share('help.txt'), 'r').read(), show_help=False)
	# Require single config file argument
	if len(args) == 0:
		utils.exit_with_usage(parser.usage(), 'Config file not specified!')
	elif len(args) > 1:
		utils.exit_with_usage(parser.usage(), 'Invalid command line arguments: %r' % cmd_line_args)
	# Warn about deprecated report options
	if opts.old_report:
		utils.deprecated('Please use the more versatile report tool in the scripts directory!')
	# Configure preliminary logging
	logging.getLogger().setLevel(max(1, logging.DEFAULT - opts.verbose))
	return (opts, args)


# Config filler which collects data from command line arguments
class OptsConfigFiller(Plugin.get_class('ConfigFiller')):
	def __init__(self, cmd_line_args):
		self._cmd_line_args = cmd_line_args

	def fill(self, container):
		combined_entry = container.get_entry('cmdargs', lambda entry: entry.section == 'global')
		new_cmd_line = self._cmd_line_args
		if combined_entry:
			new_cmd_line = combined_entry.value.split() + self._cmd_line_args
		(opts, _) = parse_cmd_line(new_cmd_line)

		def set_config_from_opt(section, option, value):
			if value is not None:
				self._add_entry(container, section, option, str(value), '<cmdline>')  # pylint:disable=no-member
		cmd_line_config_map = {
			'state!': {'#init': opts.init, '#resync': opts.resync,
				'#display config': opts.help_conf, '#display minimal config': opts.help_confmin},
			'action': {'delete': opts.delete, 'reset': opts.reset},
			'global': {'gui': opts.gui, 'submission': opts.submission},
			'jobs': {'max retry': opts.max_retry, 'selected': opts.job_selector},
			'logging': {'debug mode': opts.debug},
		}
		for section in cmd_line_config_map:
			for (option, value) in cmd_line_config_map[section].items():
				set_config_from_opt(section, option, value)
		for (logger_name, logger_level) in parse_logging_args(opts.logging):
			set_config_from_opt('logging', logger_name + ' level', logger_level)
		if opts.action is not None:
			set_config_from_opt('workflow', 'action', opts.action.replace(',', ' '))
		if opts.continuous:
			set_config_from_opt('workflow', 'duration', -1)
		if opts.override:
			Plugin.create_instance('StringConfigFiller', opts.override).fill(container)


# create config instance
def gc_create_config(cmd_line_args=None, **kwargs):
	if cmd_line_args is not None:
		(_, args) = parse_cmd_line(cmd_line_args)
		kwargs.setdefault('config_file', args[0])
		kwargs.setdefault('additional', []).append(OptsConfigFiller(cmd_line_args))
	return create_config(register=True, **kwargs)


# set up signal handler for interrupts
def get_actions(config):
	action_delete = config.get('delete', '', on_change=None)
	action_reset = config.get('reset', '', on_change=None)
	return (action_delete, action_reset)


def handle_abort_interrupt(signum, frame):
	utils.abort(True)
	handle_abort_interrupt.log = Activity('Quitting grid-control! (This can take a few seconds...)',
		parent='root')
	signal.signal(signum, signal.SIG_DFL)


# create workflow from config and do initial processing steps
def gc_create_workflow(config, do_freeze=True, **kwargs):
	# set up signal handler for interrupts and debug session requests
	signal.signal(signal.SIGURG, handle_debug_interrupt)
	signal.signal(signal.SIGINT, handle_abort_interrupt)

	# Configure logging settings
	logging_setup(config.change_view(set_sections=['logging']))

	global_config = config.change_view(set_sections=['global'])
	# Check work dir validity (default work directory is the config file name)
	if not os.path.exists(global_config.get_work_path()):
		if not global_config.get_state('init'):
			log = logging.getLogger('workflow')
			log.warning('Starting initialization of %s!', global_config.get_work_path())
			global_config.set_state(True, 'init')
		work_dn_create_msg = 'Do you want to create the working directory %s?'
		if global_config.get_choice_yes_no('workdir create', True,
				interactive_msg=work_dn_create_msg % global_config.get_work_path()):
			utils.ensure_dir_exists(global_config.get_work_path(), 'work directory')
	for package_paths in global_config.get_path_list('package paths', [], on_change=None):
		init_hpf_plugins(package_paths)

	# Query config settings before config is frozen
	help_cfg = global_config.get_state('display', detail='config')
	help_scfg = global_config.get_state('display', detail='minimal config')

	(action_delete, action_reset) = get_actions(config.change_view(set_sections=['action']))

	# Create workflow and freeze config settings
	workflow = global_config.get_plugin('workflow', 'Workflow:global', cls='Workflow', pkwargs=kwargs)
	if do_freeze:
		config.factory.freeze(write_config=config.get_state('init', detail='config'))

	# Give config help
	if help_cfg or help_scfg:
		config.write(sys.stdout, print_default=help_cfg, print_unused=False,
			print_minimal=help_scfg, print_source=help_cfg)
		sys.exit(os.EX_OK)

	# Check if user requested deletion / reset of jobs
	if action_delete:
		workflow.job_manager.delete(workflow.task, workflow.wms, action_delete)
		sys.exit(os.EX_OK)
	if action_reset:
		workflow.job_manager.reset(workflow.task, workflow.wms, action_reset)
		sys.exit(os.EX_OK)

	return workflow


def gc_run(args=None, intro=True):
	# display the 'grid-control' logo and version
	if intro and not os.environ.get('GC_DISABLE_INTRO'):
		sys.stdout.write(SafeFile(utils.get_path_share('logo.txt'), 'r').read())
		sys.stdout.write('Revision: %s\n' % utils.get_version())
	pyver = (sys.version_info[0], sys.version_info[1])
	if pyver < (2, 3):
		utils.deprecated('This python version (%d.%d) is not supported anymore!' % pyver)
	Activity.root = Activity('Running grid-control', name='root')  # top level activity instance

	# main try... except block to catch exceptions and show error message
	try:
		config = gc_create_config(args or sys.argv[1:], use_default_files=True)
		workflow = gc_create_workflow(config)
		try:
			sys.exit(workflow.run())
		finally:
			sys.stdout.write('\n')
	except SystemExit:  # avoid getting caught for Python < 2.5
		raise
	except Exception:  # coverage overrides sys.excepthook
		gc_excepthook(*sys.exc_info())
		sys.exit(os.EX_SOFTWARE)
