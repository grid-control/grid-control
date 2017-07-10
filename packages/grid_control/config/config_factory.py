# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

import os, sys, logging
from grid_control.config.cfiller_base import ConfigFiller, GeneralFileConfigFiller
from grid_control.config.cinterface_typed import SimpleConfigInterface
from grid_control.config.config_entry import ConfigContainer, ConfigError
from grid_control.config.cview_base import SimpleConfigView
from grid_control.gc_exceptions import GCLogHandler
from grid_control.utils import ensure_dir_exists, get_file_name, resolve_path
from grid_control.utils.data_structures import UniqueList
from grid_control.utils.file_tools import SafeFile, with_file
from python_compat import lfilter


def create_config(config_file=None, config_dict=None, use_default_files=False,
		additional=None, register=False, path_base=None,
		load_old_config=True, load_only_old_config=False, **kwargs):
	filler_list = []
	if use_default_files:
		filler_list.append(ConfigFiller.create_instance('DefaultFilesConfigFiller'))
	if config_file:
		filler_list.append(GeneralFileConfigFiller([config_file]))
	config_dict = config_dict or kwargs.get('configDict')
	if config_dict:
		filler_list.append(ConfigFiller.create_instance('DictConfigFiller', config_dict))
	filler_list.extend(additional or [])
	filler = ConfigFiller.create_instance('MultiConfigFiller', filler_list)
	config = ConfigFactory(filler, config_file, load_old_config, path_base).get_config()
	if load_only_old_config:
		config = create_config(config_file=os.path.join(config.get_work_path(), 'work.conf'),
			use_default_files=False, load_old_config=False, path_base=config_file, register=register)
		config.factory.freeze(write_config=False, show_unused=False, raise_on_change=False)
		return config
	if register:
		GCLogHandler.config_instances.append(config)
	return config


class ConfigFactory(object):
	# Main config interface
	def __init__(self, filler=None, config_file_path=None, load_old_config=True, path_base=None):
		def _get_name(prefix=''):
			if config_file_path:
				return ('%s.%s' % (prefix, get_file_name(config_file_path))).strip('.')
			elif prefix:
				return prefix
			return 'unnamed'

		try:
			config_dn = os.getcwd()
		except Exception:
			raise ConfigError('The current directory does not exist!')
		if config_file_path:
			config_dn = os.path.dirname(resolve_path(config_file_path,
				search_path_list=[os.getcwd()], exception_type=ConfigError))
		if path_base:
			config_dn = os.path.dirname(resolve_path(path_base, search_path_list=[os.getcwd()]))

		# Init config containers
		self._container_cur = ConfigContainer('current')
		if filler:  # Read in the current configuration ...
			filler.fill(self._container_cur)
		self._container_cur.resolve()  # resolve interpolations

		logging.getLogger('config.stored').propagate = False
		container_old = ConfigContainer('stored')
		container_old.enabled = False

		# Create config view and temporary config interface
		self._view = SimpleConfigView(_get_name(), container_old, self._container_cur)
		self._view.config_vault['path:search'] = UniqueList([os.getcwd(), config_dn])

		# Determine work directory using config interface with "global" scope
		tmp_config = SimpleConfigInterface(self._view.get_view(set_sections=['global']))
		work_dn_base = tmp_config.get_dn('workdir base', config_dn, must_exist=False)
		work_dn_default = os.path.join(work_dn_base, _get_name('work'))
		work_dn = tmp_config.get_dn('workdir', work_dn_default, must_exist=False)
		self._view.config_vault['path:work_dn'] = work_dn  # tmp_config still has undefinied
		# Set dynamic plugin search path
		sys.path.extend(tmp_config.get_dn_list('plugin paths', [os.getcwd()]))

		# Determine and load stored config settings
		self._config_path_min = os.path.join(work_dn, 'current.conf')  # Minimal config file
		self._config_path_old = os.path.join(work_dn, 'work.conf')  # Config file with saved settings
		if load_old_config:
			if os.path.exists(self._config_path_old):
				GeneralFileConfigFiller([self._config_path_old]).fill(container_old)
			old_setting_file = os.path.join(work_dn, 'task.dat')
			if os.path.exists(old_setting_file):
				ConfigFiller.create_instance('CompatConfigFiller', old_setting_file).fill(container_old)
			container_old.enabled = True
			container_old.protect()

		# Get persistent variables - only possible after container_old was enabled
		self._view.set_config_name(tmp_config.get('config id', _get_name(), persistent=True))

	def freeze(self, write_config=True, show_unused=True, raise_on_change=True):
		# Inform the user about unused options
		def _match_unused_entries(entry):
			return ('!' not in entry.section) and not entry.accessed
		self._container_cur.protect(raise_on_change)
		if show_unused:
			unused = lfilter(_match_unused_entries, self._view.iter_entries())
			log = logging.getLogger('config.freeze')
			if unused:
				log.log(logging.INFO1, 'There are %s unused config options!', len(unused))
			for entry in unused:
				log.log(logging.INFO1, '\t%s', entry.format(print_section=True))
		if write_config or not os.path.exists(self._config_path_old):
			ensure_dir_exists(os.path.dirname(self._config_path_old),
				'config storage directory', ConfigError)
			# Write user friendly, flat config file and config file with saved settings
			self._write_file(self._config_path_min, print_minimal=True, print_default=False,
				print_workdir=True, print_unused=False)
			self._write_file(self._config_path_old, print_minimal=True, print_default=True,
				print_source=True, print_unused=True,
				msg='; ==> DO NOT EDIT THIS FILE! <==\n; This file is used to find config changes!\n')

	def get_config(self):
		result = SimpleConfigInterface(self._view)
		result.factory = self
		return result

	def _write_file(self, fn, msg=None, **kwargs):
		def _write_msg_view(fp):
			if msg is not None:
				fp.write(msg)
			self._view.write(fp, **kwargs)
		with_file(SafeFile(fn, 'w'), _write_msg_view)
