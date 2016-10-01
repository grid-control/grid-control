# | Copyright 2014-2016 Karlsruhe Institute of Technology
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
from grid_control.utils.file_objects import SafeFile
from python_compat import lfilter


def create_config(config_file=None, config_dict=None, use_default_files=False,
		additional=None, register=False):
	filler_list = []
	if use_default_files:
		filler_list.append(ConfigFiller.create_instance('DefaultFilesConfigFiller'))
	if config_file:
		filler_list.append(GeneralFileConfigFiller([config_file]))
	if config_dict:
		filler_list.append(ConfigFiller.create_instance('DictConfigFiller', config_dict))
	filler_list.extend(additional or [])
	filler = ConfigFiller.create_instance('MultiConfigFiller', filler_list)
	config = ConfigFactory(filler, config_file).get_config()
	if register:
		GCLogHandler.config_instances.append(config)
	return config


class ConfigFactory(object):
	# Main config interface
	def __init__(self, filler=None, config_file_path=None):
		def _get_name(prefix=''):
			if config_file_path:
				return ('%s.%s' % (prefix, get_file_name(config_file_path))).strip('.')
			elif prefix:
				return prefix
			return 'unnamed'

		try:
			path_main = os.getcwd()
		except Exception:
			raise ConfigError('The current directory does not exist!')
		if config_file_path:
			path_main = os.path.dirname(resolve_path(config_file_path,
				search_path_list=[os.getcwd()], exception_type=ConfigError))

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
		self._view.config_vault['path:search'] = UniqueList([os.getcwd(), path_main])

		# Determine work directory using config interface with "global" scope
		tmp_config = SimpleConfigInterface(self._view.get_view(setSections=['global']))
		workdir_base = tmp_config.get_path('workdir base', path_main, must_exist=False)
		workdir_default = os.path.join(workdir_base, _get_name('work'))
		workdir_path = tmp_config.get_path('workdir', workdir_default, must_exist=False)
		self._view.config_vault['path:workdir'] = workdir_path  # tmp_config still has undefinied
		# Set dynamic plugin search path
		sys.path.extend(tmp_config.get_path_list('plugin paths', [os.getcwd()]))

		# Determine and load stored config settings
		self._config_path_min = os.path.join(workdir_path, 'current.conf')  # Minimal config file
		self._config_path_old = os.path.join(workdir_path, 'work.conf')  # Config file with saved settings
		filler_list_old = []
		if os.path.exists(self._config_path_old):
			filler_list_old.append(GeneralFileConfigFiller([self._config_path_old]))
		old_persistency_file = os.path.join(workdir_path, 'task.dat')
		if os.path.exists(old_persistency_file):
			filler_list_old.append(ConfigFiller.create_instance('CompatConfigFiller', old_persistency_file))
		for filler in filler_list_old:
			filler.fill(container_old)
		if filler_list_old:
			container_old.enabled = True
			container_old.set_read_only()

		# Get persistent variables - only possible after container_old was enabled
		self._view.set_config_name(tmp_config.get('config id', _get_name(), persistent=True))

	def freeze(self, write_config=True):
		# Inform the user about unused options
		def _match_unused_entries(entry):
			return ('!' not in entry.section) and not entry.accessed
		self._container_cur.set_read_only()
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
				message='; ==> DO NOT EDIT THIS FILE! <==\n; This file is used to find config changes!\n')

	def get_config(self):
		result = SimpleConfigInterface(self._view)
		result.factory = self
		return result

	def _write_file(self, fn, message=None, **kwargs):
		fp = SafeFile(fn, 'w')
		if message is not None:
			fp.write(message)
		self._view.write(fp, **kwargs)
		fp.close()
