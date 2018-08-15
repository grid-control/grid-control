# | Copyright 2010-2017 Karlsruhe Institute of Technology
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

import os, logging
from grid_control.config import ConfigError, TriggerInit
from grid_control.tasks.task_user import UserTask
from grid_control.utils import Result, get_path_share
from grid_control.utils.algos import dict_union


class ROOTTask(UserTask):
	alias_list = ['ROOTMod', 'root']
	config_section_list = UserTask.config_section_list + ['ROOTMod', 'ROOTTask']

	def __init__(self, config, name):
		# Determine ROOT path from previous settings / environment / config file
		def _check_root_dn(loc, obj):
			if os.path.isdir(obj):
				return obj
			raise ConfigError('Either set environment variable "ROOTSYS" or set option "root path"!')
		self._root_dn = config.get_dn('root path', os.environ.get('ROOTSYS', ''),
			persistent=True, on_change=TriggerInit('sandbox'), on_valid=_check_root_dn)
		logging.getLogger('task').info('Using the following ROOT path: %s', self._root_dn)

		# Special handling for executables bundled with ROOT
		self._executable = config.get('executable', on_change=TriggerInit('sandbox'))
		exe_full = os.path.join(self._root_dn, 'bin', self._executable.lstrip('/'))
		self._is_builtin = os.path.exists(exe_full)
		if self._is_builtin:
			config.set('send executable', 'False')
			# store resolved built-in executable path?

		# Apply default handling from UserTask
		UserTask.__init__(self, config, name)
		self._update_map_error_code2msg(get_path_share('gc-run.root.sh'))

		# TODO: Collect lib files needed by executable
		self._lib_fn_list = []

	def get_command(self):
		cmd = './gc-run.root.sh %s $@ > job.stdout 2> job.stderr' % self._executable
		if self._is_builtin:
			return cmd
		return 'chmod u+x %s; %s' % (self._executable, cmd)

	def get_sb_in_fpi_list(self):
		return UserTask.get_sb_in_fpi_list(self) + self._lib_fn_list + [
			Result(path_abs=get_path_share('gc-run.root.sh'), path_rel='gc-run.root.sh')]

	def get_task_dict(self):
		return dict_union(UserTask.get_task_dict(self), {'GC_ROOTSYS': self._root_dn})
