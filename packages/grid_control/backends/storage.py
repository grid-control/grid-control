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

import os, shutil
from grid_control import utils
from grid_control.config import ConfigError, NoVarCheck
from grid_control.gc_plugin import NamedPlugin
from grid_control.utils.activity import Activity
from grid_control.utils.process_base import LocalProcess
from hpfwk import NestedException
from python_compat import imap, set


# All functions use url_* functions from gc-run.lib (just like the job did...)


class StorageError(NestedException):
	pass


def se_copy(src, dst, force=True, tmp=''):
	cmd = 'url_copy_single'
	if force:
		cmd = 'url_copy_single_force'
	return se_runcmd('print_and_eval "%s"' % cmd, {'GC_KEEPTMP': tmp}, src, dst)


def se_exists(target):
	return se_runcmd('print_and_eval "url_exists"', {}, target)


def se_ls(target):
	return se_runcmd('url_ls', {}, target)


def se_mkdir(target):
	return se_runcmd('print_and_eval "url_mkdir"', {}, target)


def se_rm(target):
	return se_runcmd('print_and_eval "url_rm"', {}, target)


def se_runcmd(cmd, env_dict, *urls):
	lib_fn = utils.get_path_share('gc-run.lib')
	se_path_iter = imap(lambda x: '"%s"' % _ensure_se_prefix(x).replace('dir://', 'file://'), urls)
	env_str = str.join(' ', imap(lambda x: 'export %s="%s";' % (x, env_dict[x]), env_dict))
	exec_str = '. %s || exit 99; %s %s %s' % (lib_fn, env_str, cmd, str.join(' ', se_path_iter))
	return LocalProcess('/bin/bash', '-c', exec_str)


class StorageManager(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['storage']
	config_tag_name = 'storage'

	def __init__(self, config, name, storage_type, storage_channel, storage_var_prefix):
		NamedPlugin.__init__(self, config, name)
		(self._storage_channel, self._storage_var_prefix) = (storage_channel, storage_var_prefix)

	def add_file_list(self, files):
		pass

	def do_transfer(self, desc_source_target_list):
		pass

	def get_dependency_list(self):
		return []

	def get_task_dict(self):
		return {}


class LocalSBStorageManager(StorageManager):
	def __init__(self, config, name, storage_type, storage_channel, storage_var_prefix):
		StorageManager.__init__(self, config, name, storage_type, storage_channel, storage_var_prefix)
		self._sandbox_path = config.get_path('%s path' % storage_type,
			config.get_work_path('sandbox'), must_exist=False)

	def do_transfer(self, desc_source_target_list):
		for (desc, source, target) in desc_source_target_list:
			target = os.path.join(self._sandbox_path, target)
			try:
				shutil.copy(source, target)
			except Exception:
				raise StorageError('Unable to transfer %s "%s" to "%s"!' % (desc, source, target))


class SEStorageManager(StorageManager):
	def __init__(self, config, name, storage_type, storage_channel, storage_var_prefix):
		StorageManager.__init__(self, config, name, storage_type, storage_channel, storage_var_prefix)

		def _norm_se_path(se_path):
			if se_path[0] == '/':
				return 'dir:///%s' % se_path.lstrip('/')
			return se_path

		self._storage_paths = config.get_list(['%s path' % storage_type, '%s path' % storage_channel],
			default=[], on_valid=NoVarCheck(config), parse_item=_norm_se_path)
		self._storage_files = config.get_list('%s files' % storage_channel, [])
		self._storage_pattern = config.get('%s pattern' % storage_channel, '@X@')
		self._storage_timeout = config.get_time('%s timeout' % storage_channel, 2 * 60 * 60)
		self._storage_force = config.get_bool('%s force' % storage_channel, True)

	def add_file_list(self, files):
		self._storage_files.extend(files)

	def do_transfer(self, desc_source_target_list):
		for (desc, source, target) in desc_source_target_list:
			if not self._storage_paths:
				raise ConfigError("%s can't be transferred because '%s path wasn't set" % (desc,
					self._storage_channel))
			for idx, se_path in enumerate(set(self._storage_paths)):
				activity = Activity('Copy %s to SE %d ' % (desc, idx + 1))
				proc = se_copy(source, os.path.join(se_path, target), self._storage_force)
				proc.status(timeout=5 * 60, terminate=True)
				activity.finish()
				if proc.status(timeout=0) == 0:
					self._log.info('Copy %s to SE %d finished', desc, idx + 1)
				else:
					self._log.info('Copy %s to SE %d failed', desc, idx + 1)
					self._log.critical(proc.stderr.read(timeout=0))
					self._log.critical('Unable to copy %s! You can try to copy it manually.', desc)
					if not utils.get_user_bool('Is %s (%s) available on SE %s?' % (desc, source, se_path), False):
						raise StorageError('%s is missing on SE %s!' % (desc, se_path))

	def get_dependency_list(self):
		if True in imap(lambda x: not x.startswith('dir'), self._storage_paths):
			return ['glite']
		return []

	def get_task_dict(self):
		return {
			'%s_PATH' % self._storage_var_prefix: str.join(' ', self._storage_paths),
			'%s_FILES' % self._storage_var_prefix: str.join(' ', self._storage_files),
			'%s_PATTERN' % self._storage_var_prefix: self._storage_pattern,
			'%s_TIMEOUT' % self._storage_var_prefix: self._storage_timeout,
		}


def _ensure_se_prefix(fn):
	if '://' in fn:
		return fn
	return 'file:////%s' % os.path.abspath(fn).lstrip('/')
