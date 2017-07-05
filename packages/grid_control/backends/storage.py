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
from grid_control.config import ConfigError, NoVarCheck
from grid_control.gc_plugin import NamedPlugin
from grid_control.utils import get_path_share
from grid_control.utils.activity import Activity
from grid_control.utils.file_tools import VirtualFile
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.user_interface import UserInputInterface
from hpfwk import NestedException
from python_compat import imap, set, sorted


# All functions use url_* functions from gc-run.lib (just like the job did...)


class StorageError(NestedException):
	pass


def se_copy(src, dst, force=True, tmp=''):
	env_dict = dict(os.environ)
	env_dict.update({'SC_KEEPTMP': tmp, 'SC_DEBUG': '1'})
	if not force:
		return _se_runcmd('copy', src, dst, env_dict=env_dict)
	return _se_runcmd('copy_overwrite', src, dst, env_dict=env_dict)


def se_exists(target):
	return _se_runcmd('exists', target)


def se_ls(target):
	return _se_runcmd('ls', target)


def se_mkdir(target):
	return _se_runcmd('mkdir', target)


def se_rm(target):
	return _se_runcmd('rm', target)


class StorageManager(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['storage']
	config_tag_name = 'storage'

	def __init__(self, config, name, storage_type, storage_channel):
		NamedPlugin.__init__(self, config, name)
		self._storage_channel = storage_channel

	def do_transfer(self, desc_source_target_list):
		pass

	def get_dependency_list(self):
		return []

	def get_process(self, fn_list):
		return (';', [])


class LocalSBStorageManager(StorageManager):
	alias_list = ['local']

	def __init__(self, config, name, storage_type, storage_channel):
		StorageManager.__init__(self, config, name, storage_type, storage_channel)
		self._sandbox_path = config.get_dn('%s path' % storage_channel,
			config.get_work_path('sandbox'), must_exist=False)

	def do_transfer(self, desc_source_target_list):
		for (desc, source, target) in desc_source_target_list:
			target = os.path.join(self._sandbox_path, target)
			try:
				shutil.copy(source, target)
			except Exception:
				raise StorageError('Unable to transfer %s %r to %r!' % (desc, source, target))


class SEStorageManager(StorageManager):
	alias_list = ['SE']

	def __init__(self, config, name, storage_type, storage_channel):
		StorageManager.__init__(self, config, name, storage_type, storage_channel)

		self._storage_files = config.get_list('%s files' % storage_channel, [])
		self._storage_pattern = config.get('%s pattern' % storage_channel, '@X@')
		self._storage_min_size = config.get_int('%s min size' % storage_channel, -1)
		self._storage_retry = config.get_int('%s retry' % storage_channel, 3)
		self._storage_timeout = config.get_time('%s timeout' % storage_channel, 2 * 60 * 60)
		self._storage_force = config.get_bool('%s force' % storage_channel, True)
		self._storage_paths = config.get_list(['%s path' % storage_type, '%s path' % storage_channel],
			default=[], on_valid=NoVarCheck(config), parse_item=_norm_se_path)
		storage_dep_set = set()
		storage_token_set = set()
		storage_url_reqs = {
			'arc://': (['ARCAccessToken'], []),
			'sftp://': (['KerberosAccessToken'], []),
		}
		storage_url_reqs.update(dict.fromkeys(['gsidcap://', 'eos://', 'gsiftp://',
			'lfc://', 'lfn://', 'rfio://', 'root://', 'srm://'], (['VomsProxy'], ['glite'])))
		for se_path in self._storage_paths:
			for se_url_req_key in storage_url_reqs:
				if se_path.startswith(se_url_req_key):
					(se_token_list, se_dep_list) = storage_url_reqs.get(se_url_req_key, ([], []))
					self._log.debug('Storage path %s requires access token: %s and environment: %s',
						se_path, str.join(', ', se_token_list), str.join(', ', se_dep_list))
					storage_token_set.update(se_token_list)
					storage_dep_set.update(se_dep_list)
		self._storage_dep_list = sorted(storage_dep_set)
		for token_name in storage_token_set:
			config.change_view(set_sections=['backend']).set('access token', token_name, '+=')

	def do_transfer(self, desc_source_target_list):
		for (desc, source, target) in desc_source_target_list:
			if not self._storage_paths:
				raise ConfigError('%s can\'t be transferred because %s path wasn\'t set' % (desc,
					self._storage_channel))
			for idx, se_path in enumerate(set(self._storage_paths)):
				activity = Activity('Copy %s to SE %d ' % (desc, idx + 1))
				proc = se_copy(source, os.path.join(se_path, target), self._storage_force)
				proc.status(timeout=5 * 60, terminate=True)
				activity.finish()
				if proc.status(timeout=0) == 0:
					self._log.info('Copy %s to SE %d finished', desc, idx + 1)
				else:
					self._log.log_process(proc, msg='Unable to copy %s to SE %d' % (desc, idx + 1))
					msg = 'Is %s (%s) already available on SE %s?' % (desc, source, se_path)
					if not UserInputInterface().prompt_bool(msg, False):
						raise StorageError('%s is missing on SE %s!' % (desc, se_path))

	def get_dependency_list(self):
		return self._storage_dep_list

	def get_process(self, fn_list):
		var_list = [
			'GCSW_STORAGE_TOOL="%s"\n' % 'gc-storage-tool',
			'GCSW_FILE_LIST="%s"\n' % str.join(' ', fn_list + self._storage_files),
			'GCSW_TIMEOUT=%d\n' % self._storage_timeout,
			'GCSW_MINSIZE=%d\n' % self._storage_min_size,
			'GCSW_RETRY=%d\n' % self._storage_retry,
		]
		if 'input' in self._storage_channel:
			var_list.extend([
				'GCSW_LOG_PREFIX=""\n',
				'GCSW_SRC_PATH_LIST="%s"\n' % str.join(' ', self._storage_paths),
				'GCSW_SRC_PATTERN="%s"\n' % self._storage_pattern,
				'GCSW_DST_PATH_LIST="job-scratch://"\n',
				'GCSW_DST_PATTERN="@X@"\n',
			])
		elif 'output' in self._storage_channel:
			var_list.extend([
				'GCSW_LOG_PREFIX="OUTPUT_"\n',
				'GCSW_SRC_PATH_LIST="job-scratch://"\n',
				'GCSW_SRC_PATTERN="@X@"\n',
				'GCSW_DST_PATH_LIST="%s"\n' % str.join(' ', self._storage_paths),
				'GCSW_DST_PATTERN="%s"\n' % self._storage_pattern,
			])
		return (';', [
			VirtualFile('_gc_%s.conf' % self._storage_channel.replace(' ', '_'), sorted(var_list)),
		])


def _norm_se_path(se_path):
	se_path = se_path.replace('dir://', 'file://')
	if '://' not in se_path:  # <relative path>
		se_path = 'file://' + os.path.abspath(se_path)
	return se_path.replace('dav://', 'http://').replace('davs://', 'https://')


def _se_runcmd(cmd, *urls, **kwargs):
	return LocalProcess(get_path_share('gc-storage-tool'), cmd, *imap(_norm_se_path, urls), **kwargs)
