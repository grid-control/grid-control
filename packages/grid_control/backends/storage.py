# | Copyright 2010-2016 Karlsruhe Institute of Technology
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
from grid_control.config import ConfigError, validNoVar
from grid_control.gc_plugin import NamedPlugin
from grid_control.utils.activity import Activity
from grid_control.utils.process_base import LocalProcess
from hpfwk import NestedException
from python_compat import imap, set


class StorageError(NestedException):
	pass

# All functions use url_* functions from gc-run.lib (just like the job did...)

ensurePrefix = lambda fn: utils.QM('://' in fn, fn, 'file:////%s' % os.path.abspath(fn).lstrip('/'))

def se_runcmd(cmd, varDict, *urls):
	runLib = utils.pathShare('gc-run.lib')
	args = str.join(' ', imap(lambda x: '"%s"' % ensurePrefix(x).replace('dir://', 'file://'), urls))
	varString = str.join(' ', imap(lambda x: 'export %s="%s";' % (x, varDict[x]), varDict))
	return LocalProcess('/bin/bash', '-c', '. %s || exit 99; %s %s %s' % (runLib, varString, cmd, args))

se_ls = lambda target: se_runcmd('url_ls', {}, target)
se_rm = lambda target: se_runcmd('print_and_eval "url_rm"', {}, target)
se_mkdir = lambda target: se_runcmd('print_and_eval "url_mkdir"', {}, target)
se_exists = lambda target: se_runcmd('print_and_eval "url_exists"', {}, target)

def se_copy(src, dst, force = True, tmp = ''):
	cmd = 'print_and_eval "url_copy_single%s"' % utils.QM(force, '_force', '')
	return se_runcmd(cmd, {'GC_KEEPTMP': tmp}, src, dst)


class StorageManager(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['storage']
	tagName = 'storage'

	def __init__(self, config, name, storage_type, storage_channel, storage_var_prefix):
		NamedPlugin.__init__(self, config, name)
		(self._storage_channel, self._storage_var_prefix) = (storage_channel, storage_var_prefix)

	def addFiles(self, files):
		pass

	def getTaskConfig(self):
		return {}

	def getDependencies(self):
		return []

	def doTransfer(self, listDescSourceTarget):
		pass


class LocalSBStorageManager(StorageManager):
	def __init__(self, config, name, storage_type, storage_channel, storage_var_prefix):
		StorageManager.__init__(self, config, name, storage_type, storage_channel, storage_var_prefix)
		self._sandbox_path = config.getPath('%s path' % storage_type, config.getWorkPath('sandbox'), mustExist = False)

	def doTransfer(self, listDescSourceTarget):
		for (desc, source, target) in listDescSourceTarget:
			target = os.path.join(self._sandbox_path, target)
			try:
				shutil.copy(source, target)
			except Exception:
				raise StorageError('Unable to transfer %s "%s" to "%s"!' % (desc, source, target))


class SEStorageManager(StorageManager):
	def __init__(self, config, name, storage_type, storage_channel, storage_var_prefix):
		StorageManager.__init__(self, config, name, storage_type, storage_channel, storage_var_prefix)
		normSEPath = lambda x: utils.QM(x[0] == '/', 'dir:///%s' % x.lstrip('/'), x)
		self._storage_paths = config.getList(['%s path' % storage_type, '%s path' % storage_channel],
			default = [], onValid = validNoVar(config), parseItem = normSEPath)
		self._storage_files = config.getList('%s files' % storage_channel, [])
		self._storage_pattern = config.get('%s pattern' % storage_channel, '@X@')
		self._storage_timeout = config.getTime('%s timeout' % storage_channel, 2*60*60)
		self._storage_force = config.getBool('%s force' % storage_channel, True)

	def addFiles(self, files):
		self._storage_files.extend(files)

	def getTaskConfig(self):
		return {
			'%s_PATH' % self._storage_var_prefix: str.join(' ', self._storage_paths),
			'%s_FILES' % self._storage_var_prefix: str.join(' ', self._storage_files),
			'%s_PATTERN' % self._storage_var_prefix: self._storage_pattern,
			'%s_TIMEOUT' % self._storage_var_prefix: self._storage_timeout,
		}

	def getDependencies(self):
		if True in imap(lambda x: not x.startswith('dir'), self._storage_paths):
			return ['glite']
		return []

	def doTransfer(self, listDescSourceTarget):
		for (desc, source, target) in listDescSourceTarget:
			if not self._storage_paths:
				raise ConfigError("%s can't be transferred because '%s path wasn't set" % (desc, self._storage_channel))
			for idx, sePath in enumerate(set(self._storage_paths)):
				activity = Activity('Copy %s to SE %d ' % (desc, idx + 1))
				proc = se_copy(source, os.path.join(sePath, target), self._storage_force)
				proc.status(timeout = 5*60, terminate = True)
				activity.finish()
				if proc.status(timeout = 0) == 0:
					self._log.info('Copy %s to SE %d finished', desc, idx + 1)
				else:
					self._log.info('Copy %s to SE %d failed', desc, idx + 1)
					self._log.critical(proc.stderr.read(timeout = 0))
					self._log.critical('Unable to copy %s! You can try to copy it manually.', desc)
					if not utils.getUserBool('Is %s (%s) available on SE %s?' % (desc, source, sePath), False):
						raise StorageError('%s is missing on SE %s!' % (desc, sePath))
