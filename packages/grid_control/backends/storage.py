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

import os, sys, shutil
from grid_control import utils
from grid_control.config import ConfigError, validNoVar
from grid_control.gc_plugin import NamedPlugin
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
	configSections = NamedPlugin.configSections + ['storage']
	tagName = 'storage'

	def __init__(self, config, name, optDefault, optPrefix, varPrefix):
		NamedPlugin.__init__(self, config, name)
		(self.smOptPrefix, self.varPrefix) = (optPrefix, varPrefix)

	def addFiles(self, files):
		pass

	def getTaskConfig(self):
		return {}

	def getDependencies(self):
		return []

	def doTransfer(self, listDescSourceTarget):
		pass


class LocalSBStorageManager(StorageManager):
	def __init__(self, config, name, optDefault, optPrefix, varPrefix):
		StorageManager.__init__(self, config, name, optDefault, optPrefix, varPrefix)
		self.sbPath = config.getPath('%s path' % optDefault, config.getWorkPath('sandbox'), mustExist = False)

	def doTransfer(self, listDescSourceTarget):
		for (desc, source, target) in listDescSourceTarget:
			target = os.path.join(self.sbPath, target)
			try:
				shutil.copy(source, target)
			except Exception:
				raise StorageError('Unable to transfer %s "%s" to "%s"!' % (desc, source, target))


class SEStorageManager(StorageManager):
	def __init__(self, config, name, optDefault, optPrefix, varPrefix):
		StorageManager.__init__(self, config, name, optDefault, optPrefix, varPrefix)
		normSEPath = lambda x: utils.QM(x[0] == '/', 'dir:///%s' % x.lstrip('/'), x)
		self.defPaths = config.getList('%s path' % optDefault, [], onValid = validNoVar(config), parseItem = normSEPath)
		self.smPaths = config.getList('%s path' % optPrefix, self.defPaths, onValid = validNoVar(config), parseItem = normSEPath)
		self.smFiles = config.getList('%s files' % optPrefix, [])
		self.smPattern = config.get('%s pattern' % optPrefix, '@X@')
		self.smTimeout = config.getTime('%s timeout' % optPrefix, 2*60*60)
		self.smForce = config.getBool('%s force' % optPrefix, True)

	def addFiles(self, files):
		self.smFiles.extend(files)

	def getTaskConfig(self):
		return {
			'%s_PATH' % self.varPrefix: str.join(' ', self.smPaths),
			'%s_FILES' % self.varPrefix: str.join(' ', self.smFiles),
			'%s_PATTERN' % self.varPrefix: self.smPattern,
			'%s_TIMEOUT' % self.varPrefix: self.smTimeout,
		}

	def getDependencies(self):
		if True in imap(lambda x: not x.startswith('dir'), self.smPaths):
			return ['glite']
		return []

	def doTransfer(self, listDescSourceTarget):
		for (desc, source, target) in listDescSourceTarget:
			if not self.smPaths:
				raise ConfigError("%s can't be transferred because '%s path wasn't set" % (desc, self.smOptPrefix))
			for idx, sePath in enumerate(set(self.smPaths)):
				utils.vprint('Copy %s to SE %d ' % (desc, idx + 1), -1, newline = False)
				sys.stdout.flush()
				proc = se_copy(source, os.path.join(sePath, target), self.smForce)
				if proc.status(timeout = 5*60, terminate = True) == 0:
					utils.vprint('finished', -1)
				else:
					utils.vprint('failed', -1)
					utils.eprint(proc.stderr.read(timeout = 0))
					utils.eprint('Unable to copy %s! You can try to copy it manually.' % desc)
					if not utils.getUserBool('Is %s (%s) available on SE %s?' % (desc, source, sePath), False):
						raise StorageError('%s is missing on SE %s!' % (desc, sePath))
