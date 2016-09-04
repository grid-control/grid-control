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

# GCSCF: DEF,ENC
import os, sys, logging
from grid_control import utils
from grid_control.config.config_entry import ConfigEntry, ConfigError
from grid_control.utils.data_structures import UniqueList
from grid_control.utils.file_objects import SafeFile
from grid_control.utils.parsing import parseList
from grid_control.utils.thread_tools import TimeoutException, hang_protection
from hpfwk import AbstractError, Plugin
from python_compat import identity, imap, irange, itemgetter, lfilter, lmap, rsplit

# Class to fill config containers with settings
class ConfigFiller(Plugin):
	def _addEntry(self, container, section, option, value, source):
		opttype = '='
		try:
			option = option.strip()
			if option[-1] in imap(itemgetter(0), ConfigEntry.OptTypeDesc.keys()):
				opttype = option[-1] + '='
				option = option[:-1].strip()
			container.append(ConfigEntry(section.strip(), option, value.strip(), opttype, source))
		except Exception:
			raise ConfigError('Unable to register config value [%s] %s %s %s (from %s)' % (section, option, opttype, value, source))

	def fill(self, container):
		raise AbstractError


# Config filler which collects data from config files
class FileConfigFiller(ConfigFiller):
	def __init__(self, configFiles, addSearchPath = True):
		(self._configFiles, self._addSearchPath) = (configFiles, addSearchPath)

	def fill(self, container):
		searchPaths = []
		for configFile in self._configFiles:
			configContent = {}
			searchPaths.extend(self._fillContentWithIncludes(configFile, [os.getcwd()], configContent))
			# Store config settings
			for section in configContent:
				for (option, value, source) in configContent[section]:
					self._addEntry(container, section, option, value, source)
		searchString = str.join(' ', UniqueList(searchPaths))
		if self._addSearchPath:
			self._addEntry(container, 'global', 'plugin paths+', searchString, str.join(',', self._configFiles))

	def _fillContentWithIncludes(self, configFile, searchPaths, configContent):
		log = logging.getLogger(('config.%s' % utils.getRootName(configFile)).rstrip('.').lower())
		log.log(logging.INFO1, 'Reading config file %s', configFile)
		configFile = utils.resolvePath(configFile, searchPaths, ErrorClass = ConfigError)
		configFileLines = SafeFile(configFile).readlines()

		# Single pass, non-recursive list retrieval
		tmpConfigContent = {}
		self._fillContentSingleFile(configFile, configFileLines, searchPaths, tmpConfigContent)
		def getFlatList(section, option):
			for (opt, value, _) in tmpConfigContent.get(section, []):
				if opt == option:
					for entry in parseList(value, None):
						yield entry

		newSearchPaths = [os.path.dirname(configFile)]
		# Add entries from include statement recursively
		for includeFile in getFlatList('global', 'include'):
			self._fillContentWithIncludes(includeFile, searchPaths + newSearchPaths, configContent)
		# Process all other entries in current file
		self._fillContentSingleFile(configFile, configFileLines, searchPaths, configContent)
		# Override entries in current config file
		for overrideFile in getFlatList('global', 'include override'):
			self._fillContentWithIncludes(overrideFile, searchPaths + newSearchPaths, configContent)
		# Filter special global options
		if configContent.get('global', []):
			configContent['global'] = lfilter(lambda opt_v_s: opt_v_s[0] not in ['include', 'include override'], configContent['global'])
		return searchPaths + newSearchPaths

	def _fillContentSingleFile(self, configFile, configFileLines, searchPaths, configContent):
		try:
			(self._currentSection, self._currentOption, self._currentValue, self._currentIndices) = (None, None, None, None)
			exceptionIntro = 'Unable to parse config file %s' % configFile
			for idx, line in enumerate(configFileLines):
				self._parseLine(exceptionIntro, configContent, configFile, idx, line)
			if self._currentOption:
				self._storeOption(exceptionIntro, configContent, configFile)
		except Exception:
			raise ConfigError('Error while reading configuration file "%s"!' % configFile)

	def _parseLineStripComments(self, exceptionIntro, configContent, configFile, idx, line):
		return rsplit(line, ';', 1)[0].rstrip()

	def _parseLineContinueOption(self, exceptionIntro, configContent, configFile, idx, line):
		self._currentValue += '\n' + line.strip()
		self._currentIndices += [idx]

	def _parseLineSection(self, exceptionIntro, configContent, configFile, idx, line):
		self._currentSection = line[1:line.index(']')].strip()
		self._parseLine(exceptionIntro, configContent, configFile, idx, line[line.index(']') + 1:].strip())

	def _parseLineOption(self, exceptionIntro, configContent, configFile, idx, line):
		(self._currentOption, self._currentValue) = lmap(str.strip, line.split('=', 1))
		self._currentIndices = [idx]

	# Not using ConfigParser anymore! Ability to read duplicate options is needed
	def _parseLine(self, exceptionIntro, configContent, configFile, idx, line):
		def protected_call(fun, exceptionMsg, line):
			try:
				return fun(exceptionIntro, configContent, configFile, idx, line)
			except Exception:
				raise ConfigError(exceptionIntro + ':%d\n\t%r\n' % (idx, line) + exceptionMsg)

		line = protected_call(self._parseLineStripComments, 'Unable to strip comments!', line)
		exceptionIntroLineInfo = exceptionIntro + ':%d\n\t%r\n' % (idx, line)
		if line.lstrip().startswith(';') or line.lstrip().startswith('#') or not line.strip():
			return # skip empty lines or comment lines
		elif line[0].isspace():
			protected_call(self._parseLineContinueOption, 'Invalid indentation!', line)
		elif line.startswith('['):
			if self._currentOption:
				self._storeOption(exceptionIntroLineInfo, configContent, configFile)
			protected_call(self._parseLineSection, 'Unable to parse config section!', line)
		elif '=' in line:
			if self._currentOption:
				self._storeOption(exceptionIntroLineInfo, configContent, configFile)
			protected_call(self._parseLineOption, 'Unable to parse config option!', line)
		else:
			raise ConfigError(exceptionIntroLineInfo + '\nPlease use "key = value" syntax or indent values!')

	def _storeOption(self, exceptionIntro, configContent, configFile):
		def assert_set(cond, msg):
			if not cond:
				raise ConfigError(exceptionIntro + '\n' + msg)
		assert_set(self._currentSection, 'Found config option outside of config section!')
		assert_set(self._currentOption, 'Config option is not set!')
		assert_set(self._currentValue is not None, 'Config value is not set!')
		assert_set(self._currentIndices, 'Config source not set!')
		sectionContent = configContent.setdefault(self._currentSection, [])
		self._currentValue = self._currentValue.replace('$GC_CONFIG_DIR', os.path.dirname(configFile))
		self._currentValue = self._currentValue.replace('$GC_CONFIG_FILE', configFile)
		sectionContent.append((self._currentOption, self._currentValue,
			configFile + ':' + str.join(',', imap(str, self._currentIndices))))
		(self._currentOption, self._currentValue, self._currentIndices) = (None, None, None)


# Config filler which collects data from default config files
class DefaultFilesConfigFiller(FileConfigFiller):
	def __init__(self):
		# Collect host / user / installation specific config files
		def resolve_hostname():
			import socket
			host = socket.gethostname()
			try:
				return socket.gethostbyaddr(host)[0]
			except Exception:
				return host
		log = logging.getLogger('config.default')
		try:
			host = hang_protection(resolve_hostname, timeout = 5)
			hostCfg = lmap(lambda c: utils.pathPKG('../config/%s.conf' % host.split('.', c)[-1]), irange(host.count('.') + 1, -1, -1))
			log.log(logging.DEBUG1, 'Possible host config files: %s', str.join(', ', hostCfg))
		except TimeoutException:
			sys.stderr.write('System call to resolve hostname is hanging!\n')
			sys.stderr.flush()
			hostCfg = []
		defaultCfg = ['/etc/grid-control.conf', '~/.grid-control.conf', utils.pathPKG('../config/default.conf')]
		if os.environ.get('GC_CONFIG'):
			defaultCfg.append('$GC_CONFIG')
		log.log(logging.DEBUG1, 'Possible default config files: %s', str.join(', ', defaultCfg))
		fqConfigFiles = lmap(lambda p: utils.resolvePath(p, mustExist = False), hostCfg + defaultCfg)
		FileConfigFiller.__init__(self, lfilter(os.path.exists, fqConfigFiles), addSearchPath = False)


# Config filler which collects data from dictionary
class DictConfigFiller(ConfigFiller):
	def __init__(self, configDict):
		self._configDict = configDict

	def fill(self, container):
		for section in self._configDict:
			for option in self._configDict[section]:
				self._addEntry(container, section, option, str(self._configDict[section][option]), '<dict>')


# Config filler which collects data from a user string
class StringConfigFiller(ConfigFiller):
	def __init__(self, optionList):
		self._optionList = lfilter(identity, imap(str.strip, optionList))

	def fill(self, container):
		for uopt in self._optionList:
			try:
				section, tmp = tuple(uopt.lstrip('[').split(']', 1))
			except Exception:
				raise ConfigError('Unable to parse section in %s' % repr(uopt))
			try:
				option, value = tuple(imap(str.strip, tmp.split('=', 1)))
				self._addEntry(container, section, option, value, '<cmdline override>')
			except Exception:
				raise ConfigError('Unable to parse option in %s' % repr(uopt))


# Class to fill config containers with settings from a python config file
class PythonConfigFiller(DictConfigFiller):
	def __init__(self, configFiles):
		from gcSettings import Settings
		for configFile in configFiles:
			fp = SafeFile(configFile)
			try:
				utils.execWrapper(fp.read(), {'Settings': Settings})
			finally:
				fp.close()
		DictConfigFiller.__init__(self, Settings.getConfigDict())


# Fill config using multiple fillers
class MultiConfigFiller(ConfigFiller):
	def __init__(self, fillerList):
		self._fillerList = fillerList

	def fill(self, container):
		for filler in self._fillerList:
			filler.fill(container)


# Class which handles python and normal config files transparently
class GeneralFileConfigFiller(MultiConfigFiller):
	def __init__(self, configFiles):
		fillerList = []
		for configFile in configFiles:
			if configFile.endswith('py'):
				fillerList.append(PythonConfigFiller([configFile]))
			else:
				fillerList.append(FileConfigFiller([configFile]))
		MultiConfigFiller.__init__(self, fillerList)


# read old persistency file - and set appropriate config options (only used on oldContainer!)
class CompatConfigFiller(ConfigFiller):
	def __init__(self, persistencyFile):
		self._persistencyDict = {}
		if os.path.exists(persistencyFile):
			self._persistencyDict = utils.PersistentDict(persistencyFile, ' = ')

	def fill(self, container):
		def setPersistentSetting(section, key):
			if key in self._persistencyDict:
				value = self._persistencyDict.get(key)
				self._addEntry(container, section, key, value, '<persistency file>')
		setPersistentSetting('task', 'task id')
		setPersistentSetting('task', 'task date')
		setPersistentSetting('parameters', 'parameter hash')
		setPersistentSetting('jobs', 'seeds')
