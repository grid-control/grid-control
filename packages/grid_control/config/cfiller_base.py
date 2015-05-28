#-#  Copyright 2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

# GCSCF: DEF,ENC
import os, logging
from grid_control import utils
from grid_control.abstract import LoadableObject
from grid_control.config.config_entry import ConfigEntry
from grid_control.exceptions import ConfigError, RethrowError
from grid_control.utils.data_structures import UniqueList
from python_compat import rsplit

# Class to fill config containers with settings
class ConfigFiller(LoadableObject):
	def _addEntry(self, container, section, option, value, source):
		option = option.strip()
		opttype = '='
		if option[-1] in ['+', '-', '*', '?', '^']:
			opttype = option[-1] + '='
			option = option[:-1].strip()
		container.append(ConfigEntry(section.strip(), option, value.strip(), opttype, source))

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
			searchPaths.extend(self._fillContentFromFile(configFile, [os.getcwd()], configContent))
			# Store config settings
			for section in configContent:
				# Allow very basic substitutions with %(option)s syntax
				substDict = dict(map(lambda (opt, v, l): (opt, v), configContent.get('default', [])))
				substDict.update(map(lambda (opt, v, l): (opt, v), configContent.get(section, [])))
				for (option, value, source) in configContent[section]:
					# Protection for non-interpolation "%" in value 
					value = (value.replace('%', '\x01').replace('\x01(', '%(') % substDict).replace('\x01', '%')
					self._addEntry(container, section, option, value, source)
		searchString = str.join(' ', UniqueList(searchPaths))
		if self._addSearchPath:
			self._addEntry(container, 'global', 'module paths+', searchString, str.join(',', self._configFiles))

	def _fillContentFromSingleFile(self, configFile, configFileData, searchPaths, configContent):
		try:
			(self._currentSection, self._currentOption, self._currentValue, self._currentLines) = (None, None, None, None)
			def storeOption():
				if not self._currentSection:
					raise ConfigError(exceptionText + '\nFound config option outside of config section!')
				assert(self._currentOption and (self._currentValue != None) and self._currentLines)
				sectionContent = configContent.setdefault(self._currentSection, [])
				sectionContent.append((self._currentOption, self._currentValue,
					configFile + ':' + str.join(',', map(str, self._currentLines))))
				(self._currentOption, self._currentValue, self._currentLines) = (None, None, None)

			# Not using ConfigParser anymore! Ability to read duplicate options is needed
			def parseLine(idx, line):
				exceptionText = 'Unable to parse config file %s:%d\n\t%r' % (configFile, idx, line)
				try:
					line = rsplit(line, ';', 1)[0].rstrip()
				except Exception:
					raise ConfigError(exceptionText + '\nUnable to strip comments!')
				exceptionText = 'Unable to parse config file %s:%d\n\t%r' % (configFile, idx, line)
				if not line.strip() or line.startswith('#'): # skip empty lines or comment lines
					return
				elif line[0].isspace():
					try:
						self._currentValue += '\n' + line.strip()
						self._currentLines += [idx]
					except Exception:
						raise ConfigError(exceptionText + '\nInvalid indentation!')
				elif line.startswith('['):
					if self._currentOption:
						storeOption()
					try:
						self._currentSection = line[1:line.index(']')].strip()
						parseLine(idx, line[line.index(']') + 1:].strip())
					except Exception:
						raise ConfigError(exceptionText + '\nUnable to parse config section!')
				elif '=' in line:
					if self._currentOption:
						storeOption()
					try:
						(self._currentOption, self._currentValue) = map(str.strip, line.split('=', 1))
						self._currentLines = [idx]
					except Exception:
						raise ConfigError(exceptionText + '\nUnable to parse config option!')
				else:
					raise ConfigError(exceptionText + '\nPlease use "key = value" syntax or indent values!')
			for idx, line in enumerate(configFileData):
				parseLine(idx, line)
			if self._currentOption:
				storeOption()
		except Exception:
			raise RethrowError('Error while reading configuration file "%s"!' % configFile, ConfigError)

	def _fillContentFromFile(self, configFile, searchPaths, configContent = {}):
		log = logging.getLogger(('config.%s' % utils.getRootName(configFile)).rstrip('.'))
		log.log(logging.INFO1, 'Reading config file %s' % configFile)
		configFile = utils.resolvePath(configFile, searchPaths, ErrorClass = ConfigError)
		configFileData = open(configFile, 'r').readlines()

		# Single pass, non-recursive list retrieval
		tmpConfigContent = {}
		self._fillContentFromSingleFile(configFile, configFileData, searchPaths, tmpConfigContent)
		def getFlatList(section, option):
			for (opt, value, s) in filter(lambda (opt, v, s): opt == option, tmpConfigContent.get(section, [])):
				for entry in utils.parseList(value, None):
					yield entry

		newSearchPaths = [os.path.dirname(configFile)]
		# Add entries from include statement recursively
		for includeFile in getFlatList('global', 'include'):
			self._fillContentFromFile(includeFile, searchPaths + newSearchPaths, configContent)
		# Process all other entries in current file
		self._fillContentFromSingleFile(configFile, configFileData, searchPaths, configContent)
		# Override entries in current config file
		for overrideFile in getFlatList('global', 'include override'):
			self._fillContentFromFile(overrideFile, searchPaths + newSearchPaths, configContent)
		# Filter special global options
		if configContent.get('global', []):
			configContent['global'] = filter(lambda (opt, v, s): opt not in ['include', 'include override'], configContent['global'])
		return searchPaths + newSearchPaths


# Config filler which collects data from default config files
class DefaultFilesConfigFiller(FileConfigFiller):
	def __init__(self):
		# Collect host / user / installation specific config files
		import socket
		host = socket.gethostbyaddr(socket.gethostname())[0]
		hostCfg = map(lambda c: utils.pathGC('config/%s.conf' % host.split('.', c)[-1]), range(host.count('.') + 1, 0, -1))
		defaultCfg = ['/etc/grid-control.conf', '~/.grid-control.conf', utils.pathGC('config/default.conf')]
		if os.environ.get('GC_CONFIG'):
			defaultCfg.append('$GC_CONFIG')
		fqConfigFiles = map(lambda p: utils.resolvePath(p, mustExist = False), hostCfg + defaultCfg)
		FileConfigFiller.__init__(self, filter(os.path.exists, fqConfigFiles), addSearchPath = False)


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
		self._optionList = filter(lambda x: x, map(str.strip, optionList))

	def fill(self, container):
		for uopt in self._optionList:
			try:
				section, tmp = tuple(uopt.lstrip('[').split(']', 1))
				option, value = tuple(map(str.strip, tmp.split('=', 1)))
				self._addEntry(container, section, option, value, '<cmdline override>')
			except Exception:
				raise RethrowError('Unable to parse option %s' % uopt, ConfigError)


# Class to fill config containers with settings from a python config file
class PythonConfigFiller(DictConfigFiller):
	def __init__(self, configFiles):
		from gcSettings import Settings
		for configFile in configFiles:
			exec(open(configFile), {}, {'Settings': Settings})
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
