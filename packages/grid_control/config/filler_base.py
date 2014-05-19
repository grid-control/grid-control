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
from grid_control import ConfigError, RethrowError, utils
import sys, os, logging, ConfigParser, socket

# Class to fill config containers with settings
class ConfigFiller(object):
	def fill(self, container):
		raise AbstractError


# Config filler which collects data from config files
class FileConfigFiller(ConfigFiller):
	def __init__(self, configFiles):
		self._configFiles = configFiles

	def fill(self, container):
		for configFile in self._configFiles:
			self._parseFile(container, configFile)

	def _parseFile(self, container, configFile, defaults = None, searchPaths = []):
		try:
			configFile = utils.resolvePath(configFile, searchPaths, ErrorClass = ConfigError)
			log = logging.getLogger(('config.%s' % utils.getRootName(configFile)).rstrip('.'))
			log.log(logging.INFO1, 'Reading config file %s' % configFile)
			for line in map(lambda x: x.rstrip() + '=:', open(configFile, 'r').readlines()):
				if line.startswith('[') or line.lstrip().startswith(';'):
					continue # skip section and comment lines
				# Abort if non-indented line with ":" preceeding "=" was found
				if (line.lstrip() == line) and (line.find(":") < line.find("=")):
					raise ConfigError('Invalid config line:\n\t%s\nPlease use "key = value" syntax or indent values!' % line)
			# Parse with python config parser
			parser = ConfigParser.ConfigParser(defaults)
			parser.readfp(open(configFile, 'r'))
			# Parse include files
			if parser.has_option('global', 'include'):
				includeFiles = parser.get('global', 'include').split('#')[0].split(';')[0]
				for includeFile in utils.parseList(includeFiles, None):
					self._parseFile(container, includeFile, parser.defaults(),
						searchPaths + [os.path.dirname(configFile)])
			# Store config settings
			for section in parser.sections():
				for option in parser.options(section):
					if (section, option) != ('global', 'include'):
						value_list = parser.get(section, option).splitlines() # Strip comments
						value_list = map(lambda l: l.rsplit(';', 1)[0].strip(), value_list)
						value_list = filter(lambda l: l != '', value_list)
						container.setEntry(section, option, str.join('\n', value_list), configFile)
		except:
			raise RethrowError('Error while reading configuration file "%s"!' % configFile, ConfigError)


# Config filler which collects data from default config files
class DefaultFilesConfigFiller(FileConfigFiller):
	def __init__(self):
		# Collect host / user / installation specific config files
		host = socket.gethostbyaddr(socket.gethostname())[0]
		hostCfg = map(lambda c: utils.pathGC('config/%s.conf' % host.split('.', c)[-1]), range(host.count('.') + 1, 0, -1))
		defaultCfg = ['/etc/grid-control.conf', '~/.grid-control.conf', utils.pathGC('config/default.conf')]
		if os.environ.get('GC_CONFIG'):
			defaultCfg.append('$GC_CONFIG')
		fqConfigFiles = map(lambda p: utils.resolvePath(p, mustExist = False), hostCfg + defaultCfg)
		FileConfigFiller.__init__(self, filter(os.path.exists, fqConfigFiles))


# Config filler which collects data from dictionary
class DictConfigFiller(ConfigFiller):
	def __init__(self, configDict):
		self._configDict = configDict

	def fill(self, container):
		for section in self._configDict:
			for option in self._configDict[section]:
				container.setEntry(section, option, self._configDict[section][option], '<dict>')


# Config filler which collects data from a user string
class StringConfigFiller(ConfigFiller):
	def __init__(self, optionList):
		self._optionList = optionList

	def fill(self, container):
		for uopt in self._optionList:
			try:
				section, tmp = tuple(uopt.lstrip('[').split(']', 1))
				option, value = tuple(map(str.strip, tmp.split('=', 1)))
				container.setEntry(section, option, value, '<cmdline override>')
			except:
				raise RethrowError('Unable to parse option %s' % uopt, ConfigError)


# Class to fill config containers with settings from a python config file
class PythonConfigFiller(DictConfigFiller):
	def __init__(self, configFile):
		from gcSettings import Settings
		exec open(configFile) in {}, {'Settings': Settings}
		DictConfigFiller.__init__(self, Settings._base)
