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
	def _add_entry(self, container, section, option, value, source):
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
	def __init__(self, config_file_list, add_search_path = True):
		(self._config_file_list, self._add_search_path) = (config_file_list, add_search_path)
		(self._current_section, self._current_option, self._current_value, self._current_indices) = (None, None, None, None)

	def fill(self, container):
		search_paths = []
		for config_file in self._config_file_list:
			content_configfile = {}
			search_paths.extend(self._fill_content_deep(config_file, [os.getcwd()], content_configfile))
			# Store config settings
			for section in content_configfile:
				for (option, value, source) in content_configfile[section]:
					self._add_entry(container, section, option, value, source)
		searchString = str.join(' ', UniqueList(search_paths))
		if self._add_search_path:
			self._add_entry(container, 'global', 'plugin paths+', searchString, str.join(',', self._config_file_list))

	def _fill_content_deep(self, config_file, search_paths, content_configfile):
		log = logging.getLogger(('config.%s' % utils.getRootName(config_file)).rstrip('.').lower())
		log.log(logging.INFO1, 'Reading config file %s', config_file)
		config_file = utils.resolvePath(config_file, search_paths, ErrorClass = ConfigError)
		config_file_lines = SafeFile(config_file).readlines()

		# Single pass, non-recursive list retrieval
		tmp_content_configfile = {}
		self._fill_content_shallow(config_file, config_file_lines, search_paths, tmp_content_configfile)
		def getFlatList(section, option):
			for (opt, value, _) in tmp_content_configfile.get(section, []):
				if opt == option:
					for entry in parseList(value, None):
						yield entry

		newsearch_paths = [os.path.dirname(config_file)]
		# Add entries from include statement recursively
		for includeFile in getFlatList('global', 'include'):
			self._fill_content_deep(includeFile, search_paths + newsearch_paths, content_configfile)
		# Process all other entries in current file
		self._fill_content_shallow(config_file, config_file_lines, search_paths, content_configfile)
		# Override entries in current config file
		for overrideFile in getFlatList('global', 'include override'):
			self._fill_content_deep(overrideFile, search_paths + newsearch_paths, content_configfile)
		# Filter special global options
		if content_configfile.get('global', []):
			content_configfile['global'] = lfilter(lambda opt_v_s: opt_v_s[0] not in ['include', 'include override'], content_configfile['global'])
		return search_paths + newsearch_paths

	def _fill_content_shallow(self, config_file, config_file_lines, search_paths, content_configfile):
		try:
			(self._current_section, self._current_option, self._current_value, self._current_indices) = (None, None, None, None)
			exception_intro = 'Unable to parse config file %s' % config_file
			for idx, line in enumerate(config_file_lines):
				self._parse_line(exception_intro, content_configfile, config_file, idx, line)
			if self._current_option:
				self._store_option(exception_intro, content_configfile, config_file)
		except Exception:
			raise ConfigError('Error while reading configuration file "%s"!' % config_file)

	def _parse_line_strip_comments(self, exception_intro, content_configfile, config_file, idx, line):
		return rsplit(line, ';', 1)[0].rstrip()

	def _parse_line_continue_option(self, exception_intro, content_configfile, config_file, idx, line):
		self._current_value += '\n' + line.strip()
		self._current_indices += [idx]

	def _parse_line_section(self, exception_intro, content_configfile, config_file, idx, line):
		self._current_section = line[1:line.index(']')].strip()
		self._parse_line(exception_intro, content_configfile, config_file, idx, line[line.index(']') + 1:].strip())

	def _parse_line_option(self, exception_intro, content_configfile, config_file, idx, line):
		(self._current_option, self._current_value) = lmap(str.strip, line.split('=', 1))
		self._current_indices = [idx]

	# Not using ConfigParser anymore! Ability to read duplicate options is needed
	def _parse_line(self, exception_intro, content_configfile, config_file, idx, line):
		def protected_call(fun, exceptionMsg, line):
			try:
				return fun(exception_intro, content_configfile, config_file, idx, line)
			except Exception:
				raise ConfigError(exception_intro + ':%d\n\t%r\n' % (idx, line) + exceptionMsg)

		line = protected_call(self._parse_line_strip_comments, 'Unable to strip comments!', line)
		exception_introLineInfo = exception_intro + ':%d\n\t%r\n' % (idx, line)
		if line.lstrip().startswith(';') or line.lstrip().startswith('#') or not line.strip():
			return # skip empty lines or comment lines
		elif line[0].isspace():
			protected_call(self._parse_line_continue_option, 'Invalid indentation!', line)
		elif line.startswith('['):
			if self._current_option:
				self._store_option(exception_introLineInfo, content_configfile, config_file)
			protected_call(self._parse_line_section, 'Unable to parse config section!', line)
		elif '=' in line:
			if self._current_option:
				self._store_option(exception_introLineInfo, content_configfile, config_file)
			protected_call(self._parse_line_option, 'Unable to parse config option!', line)
		else:
			raise ConfigError(exception_introLineInfo + '\nPlease use "key = value" syntax or indent values!')

	def _store_option(self, exception_intro, content_configfile, config_file):
		def assert_set(cond, msg):
			if not cond:
				raise ConfigError(exception_intro + '\n' + msg)
		assert_set(self._current_section, 'Found config option outside of config section!')
		assert_set(self._current_option, 'Config option is not set!')
		assert_set(self._current_value is not None, 'Config value is not set!')
		assert_set(self._current_indices, 'Config source not set!')
		content_section = content_configfile.setdefault(self._current_section, [])
		self._current_value = self._current_value.replace('$GC_CONFIG_DIR', os.path.dirname(config_file))
		self._current_value = self._current_value.replace('$GC_CONFIG_FILE', config_file)
		content_section.append((self._current_option, self._current_value,
			config_file + ':' + str.join(',', imap(str, self._current_indices))))
		(self._current_option, self._current_value, self._current_indices) = (None, None, None)


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
			host_config = lmap(lambda c: utils.pathPKG('../config/%s.conf' % host.split('.', c)[-1]), irange(host.count('.') + 1, -1, -1))
			log.log(logging.DEBUG1, 'Possible host config files: %s', str.join(', ', host_config))
		except TimeoutException:
			sys.stderr.write('System call to resolve hostname is hanging!\n')
			sys.stderr.flush()
			host_config = []
		default_config = ['/etc/grid-control.conf', '~/.grid-control.conf', utils.pathPKG('../config/default.conf')]
		if os.environ.get('GC_CONFIG'):
			default_config.append('$GC_CONFIG')
		log.log(logging.DEBUG1, 'Possible default config files: %s', str.join(', ', default_config))
		fqconfig_file_list = lmap(lambda p: utils.resolvePath(p, mustExist = False), host_config + default_config)
		FileConfigFiller.__init__(self, lfilter(os.path.exists, fqconfig_file_list), add_search_path = False)


# Config filler which collects data from dictionary
class DictConfigFiller(ConfigFiller):
	def __init__(self, config_dict):
		self._config_dict = config_dict

	def fill(self, container):
		for section in self._config_dict:
			for option in self._config_dict[section]:
				self._add_entry(container, section, option, str(self._config_dict[section][option]), '<dict>')


# Config filler which collects data from a user string
class StringConfigFiller(ConfigFiller):
	def __init__(self, option_list):
		self._option_list = lfilter(identity, imap(str.strip, option_list))

	def fill(self, container):
		for uopt in self._option_list:
			try:
				section, tmp = tuple(uopt.lstrip('[').split(']', 1))
			except Exception:
				raise ConfigError('Unable to parse section in %s' % repr(uopt))
			try:
				option, value = tuple(imap(str.strip, tmp.split('=', 1)))
				self._add_entry(container, section, option, value, '<cmdline override>')
			except Exception:
				raise ConfigError('Unable to parse option in %s' % repr(uopt))


# Class to fill config containers with settings from a python config file
class PythonConfigFiller(DictConfigFiller):
	def __init__(self, config_file_list):
		from gcSettings import Settings
		for config_file in config_file_list:
			fp = SafeFile(config_file)
			try:
				utils.execWrapper(fp.read(), {'Settings': Settings})
			finally:
				fp.close()
		DictConfigFiller.__init__(self, Settings.getConfigDict())


# Fill config using multiple fillers
class MultiConfigFiller(ConfigFiller):
	def __init__(self, config_filler_list):
		self._config_filler_list = config_filler_list

	def fill(self, container):
		for filler in self._config_filler_list:
			filler.fill(container)


# Class which handles python and normal config files transparently
class GeneralFileConfigFiller(MultiConfigFiller):
	def __init__(self, config_file_list):
		config_filler_list = []
		for config_file in config_file_list:
			if config_file.endswith('py'):
				config_filler_list.append(PythonConfigFiller([config_file]))
			else:
				config_filler_list.append(FileConfigFiller([config_file]))
		MultiConfigFiller.__init__(self, config_filler_list)


# read old persistency file - and set appropriate config options (only used on oldContainer!)
class CompatConfigFiller(ConfigFiller):
	def __init__(self, persistency_file):
		self._persistency_dict = {}
		if os.path.exists(persistency_file):
			self._persistency_dict = utils.PersistentDict(persistency_file, ' = ')

	def fill(self, container):
		def setPersistentSetting(section, key):
			if key in self._persistency_dict:
				value = self._persistency_dict.get(key)
				self._add_entry(container, section, key, value, '<persistency file>')
		setPersistentSetting('task', 'task id')
		setPersistentSetting('task', 'task date')
		setPersistentSetting('parameters', 'parameter hash')
		setPersistentSetting('jobs', 'seeds')
