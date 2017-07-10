# | Copyright 2014-2017 Karlsruhe Institute of Technology
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
from grid_control.config.config_entry import ConfigEntry, ConfigError
from grid_control.utils import exec_wrapper, get_file_name, get_path_pkg, resolve_path
from grid_control.utils.data_structures import UniqueList
from grid_control.utils.file_tools import SafeFile
from grid_control.utils.parsing import parse_list
from grid_control.utils.persistency import load_dict
from grid_control.utils.thread_tools import TimeoutException, hang_protection
from hpfwk import AbstractError, Plugin, clear_current_exception, ignore_exception
from python_compat import imap, irange, itemgetter, lfilter, lidfilter, rsplit


class ConfigFiller(Plugin):
	# Class to fill config containers with settings
	def fill(self, container):
		raise AbstractError

	def _add_entry(self, container, section, option, value, source):
		opttype = '='
		try:
			option = option.strip()
			if option[-1] in imap(itemgetter(0), ConfigEntry.map_opt_type2desc.keys()):
				opttype = option[-1] + '='
				option = option[:-1].strip()
			container.append(ConfigEntry(section.strip(), option, value.strip(), opttype, source))
		except Exception:
			raise ConfigError('Unable to register config value [%s] %s %s %s (from %s)' % (
				section, option, opttype, value, source))


class CompatConfigFiller(ConfigFiller):
	# read old persistency file - and set appropriate config options (only used on oldContainer!)
	def __init__(self, persistency_file):
		self._persistency_dict = {}
		if os.path.exists(persistency_file):
			self._persistency_dict = load_dict(persistency_file, ' = ', fmt_key=str.lower)

	def fill(self, container):
		def _set_persistent_setting(section, key):
			if key in self._persistency_dict:
				value = self._persistency_dict.get(key)
				self._add_entry(container, section, key, value, '<persistency file>')
		_set_persistent_setting('task', 'task id')
		_set_persistent_setting('task', 'task date')
		_set_persistent_setting('parameters', 'parameter hash')
		_set_persistent_setting('jobs', 'seeds')


class DictConfigFiller(ConfigFiller):
	# Config filler which collects data from dictionary
	def __init__(self, config_dict):
		self._config_dict = config_dict

	def fill(self, container):
		for section in self._config_dict:
			for option in self._config_dict[section]:
				self._add_entry(container, section, option, str(self._config_dict[section][option]), '<dict>')


class FileConfigFiller(ConfigFiller):
	# Config filler which collects data from config files
	def __init__(self, config_fn_list, add_search_path=True):
		(self._config_fn_list, self._add_search_path) = (config_fn_list, add_search_path)
		(self._cur_section, self._cur_option) = (None, None)
		(self._cur_value, self._cur_indices) = (None, None)

	def fill(self, container):
		search_path_list = []
		for config_fn in self._config_fn_list:
			content_configfile = {}
			search_path_list.extend(self._fill_content_deep(config_fn, [os.getcwd()], content_configfile))
			# Store config settings
			for section in content_configfile:
				for (option, value, source) in content_configfile[section]:
					self._add_entry(container, section, option, value, source)
		search_path_str = str.join(' ', UniqueList(search_path_list))
		if self._add_search_path:
			plugin_paths_source = str.join(',', self._config_fn_list)
			self._add_entry(container, 'global', 'plugin paths+', search_path_str, plugin_paths_source)

	def _fill_content_deep(self, config_fn, search_path_list, content_configfile):
		log = logging.getLogger(('config.%s' % get_file_name(config_fn)).rstrip('.').lower())
		log.log(logging.INFO1, 'Reading config file %s', config_fn)
		config_fn = resolve_path(config_fn, search_path_list, exception_type=ConfigError)
		config_str_list = list(SafeFile(config_fn).iter_close())

		# Single pass, non-recursive list retrieval
		tmp_content_configfile = {}
		self._fill_content_shallow(config_fn, config_str_list,
			search_path_list, tmp_content_configfile)

		def _get_list_shallow(section, option):
			for (opt, value, _) in tmp_content_configfile.get(section, []):
				if opt == option:
					for entry in parse_list(value, None):
						yield entry

		search_path_list_new = [os.path.dirname(config_fn)]
		# Add entries from include statement recursively
		for include_fn in _get_list_shallow('global', 'include'):
			self._fill_content_deep(include_fn, search_path_list + search_path_list_new, content_configfile)
		# Process all other entries in current file
		self._fill_content_shallow(config_fn, config_str_list, search_path_list, content_configfile)
		# Override entries in current config file
		for override_fn in _get_list_shallow('global', 'include override'):
			self._fill_content_deep(override_fn, search_path_list + search_path_list_new, content_configfile)
		# Filter special global options
		if content_configfile.get('global', []):
			def _ignore_includes(opt_v_s_tuple):
				return opt_v_s_tuple[0] not in ['include', 'include override']
			content_configfile['global'] = lfilter(_ignore_includes, content_configfile['global'])
		return search_path_list + search_path_list_new

	def _fill_content_shallow(self, config_fn, config_str_list, search_path_list, content_configfile):
		try:
			(self._cur_section, self._cur_option) = (None, None)
			(self._cur_value, self._cur_indices) = (None, None)
			exception_intro = 'Unable to parse config file %s' % config_fn
			for idx, line in enumerate(config_str_list):
				self._parse_line(exception_intro, content_configfile, config_fn, idx, line)
			if self._cur_option:
				self._store_option(exception_intro, content_configfile, config_fn)
		except Exception:
			raise ConfigError('Error while reading configuration file "%s"!' % config_fn)

	def _parse_line(self, exception_intro, content_configfile, config_fn, idx, line):
		# Not using ConfigParser anymore! Ability to read duplicate options is needed
		def _protected_call(fun, exception_msg, line):
			try:
				return fun(exception_intro, content_configfile, config_fn, idx, line)
			except Exception:
				raise ConfigError(exception_intro + ':%d\n\t%r\n' % (idx, line) + exception_msg)

		line = _protected_call(self._parse_line_strip_comments, 'Unable to strip comments!', line)
		exception_intro_ext = exception_intro + ':%d\n\t%r\n' % (idx, line)
		if line.lstrip().startswith(';') or line.lstrip().startswith('#') or not line.strip():
			return  # skip empty lines or comment lines
		elif line[0].isspace():
			_protected_call(self._parse_line_option_continued, 'Invalid indentation!', line)
		elif line.startswith('['):
			if self._cur_option:
				self._store_option(exception_intro_ext, content_configfile, config_fn)
			_protected_call(self._parse_line_section, 'Unable to parse config section!', line)
		elif '=' in line:
			if self._cur_option:
				self._store_option(exception_intro_ext, content_configfile, config_fn)
			_protected_call(self._parse_line_option, 'Unable to parse config option!', line)
		else:
			raise ConfigError(exception_intro_ext + '\nPlease use "key = value" syntax or indent values!')

	def _parse_line_option(self, exception_intro, content_configfile, config_fn, idx, line):
		(option, value) = line.split('=', 1)
		(self._cur_option, self._cur_value, self._cur_indices) = (option.strip(), value.strip(), [idx])

	def _parse_line_option_continued(self, exception_intro, content_configfile, config_fn, idx, line):
		self._cur_value += '\n' + line.strip()
		self._cur_indices += [idx]

	def _parse_line_section(self, exception_intro, content_configfile, config_fn, idx, line):
		self._cur_section = line[1:line.index(']')].strip()
		self._parse_line(exception_intro, content_configfile, config_fn,
			idx, line[line.index(']') + 1:].strip())

	def _parse_line_strip_comments(self, exception_intro, content_configfile, config_fn, idx, line):
		return rsplit(line, ';', 1)[0].rstrip()

	def _store_option(self, exception_intro, content_configfile, config_fn):
		def _assert_set(cond, msg):
			if not cond:
				raise ConfigError(exception_intro + '\n' + msg)
		_assert_set(self._cur_section, 'Found config option outside of config section!')
		_assert_set(self._cur_option, 'Config option is not set!')
		_assert_set(self._cur_value is not None, 'Config value is not set!')
		_assert_set(self._cur_indices, 'Config source not set!')
		content_section = content_configfile.setdefault(self._cur_section, [])
		self._cur_value = self._cur_value.replace('$GC_CONFIG_DIR', os.path.dirname(config_fn))
		self._cur_value = self._cur_value.replace('$GC_CONFIG_FILE', config_fn)
		content_section.append((self._cur_option, self._cur_value,
			config_fn + ':' + str.join(',', imap(str, self._cur_indices))))
		(self._cur_option, self._cur_value, self._cur_indices) = (None, None, None)


class MultiConfigFiller(ConfigFiller):
	# Fill config using multiple fillers
	def __init__(self, config_filler_list):
		self._config_filler_list = config_filler_list

	def fill(self, container):
		for filler in self._config_filler_list:
			filler.fill(container)


class StringConfigFiller(ConfigFiller):
	# Config filler which collects data from a user string
	def __init__(self, option_list, default_section=None):
		self._default_section = default_section
		self._option_list = lidfilter(imap(str.strip, option_list))

	def fill(self, container):
		for section_option_value_str in self._option_list:
			(section, option, value) = self.parse_config_str(section_option_value_str, self._default_section)
			self._add_entry(container, section, option, value, '<cmdline override>')

	def parse_config_str(cls, section_option_value_str, default_section):
		try:
			(section, option_value_str) = tuple(section_option_value_str.lstrip('[').split(']', 1))
		except Exception:
			if default_section is not None:
				(section, option_value_str) = (default_section, section_option_value_str)
			else:
				raise ConfigError('Unable to parse section in %s' % repr(section_option_value_str))
		try:
			option, value = tuple(imap(str.strip, option_value_str.split('=', 1)))
			return (section, option, value)
		except Exception:
			raise ConfigError('Unable to parse option in %s' % repr(section_option_value_str))
	parse_config_str = classmethod(parse_config_str)


class PythonConfigFiller(DictConfigFiller):
	# Class to fill config containers with settings from a python config file
	def __init__(self, config_fn_list):
		from grid_control_settings import Settings
		for config_fn in config_fn_list:
			exec_wrapper(SafeFile(resolve_path(config_fn, ['.'])).read_close(), {'Settings': Settings})
		DictConfigFiller.__init__(self, Settings.get_config_dict())


class DefaultFilesConfigFiller(FileConfigFiller):
	# Config filler which collects data from default config files
	def __init__(self):
		# Collect host / user / installation specific config files
		def _resolve_hostname():
			import socket
			host = socket.gethostname()
			return ignore_exception(Exception, host, lambda: socket.gethostbyaddr(host)[0])

		try:
			hostname = hang_protection(_resolve_hostname, timeout=5)
		except TimeoutException:
			clear_current_exception()
			hostname = None
			logging.getLogger('console').warning('System call to resolve hostname is hanging!')

		def _get_default_config_fn_iter():  # return possible default config files
			if hostname:  # host / domain specific
				for part_idx in irange(hostname.count('.') + 1, -1, -1):
					yield get_path_pkg('../config/%s.conf' % hostname.split('.', part_idx)[-1])
			yield '/etc/grid-control.conf'  # system specific
			yield '~/.grid-control.conf'  # user specific
			yield get_path_pkg('../config/default.conf')  # installation specific
			if os.environ.get('GC_CONFIG'):
				yield '$GC_CONFIG'  # environment specific

		config_fn_list = list(_get_default_config_fn_iter())
		log = logging.getLogger('config.sources.default')
		log.log(logging.DEBUG1, 'Possible default config files: %s', str.join(', ', config_fn_list))
		config_fn_iter = imap(lambda fn: resolve_path(fn, must_exist=False), config_fn_list)
		FileConfigFiller.__init__(self, lfilter(os.path.exists, config_fn_iter), add_search_path=False)


class GeneralFileConfigFiller(MultiConfigFiller):
	# Class which handles python and normal config files transparently
	def __init__(self, config_fn_list):
		config_filler_list = []
		for config_fn in config_fn_list:
			if config_fn.endswith('py'):
				config_filler_list.append(PythonConfigFiller([config_fn]))
			else:
				config_filler_list.append(FileConfigFiller([config_fn]))
		MultiConfigFiller.__init__(self, config_filler_list)
