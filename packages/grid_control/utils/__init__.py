# | Copyright 2007-2017 Karlsruhe Institute of Technology
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

import os, sys, glob, stat, time, logging
from grid_control.utils.activity import Activity
from grid_control.utils.algos import accumulate
from grid_control.utils.data_structures import UniqueList
from grid_control.utils.file_tools import SafeFile
from grid_control.utils.parsing import parse_type, str_dict_linear
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.thread_tools import TimeoutException, hang_protection
from grid_control.utils.user_interface import UserInputInterface
from hpfwk import NestedException, clear_current_exception, ignore_exception
from python_compat import exit_without_cleanup, ifilter, iidfilter, imap, irange, lfilter, lmap, lzip, reduce, rsplit, sorted, tarfile  # pylint:disable=line-too-long


_GLOBAL_STATE = {}


class ParsingError(NestedException):
	pass


class PathError(NestedException):
	pass


def abort(new=None):
	if new is not None:
		_GLOBAL_STATE[abort] = new
	return _GLOBAL_STATE.get(abort, False)


def clean_path(value):
	return os.path.normpath(os.path.expandvars(os.path.expanduser(value.strip())))


def create_tarball(match_info_iter, **kwargs):
	tar = tarfile.open(mode='w:gz', **kwargs)
	activity = Activity('Generating tarball')
	for match_info in match_info_iter:
		if isinstance(match_info, tuple):
			(path_source, path_target) = match_info
		else:
			(path_source, path_target) = (match_info, None)
		if isinstance(path_source, str):
			if not os.path.exists(path_source):
				raise PathError('File %s does not exist!' % path_source)
			tar.add(path_source, path_target or os.path.basename(path_source), recursive=False)
		elif path_source is None:  # Update activity
			activity.update('Generating tarball: %s' % path_target)
		else:  # File handle
			info, handle = path_source.get_tar_info()
			if path_target:
				info.name = path_target
			info.mtime = time.time()
			info.mode = stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP + stat.S_IROTH
			if info.name.endswith('.sh') or info.name.endswith('.py'):
				info.mode += stat.S_IXUSR + stat.S_IXGRP + stat.S_IXOTH
			tar.addfile(info, handle)
			handle.close()
	activity.finish()
	tar.close()


def deprecated(text):
	log = logging.getLogger('console')
	log.critical('\n%s\n[DEPRECATED] %s', SafeFile(get_path_share('fail.txt')).read_close(), text)
	if not UserInputInterface().prompt_bool('Do you want to continue?', False):
		sys.exit(os.EX_TEMPFAIL)


def disk_space_avail(dn, timeout=5):
	def _disk_space_avail():
		if os.path.exists(dn):
			try:
				stat_info = os.statvfs(dn)
				return stat_info.f_bavail * stat_info.f_bsize / 1024 ** 2
			except Exception:
				import ctypes
				free_bytes = ctypes.c_ulonglong(0)
				ctypes.windll.kernel32.GetDiskFreeSpaceExW(
					ctypes.c_wchar_p(dn), None, None, ctypes.pointer(free_bytes))
				return free_bytes.value / 1024 ** 2
		return -1

	try:
		return hang_protection(_disk_space_avail, timeout)
	except TimeoutException:
		logging.getLogger('console').critical(
			'Unable to get free disk space for directory %s after waiting for %d sec!\n' % (dn, timeout) +
			'The file system is probably hanging or corrupted' +
			' - try to check the free disk space manually. ' +
			'Refer to the documentation to disable checking the free disk space - at your own risk')
		exit_without_cleanup(os.EX_OSERR)


def display_selection(log, items_before, items_after, msg, formatter, log_level=logging.DEBUG1):
	if len(items_before) != len(items_after):
		log.log(logging.DEBUG, msg, (len(items_before) - len(items_after)))
		for item in items_before:
			if item in items_after:
				log.log(log_level, ' * %s', formatter(item))
			else:
				log.log(log_level, '   %s', formatter(item))


def ensure_dir_exists(dn, name='directory', exception_type=PathError):
	if not os.path.exists(dn):
		try:
			os.makedirs(dn)
		except Exception:
			raise exception_type('Problem creating %s "%s"' % (name, dn))
	return dn


def exec_wrapper(script, context=None):
	if context is None:
		context = dict()
	exec(script, context)  # pylint:disable=exec-used
	return context


def filter_processors(processor_list, id_fun=lambda proc: proc.__class__.__name__):
	(result, processor_id_list) = ([], [])
	for proc in processor_list:
		if proc.enabled() and (id_fun(proc) not in processor_id_list):
			result.append(proc)
			processor_id_list.append(id_fun(proc))
	return result


def get_file_name(fn):  # Return file name without extension
	return rsplit(os.path.basename(str(fn)).lstrip('.'), '.', 1)[0]


def get_local_username():
	for username in iidfilter(imap(os.environ.get, ['LOGNAME', 'USER', 'LNAME', 'USERNAME'])):
		return username
	return ''


def get_path_pkg(*args):
	return clean_path(os.path.join(os.environ['GC_PACKAGES_PATH'], *args))


def get_path_share(*args, **kwargs):
	return get_path_pkg(kwargs.get('pkg', 'grid_control'), 'share', *args)


def get_version():
	return sys.modules['grid_control'].__version__


def is_dumb_terminal(stream=sys.stdout):
	term_env = os.environ.get('TERM', 'dumb')
	if os.environ.get('GC_TERM', ''):
		term_env = os.environ['GC_TERM']
	if term_env == 'gc_color256':
		return False
	elif term_env == 'gc_color':
		return None
	missing_attr = (not hasattr(stream, 'isatty')) or not hasattr(stream, 'fileno')
	if (term_env == 'dumb') or missing_attr or not stream.isatty():
		return True
	if '16' in term_env:
		return None  # low color mode
	return False  # high color mode


def ping_host(host, timeout=1):
	proc = ignore_exception(Exception, None, LocalProcess, 'ping', '-Uqnc', 1, '-W', timeout, host)
	ping_str_list = ignore_exception(Exception, '', proc.get_output, timeout).strip().split('\n')
	if ping_str_list[-1].endswith('ms'):
		return ignore_exception(Exception, None, lambda: float(ping_str_list[-1].split('/')[-2]) / 1000.)


def prune_processors(do_prune, processor_list, log, msg, formatter=None, id_fun=None):
	def _get_class_name(proc):
		return proc.__class__.__name__
	selected = filter_processors(processor_list, id_fun or _get_class_name)
	display_selection(log, processor_list, selected, msg, formatter or _get_class_name)
	return selected


def remove_files(args):
	for item in args:
		try:
			if os.path.isdir(item):
				os.rmdir(item)
			else:
				os.unlink(item)
		except Exception:
			clear_current_exception()


def rename_file(old, new):
	if os.path.exists(new):
		os.unlink(new)
	os.rename(old, new)


def replace_with_dict(value, mapping_values, mapping_keys=None):
	mapping_keys = mapping_keys or lzip(mapping_values.keys(), mapping_values.keys())
	for (virtual, real) in mapping_keys:
		for delim in ['@', '__']:
			value = value.replace(delim + virtual + delim, str(mapping_values.get(real, '')))
	return value


def resolve_install_path(path):
	os_path_list = UniqueList(os.environ['PATH'].split(os.pathsep))
	result = resolve_paths(path, os_path_list, True, PathError)
	result_exe = lfilter(lambda fn: os.access(fn, os.X_OK), result)  # filter executable files
	if not result_exe:
		raise PathError('Files matching %s:\n\t%s\nare not executable!' % (
			path, str.join('\n\t', result)))
	return result_exe[0]


def resolve_path(path, search_path_list=None, must_exist=True, exception_type=PathError):
	result = resolve_paths(path, search_path_list, must_exist, exception_type)
	if len(result) > 1:
		raise exception_type('Path "%s" matches multiple files:\n\t%s' % (path, str.join('\n\t', result)))
	return result[0]


def resolve_paths(path, search_path_list=None, must_exist=True, exception_type=PathError):
	path = clean_path(path)  # replace $VAR, ~user, \ separators
	result = []
	if os.path.isabs(path):
		result.extend(sorted(glob.glob(path)))  # Resolve wildcards for existing files
		if not result:
			if must_exist:
				raise exception_type('Could not find file "%s"' % path)
			return [path]  # Return non-existing, absolute path
	else:  # search relative path in search directories
		search_path_list = search_path_list or []
		for spath in UniqueList(search_path_list):
			result.extend(sorted(glob.glob(clean_path(os.path.join(spath, path)))))
		if not result:
			if must_exist:
				raise exception_type('Could not find file "%s" in \n\t%s' % (
					path, str.join('\n\t', search_path_list)))
			return [path]  # Return non-existing, relative path
	return result


def safe_write(fp, content):
	fp.writelines(content)
	fp.truncate()
	fp.close()


def split_blackwhite_list(bwfilter):
	blacklist = lmap(lambda x: x[1:], ifilter(lambda x: x.startswith('-'), bwfilter or []))
	whitelist = lfilter(lambda x: not x.startswith('-'), bwfilter or [])
	return (blacklist, whitelist)


def split_opt(opt, delim):
	""" Split option strings into fixed tuples
	>>> split_opt('abc : ghi # def', ['#', ':'])
	('abc', 'def', 'ghi')
	>>> split_opt('abc:def', '::')
	('abc', 'def', '')
	"""
	def _get_delimeter_part(old_result, prefix):
		try:
			tmp = old_result[0].split(prefix)
			new = tmp.pop(1)
			try:  # Find position of other delimeters in string
				other_delim = min(ifilter(lambda idx: idx >= 0, imap(new.find, delim)))
				tmp[0] += new[other_delim:]
			except Exception:
				other_delim = None
			return [str.join(prefix, tmp)] + old_result[1:] + [new[:other_delim]]
		except Exception:
			return old_result + ['']
	result = imap(str.strip, reduce(_get_delimeter_part, delim, [opt]))
	return tuple(result)


def wait(timeout):
	activity = Activity('Waiting', parent='root')
	for remaining in irange(timeout, 0, -1):
		if abort():
			return False
		if (remaining == timeout) or (remaining < 5) or (remaining % 5 == 0):
			activity.update('Waiting for %d seconds' % remaining)
		time.sleep(1)
	activity.finish()
	return True


def wrap_list(value, length, delim_lines=',\n', delim_entries=', '):
	def _counter(item, buffer):
		return len(item) + sum(imap(len, buffer)) + 2 * len(buffer) > length
	wrapped = accumulate(value, [], _counter, add_fun=lambda x, y: x + [y])
	return str.join(delim_lines, imap(lambda x: str.join(delim_entries, x), wrapped))


class DictFormat(object):
	# escape_strings = escape '"', '$'
	# types = preserve type information
	def __init__(self, delimeter='=', escape_strings=False):
		self.delimeter = delimeter
		self._escape_strings = escape_strings

	# Parse dictionary lists
	def parse(self, lines, key_parser=None, value_parser=None):
		key_parser = key_parser or {}
		value_parser = value_parser or {}
		key_parser_default = key_parser.get(None, lambda k: parse_type(k.lower()))
		value_parser_default = value_parser.get(None, parse_type)
		data = {}
		do_add = False
		currentline = ''
		try:
			lines = lines.splitlines()
		except Exception:
			clear_current_exception()
		for line in lines:
			try:
				if not isinstance(line, str):
					line = line.decode('utf-8')
				if self._escape_strings:
					# Switch accumulate on/off when odd number of quotes found
					if (line.count('"') - line.count('\\"')) % 2 == 1:
						do_add = not do_add
					currentline += line
					if do_add:
						continue
				else:
					currentline = line
				# split at first occurence of delimeter and strip spaces around
				key_value_list = currentline.split(self.delimeter, 1)
				if len(key_value_list) == 2:
					key, value = key_value_list
					currentline = ''
				else:  # in case no delimeter was found
					currentline = ''
					continue
				if self._escape_strings:
					value = value.strip().strip('"').replace('\\"', '"').replace('\\$', '$')
				key = key_parser.get(key, key_parser_default)(key.strip())
				data[key] = value_parser.get(key, value_parser_default)(value.strip())
			except Exception:
				raise ParsingError('Invalid dict format in %s' % repr(line))
		if do_add:
			raise ParsingError('Invalid dict format in %s' % repr(lines))
		return data

	# Format dictionary list
	def format(self, entries, do_print_none=False, fkt=lambda x_y_z: x_y_z, format='%s%s%s\n'):
		result = []
		for key in sorted(entries.keys()):
			value = entries[key]
			if (value is None) and not do_print_none:
				continue
			if self._escape_strings and isinstance(value, str):
				value = '"%s"' % str(value).replace('"', '\\"').replace('$', '\\$')
				lines = value.splitlines()
				result.append(format % fkt((key, self.delimeter, lines[0])))
				result.extend(imap(lambda x: x + '\n', lines[1:]))
			else:
				result.append(format % fkt((key, self.delimeter, value)))
		return result


class Result(object):
	# Use with caution! Compared with tuples: +25% accessing, 8x slower instantiation
	def __init__(self, **kwargs):
		self.__dict__ = kwargs

	def __repr__(self):
		return 'Result(%s)' % str_dict_linear(self.__dict__)


class TwoSidedIterator(object):
	def __init__(self, _content):
		(self.__content, self._left, self._right) = (_content, 0, 0)

	def backward(self):
		while self._left + self._right < len(self.__content):
			self._right += 1
			yield self.__content[len(self.__content) - self._right]

	def forward(self):
		while self._left + self._right < len(self.__content):
			self._left += 1
			yield self.__content[self._left - 1]
