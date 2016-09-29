# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

import os, sys, glob, stat, time, signal, fnmatch, logging, operator
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import UniqueList
from grid_control.utils.parsing import parse_bool, parse_type, str_dict
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.table import ColumnTable, ParseableTable, RowTable
from grid_control.utils.thread_tools import TimeoutException, hang_protection
from hpfwk import NestedException, clear_current_exception
from python_compat import exit_without_cleanup, get_user_input, identity, ifilter, imap, irange, lfilter, lmap, lzip, next, reduce, rsplit, sorted, tarfile, unspecified, sort_inplace


class GCIOError(NestedException):
	pass


class ParsingError(NestedException):
	pass


class PathError(NestedException):
	pass


def abort(new=None):
	if new is not None:
		abort.state = new
	try:
		return abort.state
	except Exception:
		return False


def accumulate(iterable, empty, do_emit, do_add=lambda item, buffer: True, add_fun=operator.add):
	buf = empty
	for item in iterable:
		if do_add(item, buf):
			buf = add_fun(buf, item)
		if do_emit(item, buf):
			if buf != empty:
				yield buf
			buf = empty
	if buf != empty:
		yield buf


def clean_path(value):
	return os.path.normpath(os.path.expandvars(os.path.expanduser(value.strip())))


def create_tarball(tar_path, match_info_list):
	tar = tarfile.open(tar_path, 'w:gz')
	activity = Activity('Generating tarball')
	for (path_abs, path_rel, path_status) in match_info_list:
		if path_status is True:  # Existing file
			tar.add(path_abs, path_rel, recursive=False)
		elif path_status is False:  # Existing file
			if not os.path.exists(path_abs):
				raise PathError('File %s does not exist!' % path_rel)
			tar.add(path_abs, path_rel, recursive=False)
		elif path_status is None:  # Directory
			activity.update('Generating tarball: %s' % path_rel)
		else:  # File handle
			info, handle = path_status.get_tar_info()
			info.mtime = time.time()
			info.mode = stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP + stat.S_IROTH
			if info.name.endswith('.sh') or info.name.endswith('.py'):
				info.mode += stat.S_IXUSR + stat.S_IXGRP + stat.S_IXOTH
			tar.addfile(info, handle)
			handle.close()
	activity.finish()
	tar.close()


def deprecated(text):
	sys.stderr.write('%s\n[DEPRECATED] %s\n' % (open(get_path_share('fail.txt'), 'r').read(), text))
	if not get_user_bool('Do you want to continue?', False):
		sys.exit(os.EX_TEMPFAIL)


def disk_usage(dn, timeout=5):
	def _disk_usage():
		if os.path.exists(dn):
			try:
				stat_info = os.statvfs(dn)
				return stat_info.f_bavail * stat_info.f_bsize / 1024**2
			except Exception:
				import ctypes
				free_bytes = ctypes.c_ulonglong(0)
				ctypes.windll.kernel32.GetDiskFreeSpaceExW(
					ctypes.c_wchar_p(dn), None, None, ctypes.pointer(free_bytes))
				return free_bytes.value / 1024**2
		return -1

	try:
		return hang_protection(_disk_usage, timeout)
	except TimeoutException:
		sys.stderr.write(
			'Unable to get free disk space for directory %s after waiting for %d sec!\n' % (dn, timeout) +
			'The file system is probably hanging or corrupted' +
			' - try to check the free disk space manually. ' +
			'Refer to the documentation to disable checking the free disk space - at your own risk')
		exit_without_cleanup(os.EX_OSERR)


def display_selection(log, items_before, items_after, message, formatter, log_level=logging.DEBUG1):
	if len(items_before) != len(items_after):
		log.log(logging.DEBUG, message, (len(items_before) - len(items_after)))
		for item in items_before:
			if item in items_after:
				log.log(log_level, ' * %s', formatter(item))
			else:
				log.log(log_level, '   %s', formatter(item))


def display_table(head, data, fmt_string='', fmt=None):
	if display_table.mode == 'parseable':
		return ParseableTable(head, data, '|')
	elif display_table.mode == 'longlist':
		return RowTable(head, data, fmt, display_table.wraplen)
	return ColumnTable(head, data, fmt_string, fmt, display_table.wraplen)
display_table.wraplen = 100
display_table.mode = 'default'


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


def exit_with_usage(usage, msg=None, show_help=True):
	exit_msg = 'Syntax: %s\n' % usage
	if show_help:
		exit_msg += 'Use --help to get a list of options!\n'
	if msg:
		exit_msg += msg + '\n'
	sys.stderr.write(exit_msg)
	sys.exit(os.EX_USAGE)


def filter_dict(mapping, key_filter=lambda k: True, value_filter=lambda v: True):
	def _filter_items(k_v):
		return key_filter(k_v[0]) and value_filter(k_v[1])
	return dict(ifilter(_filter_items, mapping.items()))


def filter_processors(processor_list, id_fun=lambda proc: proc.__class__.__name__):
	(result, processor_id_list) = ([], [])
	for proc in processor_list:
		if proc.enabled() and (id_fun(proc) not in processor_id_list):
			result.append(proc)
			processor_id_list.append(id_fun(proc))
	return result


def get_file_name(fn):  # Return file name without extension
	return rsplit(os.path.basename(str(fn)).lstrip('.'), '.', 1)[0]


def get_list_difference(list_old, list_new, key_fun, on_matching_fun,
		is_sorted=False, key_fun_sort=None):
	(list_added, list_missing, list_matching) = ([], [], [])
	if not is_sorted:
		list_new = sorted(list_new, key=key_fun_sort or key_fun)
		list_old = sorted(list_old, key=key_fun_sort or key_fun)
	(iter_new, iter_old) = (iter(list_new), iter(list_old))
	(new, old) = (next(iter_new, None), next(iter_old, None))
	while True:
		if (new is None) or (old is None):
			break
		key_new = key_fun(new)
		key_old = key_fun(old)
		if key_new < key_old:  # new[npos] < old[opos]
			list_added.append(new)
			new = next(iter_new, None)
		elif key_new > key_old:  # new[npos] > old[opos]
			list_missing.append(old)
			old = next(iter_old, None)
		else:  # new[npos] == old[opos] according to *active* comparison
			on_matching_fun(list_added, list_missing, list_matching, old, new)
			(new, old) = (next(iter_new, None), next(iter_old, None))
	while new is not None:
		list_added.append(new)
		new = next(iter_new, None)
	while old is not None:
		list_missing.append(old)
		old = next(iter_old, None)
	return (list_added, list_missing, list_matching)


def get_path_pkg(*args):
	return clean_path(os.path.join(os.environ['GC_PACKAGES_PATH'], *args))


def get_path_share(*args, **kw):
	return get_path_pkg(kw.get('pkg', 'grid_control'), 'share', *args)


def get_user_bool(text, default):
	def _get_user_input(text, default, choices, parser=identity):
		log = logging.getLogger('console')
		while True:
			handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
			try:
				userinput = get_user_input('%s %s: ' % (text, '[%s]' % default))
			except Exception:
				sys.stdout.write('\n')  # continue on next line
				raise
			signal.signal(signal.SIGINT, handler)
			if userinput == '':
				return parser(default)
			if parser(userinput) is not None:
				return parser(userinput)
			valid = str.join(', ', imap(lambda x: '"%s"' % x, choices[:-1]))
			log.critical('Invalid input! Answer with %s or "%s"', valid, choices[-1])
	return _get_user_input(text, QM(default, 'yes', 'no'), ['yes', 'no'], parse_bool)


def get_version():
	def _get_version():
		try:
			proc_ver = LocalProcess('svnversion', '-c', get_path_pkg())
			version = proc_ver.get_output(timeout=10).strip()
			if lfilter(str.isdigit, version):
				proc_branch = LocalProcess('svn info', get_path_pkg())
				if 'stable' in proc_branch.get_output(timeout=10):
					return '%s - stable' % version
				return '%s - testing' % version
		except Exception:
			clear_current_exception()
		return __import__('grid_control').__version__ + ' or later'
	if not hasattr(get_version, 'cache'):
		get_version.cache = _get_version()
	return get_version.cache


def intersect_first_dict(dict1, dict2):
	for key1 in list(dict1.keys()):
		if (key1 in dict2) and (dict1[key1] != dict2[key1]):
			dict1.pop(key1)


def match_file_name(fn, pat_list):
	match = None
	for pat in pat_list:
		if fnmatch.fnmatch(fn, pat.lstrip('-')):
			match = not pat.startswith('-')
	return match


def match_files(path_root, pattern_list, path_rel=''):
	# Return (root, fn, state) - state: None == dir, True/False = (un)checked file, other = filehandle
	yield (path_root, path_rel, None)
	file_list = os.listdir(os.path.join(path_root, path_rel))
	for name in imap(lambda x: os.path.join(path_rel, x), file_list):
		match = match_file_name(name, pattern_list)
		path_abs = os.path.join(path_root, name)
		if match is False:
			continue
		elif os.path.islink(path_abs):  # Not excluded symlinks
			yield (path_abs, name, True)
		elif os.path.isdir(path_abs):  # Recurse into directories
			if match is True:  # (backwards compat: add parent directory - not needed?)
				yield (path_abs, name, True)
				for result in match_files(path_root, ['*'], name):
					yield result
			else:
				for result in match_files(path_root, pattern_list, name):
					yield result
		elif match is True:  # Add matches
			yield (path_abs, name, True)


def merge_dict_list(dict_list):
	tmp = dict()
	for mapping in dict_list:
		tmp.update(mapping)
	return tmp


def ping_host(host):
	proc = LocalProcess('ping', '-Uqnc', 1, '-W', 1, host)
	try:
		tmp = proc.get_output(timeout=1).splitlines()
		if tmp[-1].endswith('ms'):
			return float(tmp[-1].split('/')[-2]) / 1000.
	except Exception:
		return None


def prune_processors(do_prune, processor_list, log, message, formatter=None, id_fun=None):
	def get_class_name(proc):
		return proc.__class__.__name__
	selected = filter_processors(processor_list, id_fun or get_class_name)
	display_selection(log, processor_list, selected, message, formatter or get_class_name)
	return selected


def QM(cond, value1, value2):
	if cond:
		return value1
	return value2


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
			path, str.join('\n\t', result_exe)))
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


def safe_index(indexable, idx, default=None):
	try:
		return indexable.index(idx)
	except Exception:
		return default


def safe_write(fp, content):
	fp.writelines(content)
	fp.truncate()
	fp.close()


def split_blackwhite_list(bwfilter):
	blacklist = lmap(lambda x: x[1:], ifilter(lambda x: x.startswith('-'), bwfilter or []))
	whitelist = lfilter(lambda x: not x.startswith('-'), bwfilter or [])
	return (blacklist, whitelist)


def split_list(iterable, fun, sort_key=unspecified):
	# single pass on iterable!
	(result_true, result_false) = ([], [])
	for value in iterable:
		if fun(value):
			result_true.append(value)
		else:
			result_false.append(value)
	if not unspecified(sort_key):
		sort_inplace(result_true, key=sort_key)
		sort_inplace(result_false, key=sort_key)
	return (result_true, result_false)


def split_opt(opt, delim, empty=''):
	""" Split option strings into fixed tuples
	>>> split_opt('abc : ghi # def', ['#', ':'])
	('abc', 'def', 'ghi')
	>>> split_opt('abc:def', '::')
	('abc', 'def', '')
	"""
	def get_delimeter_part(old_result, prefix):
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
	result = imap(str.strip, reduce(get_delimeter_part, delim, [opt]))
	return tuple(imap(lambda x: QM(x == '', empty, x), result))


def swap(value1, value2):
	return (value2, value1)


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
	def counter(item, buffer):
		return len(item) + sum(imap(len, buffer)) + 2 * len(buffer) > length
	wrapped = accumulate(value, [], counter, add_fun=lambda x, y: x + [y])
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
				# FIXME: it may better to do .encode('utf-8') here
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
		return 'Result(%s)' % str_dict(self.__dict__)


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


class PersistentDict(dict):
	def __init__(self, filename, delimeter='=', lower_case_key=True):
		dict.__init__(self)
		self._fmt = DictFormat(delimeter)
		self._fn = filename
		key_parser = {None: QM(lower_case_key, lambda k: parse_type(k.lower()), parse_type)}
		try:
			self.update(self._fmt.parse(open(filename), key_parser=key_parser))
		except Exception:
			clear_current_exception()
		self._old_dict = self.items()

	def get(self, key, default=None, autoUpdate=True):
		value = dict.get(self, key, default)
		if autoUpdate:
			self.write({key: value})
		return value

	def write(self, newdict=None, update=True):
		if not update:
			self.clear()
		self.update(newdict or {})
		if dict(self._old_dict) == dict(self.items()):
			return
		try:
			if self._fn:
				safe_write(open(self._fn, 'w'), self._fmt.format(self))
		except Exception:
			raise GCIOError('Could not write to file %s' % self._fn)
		self._old_dict = self.items()
