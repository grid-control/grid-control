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
from grid_control.gc_exceptions import GCError, InstallationError, UserError
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import UniqueList
from grid_control.utils.parsing import parseBool, parseType, strDict
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.table import ColumnTable, ParseableTable, RowTable
from grid_control.utils.thread_tools import TimeoutException, hang_protection
from hpfwk import clear_current_exception
from python_compat import exit_without_cleanup, get_user_input, identity, ifilter, imap, irange, lfilter, lmap, lru_cache, lzip, next, reduce, sorted, tarfile

def execWrapper(script, context = None):
	if context is None:
		context = dict()
	exec(script, context) # pylint:disable=exec-used
	return context

def QM(cond, a, b):
	if cond:
		return a
	return b

def swap(a, b):
	return (b, a)

################################################################
# Path helper functions

cleanPath = lambda x: os.path.normpath(os.path.expandvars(os.path.expanduser(x.strip())))

def getRootName(fn): # Return file name without extension
	bn = os.path.basename(str(fn)).lstrip('.')
	return QM('.' in bn, str.join('', bn.split('.')[:-1]), bn)

def pathPKG(*args):
	return cleanPath(os.path.join(os.environ['GC_PACKAGES_PATH'], *args))

def pathShare(*args, **kw):
	return pathPKG(kw.get('pkg', 'grid_control'), 'share', *args)

def resolvePaths(path, searchPaths = None, mustExist = True, ErrorClass = GCError):
	path = cleanPath(path) # replace $VAR, ~user, \ separators
	result = []
	if os.path.isabs(path):
		result.extend(sorted(glob.glob(path))) # Resolve wildcards for existing files
		if not result:
			if mustExist:
				raise ErrorClass('Could not find file "%s"' % path)
			return [path] # Return non-existing, absolute path
	else: # search relative path in search directories
		searchPaths = searchPaths or []
		for spath in UniqueList(searchPaths):
			result.extend(sorted(glob.glob(cleanPath(os.path.join(spath, path)))))
		if not result:
			if mustExist:
				raise ErrorClass('Could not find file "%s" in \n\t%s' % (path, str.join('\n\t', searchPaths)))
			return [path] # Return non-existing, relative path
	return result


def resolvePath(path, searchPaths = None, mustExist = True, ErrorClass = GCError):
	result = resolvePaths(path, searchPaths, mustExist, ErrorClass)
	if len(result) > 1:
		raise ErrorClass('Path "%s" matches multiple files:\n\t%s' % (path, str.join('\n\t', result)))
	return result[0]


def resolveInstallPath(path):
	result = resolvePaths(path, UniqueList(os.environ['PATH'].split(os.pathsep)), True, InstallationError)
	result_exe = lfilter(lambda fn: os.access(fn, os.X_OK), result) # filter executable files
	if not result_exe:
		raise InstallationError('Files matching %s:\n\t%s\nare not executable!' % (path, str.join('\n\t', result_exe)))
	return result_exe[0]


def ensureDirExists(dn, name = 'directory', ExceptionClass = GCError):
	if not os.path.exists(dn):
		try:
			os.makedirs(dn)
		except Exception:
			raise ExceptionClass('Problem creating %s "%s"' % (name, dn))
	return dn


def freeSpace(dn, timeout = 5):
	def freeSpace_int():
		if os.path.exists(dn):
			try:
				stat_info = os.statvfs(dn)
				return stat_info.f_bavail * stat_info.f_bsize / 1024**2
			except Exception:
				import ctypes
				free_bytes = ctypes.c_ulonglong(0)
				ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(dn), None, None, ctypes.pointer(free_bytes))
				return free_bytes.value / 1024**2
		return -1

	try:
		return hang_protection(freeSpace_int, timeout)
	except TimeoutException:
		sys.stderr.write(
			'Unable to get free disk space for directory %s after waiting for %d sec!\n' % (dn, timeout) +
			'The file system is probably hanging or corrupted - try to check the free disk space manually. ' +
			'Refer to the documentation to disable checking the free disk space - at your own risk')
		exit_without_cleanup(os.EX_OSERR)


################################################################
# Global state functions

def globalSetupProxy(fun, default, new = None):
	if new is not None:
		fun.setting = new
	try:
		return fun.setting
	except Exception:
		return default


def abort(new = None):
	return globalSetupProxy(abort, False, new)


################################################################
# Dictionary tools

class Result(object): # Use with caution! Compared with tuples: +25% accessing, 8x slower instantiation
	def __init__(self, **kwargs):
		self.__dict__ = kwargs
	def __repr__(self):
		return 'Result(%s)' % strDict(self.__dict__)


def mergeDicts(dicts):
	tmp = dict()
	for x in dicts:
		tmp.update(x)
	return tmp


def intersectDict(dictA, dictB):
	for keyA in list(dictA.keys()):
		if (keyA in dictB) and (dictA[keyA] != dictB[keyA]):
			dictA.pop(keyA)


def replaceDict(result, allVars, varMapping = None):
	for (virtual, real) in (varMapping or lzip(allVars.keys(), allVars.keys())):
		for delim in ['@', '__']:
			result = result.replace(delim + virtual + delim, str(allVars.get(real, '')))
	return result


def filterDict(dictType, kF = lambda k: True, vF = lambda v: True):
	def filterItems(k_v):
		return kF(k_v[0]) and vF(k_v[1])
	return dict(ifilter(filterItems, dictType.items()))


class PersistentDict(dict):
	def __init__(self, filename, delimeter = '=', lowerCaseKey = True):
		dict.__init__(self)
		self.fmt = DictFormat(delimeter)
		self.filename = filename
		keyParser = {None: QM(lowerCaseKey, lambda k: parseType(k.lower()), parseType)}
		try:
			self.update(self.fmt.parse(open(filename), keyParser = keyParser))
		except Exception:
			clear_current_exception()
		self.olddict = self.items()

	def get(self, key, default = None, autoUpdate = True):
		value = dict.get(self, key, default)
		if autoUpdate:
			self.write({key: value})
		return value

	def write(self, newdict = None, update = True):
		if not update:
			self.clear()
		self.update(newdict or {})
		if dict(self.olddict) == dict(self.items()):
			return
		try:
			if self.filename:
				safeWrite(open(self.filename, 'w'), self.fmt.format(self))
		except Exception:
			raise GCError('Could not write to file %s' % self.filename)
		self.olddict = self.items()


################################################################
# File IO helper

def safeWrite(fp, content):
	fp.writelines(content)
	fp.truncate()
	fp.close()


def renameFile(old, new):
	if os.path.exists(new):
		os.unlink(new)
	os.rename(old, new)


def removeFiles(args):
	for item in args:
		try:
			if os.path.isdir(item):
				os.rmdir(item)
			else:
				os.unlink(item)
		except Exception:
			clear_current_exception()


################################################################
# String manipulation

def optSplit(opt, delim, empty = ''):
	""" Split option strings into fixed tuples
	>>> optSplit('abc : ghi # def', ['#', ':'])
	('abc', 'def', 'ghi')
	>>> optSplit('abc:def', '::')
	('abc', 'def', '')
	"""
	def getDelimeterPart(oldResult, prefix):
		try:
			tmp = oldResult[0].split(prefix)
			new = tmp.pop(1)
			try: # Find position of other delimeters in string
				otherDelim = min(ifilter(lambda idx: idx >= 0, imap(new.find, delim)))
				tmp[0] += new[otherDelim:]
			except Exception:
				otherDelim = None
			return [str.join(prefix, tmp)] + oldResult[1:] + [new[:otherDelim]]
		except Exception:
			return oldResult + ['']
	result = imap(str.strip, reduce(getDelimeterPart, delim, [opt]))
	return tuple(imap(lambda x: QM(x == '', empty, x), result))

################################################################

class TwoSidedIterator(object):
	def __init__(self, allInfo):
		(self.allInfo, self.left, self.right) = (allInfo, 0, 0)
	def forward(self):
		while self.left + self.right < len(self.allInfo):
			self.left += 1
			yield self.allInfo[self.left - 1]
	def backward(self):
		while self.left + self.right < len(self.allInfo):
			self.right += 1
			yield self.allInfo[len(self.allInfo) - self.right]


def accumulate(iterable, empty, doEmit, doAdd = lambda item, buffer: True, opAdd = operator.add):
	buf = empty
	for item in iterable:
		if doAdd(item, buf):
			buf = opAdd(buf, item)
		if doEmit(item, buf):
			if buf != empty:
				yield buf
			buf = empty
	if buf != empty:
		yield buf


def wrapList(value, length, delimLines = ',\n', delimEntries = ', '):
	counter = lambda item, buffer: len(item) + sum(imap(len, buffer)) + 2*len(buffer) > length
	wrapped = accumulate(value, [], counter, opAdd = lambda x, y: x + [y])
	return str.join(delimLines, imap(lambda x: str.join(delimEntries, x), wrapped))


def DiffLists(oldList, newList, keyFun, changedFkt, isSorted = False):
	(listAdded, listMissing, listChanged) = ([], [], [])
	if not isSorted:
		(newList, oldList) = (sorted(newList, key = keyFun), sorted(oldList, key = keyFun))
	(newIter, oldIter) = (iter(newList), iter(oldList))
	(new, old) = (next(newIter, None), next(oldIter, None))
	while True:
		if (new is None) or (old is None):
			break
		keyNew = keyFun(new)
		keyOld = keyFun(old)
		if keyNew < keyOld: # new[npos] < old[opos]
			listAdded.append(new)
			new = next(newIter, None)
		elif keyNew > keyOld: # new[npos] > old[opos]
			listMissing.append(old)
			old = next(oldIter, None)
		else: # new[npos] == old[opos] according to *active* comparison
			changedFkt(listAdded, listMissing, listChanged, old, new)
			(new, old) = (next(newIter, None), next(oldIter, None))
	while new is not None:
		listAdded.append(new)
		new = next(newIter, None)
	while old is not None:
		listMissing.append(old)
		old = next(oldIter, None)
	return (listAdded, listMissing, listChanged)


def splitBlackWhiteList(bwfilter):
	blacklist = lmap(lambda x: x[1:], ifilter(lambda x: x.startswith('-'), bwfilter or []))
	whitelist = lfilter(lambda x: not x.startswith('-'), bwfilter or [])
	return (blacklist, whitelist)


class DictFormat(object):
	# escapeString = escape '"', '$'
	# types = preserve type information
	def __init__(self, delimeter = '=', escapeString = False):
		self.delimeter = delimeter
		self.escapeString = escapeString

	# Parse dictionary lists
	def parse(self, lines, keyParser = None, valueParser = None):
		keyParser = keyParser or {}
		valueParser = valueParser or {}
		defaultKeyParser = keyParser.get(None, lambda k: parseType(k.lower()))
		defaultValueParser = valueParser.get(None, parseType)
		data = {}
		doAdd = False
		currentline = ''
		try:
			lines = lines.splitlines()
		except Exception:
			clear_current_exception()
		for line in lines:
			try:
				if not isinstance(line, str):
					line = line.decode('utf-8')
				if self.escapeString:
					# Switch accumulate on/off when odd number of quotes found
					if (line.count('"') - line.count('\\"')) % 2 == 1:
						doAdd = not doAdd
					currentline += line
					if doAdd:
						continue
				else:
					currentline = line
				# split at first occurence of delimeter and strip spaces around
				key_value_list = lmap(str.strip, currentline.split(self.delimeter, 1))
				if len(key_value_list) == 2:
					key, value = key_value_list
					currentline = ''
				else: # in case no delimeter was found
					currentline = ''
					continue
				if self.escapeString:
					value = value.strip('"').replace('\\"', '"').replace('\\$', '$')
				key = keyParser.get(key, defaultKeyParser)(key)
				data[key] = valueParser.get(key, defaultValueParser)(value) # do .encode('utf-8') ?
			except Exception:
				raise GCError('Invalid dict format in %s' % repr(line))
		if doAdd:
			raise GCError('Invalid dict format in %s' % repr(lines))
		return data

	# Format dictionary list
	def format(self, entries, printNone = False, fkt = lambda x_y_z: x_y_z, format = '%s%s%s\n'):
		result = []
		for key in sorted(entries.keys()):
			value = entries[key]
			if value is None and not printNone:
				continue
			if self.escapeString and isinstance(value, str):
				value = '"%s"' % str(value).replace('"', '\\"').replace('$', '\\$')
				lines = value.splitlines()
				result.append(format % fkt((key, self.delimeter, lines[0])))
				result.extend(imap(lambda x: x + '\n', lines[1:]))
			else:
				result.append(format % fkt((key, self.delimeter, value)))
		return result


def matchFileName(fn, patList):
	match = None
	for p in patList:
		if fnmatch.fnmatch(fn, p.lstrip('-')):
			match = not p.startswith('-')
	return match


def matchFiles(pathRoot, pattern, pathRel = ''):
	# Return (root, fn, state) - state: None == dir, True/False = (un)checked file, other = filehandle
	yield (pathRoot, pathRel, None)
	for name in imap(lambda x: os.path.join(pathRel, x), os.listdir(os.path.join(pathRoot, pathRel))):
		match = matchFileName(name, pattern)
		pathAbs = os.path.join(pathRoot, name)
		if match is False:
			continue
		elif os.path.islink(pathAbs): # Not excluded symlinks
			yield (pathAbs, name, True)
		elif os.path.isdir(pathAbs): # Recurse into directories
			if match is True: # (backwards compat: add parent directory - not needed?)
				yield (pathAbs, name, True)
			for result in matchFiles(pathRoot, QM(match is True, ['*'], pattern), name):
				yield result
		elif match is True: # Add matches
			yield (pathAbs, name, True)


def genTarball(outFile, fileList):
	tar = tarfile.open(outFile, 'w:gz')
	activity = Activity('Generating tarball')
	for (pathAbs, pathRel, pathStatus) in fileList:
		if pathStatus is True: # Existing file
			tar.add(pathAbs, pathRel, recursive = False)
		elif pathStatus is False: # Existing file
			if not os.path.exists(pathAbs):
				raise UserError('File %s does not exist!' % pathRel)
			tar.add(pathAbs, pathRel, recursive = False)
		elif pathStatus is None: # Directory
			activity.update('Generating tarball: %s' % pathRel)
		else: # File handle
			info, handle = pathStatus.getTarInfo()
			info.mtime = time.time()
			info.mode = stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP + stat.S_IROTH
			if info.name.endswith('.sh') or info.name.endswith('.py'):
				info.mode += stat.S_IXUSR + stat.S_IXGRP + stat.S_IXOTH
			tar.addfile(info, handle)
			handle.close()
	activity.finish()
	tar.close()


def getVersion():
	try:
		proc_ver = LocalProcess('svnversion', '-c', pathPKG())
		version = proc_ver.get_output(timeout = 10).strip()
		if version != '':
			assert(lfilter(str.isdigit, version))
			proc_branch = LocalProcess('svn info', pathPKG())
			if 'stable' in proc_branch.get_output(timeout = 10):
				return '%s - stable' % version
			return '%s - testing' % version
	except Exception:
		clear_current_exception()
	return __import__('grid_control').__version__ + ' or later'
getVersion = lru_cache(getVersion)


def wait(timeout):
	activity = Activity('Waiting', parent = 'root')
	for remaining in irange(timeout, 0, -1):
		if abort():
			return False
		if (remaining == timeout) or (remaining < 5) or (remaining % 5 == 0):
			activity.update('Waiting for %d seconds' % remaining)
		time.sleep(1)
	activity.finish()
	return True


def printTabular(head, data, fmtString = '', fmt = None):
	if printTabular.mode == 'parseable':
		return ParseableTable(head, data, '|')
	elif printTabular.mode == 'longlist':
		return RowTable(head, data, fmt, printTabular.wraplen)
	return ColumnTable(head, data, fmtString, fmt, printTabular.wraplen)
printTabular.wraplen = 100
printTabular.mode = 'default'


def getUserInput(text, default, choices, parser = identity):
	while True:
		handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
		try:
			userinput = get_user_input('%s %s: ' % (text, '[%s]' % default))
		except Exception:
			sys.stdout.write('\n') # continue on next line
			raise
		signal.signal(signal.SIGINT, handler)
		if userinput == '':
			return parser(default)
		if parser(userinput) is not None:
			return parser(userinput)
		valid = str.join(', ', imap(lambda x: '"%s"' % x, choices[:-1]))
		logging.getLogger('console').critical('Invalid input! Answer with %s or "%s"', valid, choices[-1])


def getUserBool(text, default):
	return getUserInput(text, QM(default, 'yes', 'no'), ['yes', 'no'], parseBool)


def deprecated(text):
	sys.stderr.write('%s\n[DEPRECATED] %s\n' % (open(pathShare('fail.txt'), 'r').read(), text))
	if not getUserBool('Do you want to continue?', False):
		sys.exit(os.EX_TEMPFAIL)


def exitWithUsage(usage, msg = None, show_help = True):
	sys.stderr.write('Syntax: %s\n%s' % (usage, QM(show_help, 'Use --help to get a list of options!\n', '')))
	sys.stderr.write(QM(msg, '%s\n' % msg, ''))
	sys.exit(os.EX_USAGE)


def ping_host(host):
	proc = LocalProcess('ping', '-Uqnc', 1, '-W', 1, host)
	try:
		tmp = proc.get_output(timeout = 1).splitlines()
		assert(tmp[-1].endswith('ms'))
		return float(tmp[-1].split('/')[-2]) / 1000.
	except Exception:
		return None


def display_selection(log, items_before, items_after, message, formatter):
	if len(items_before) != len(items_after):
		log.log(logging.DEBUG, message, (len(items_before) - len(items_after)))
		for item in items_before:
			if item in items_after:
				log.log(logging.DEBUG1, ' * %s', formatter(item))
			else:
				log.log(logging.DEBUG1, '   %s', formatter(item))


def filter_processors(processorList, id_fun = lambda proc: proc.__class__.__name__):
	(result, processorIDs) = ([], [])
	for proc in processorList:
		if proc.enabled() and (id_fun(proc) not in processorIDs):
			result.append(proc)
			processorIDs.append(id_fun(proc))
	return result


def prune_processors(do_prune, processorList, log, message, formatter = None, id_fun = None):
	def get_class_name(proc):
		return proc.__class__.__name__
	selected = filter_processors(processorList, id_fun or get_class_name)
	display_selection(log, processorList, selected, message, formatter or get_class_name)
	return selected
