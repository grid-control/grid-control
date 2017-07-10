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

# pylint:disable=invalid-name,wrong-import-position
import os, sys, logging, itertools


class ZipExhausted(Exception):
	pass


def identity(value):
	return value


def iidfilter(value):
	return ifilter(identity, value)


def lidfilter(value):
	return lfilter(identity, value)


def resolve_fun(*args):
	_log = logging.getLogger('python_compat')
	module_map = {'<builtin>': ['__builtin__', 'builtins'],
		'<builtin-py2>': ['__builtin__'], '<builtin-py3>': ['builtins']}
	for location in args:
		if isinstance(location, str):
			module_name_raw, member_name = location.split(':', 1)
			for module_name in module_map.get(module_name_raw, [module_name_raw]):
				try:
					result = __import__(module_name, {}, {}, [module_name.split('.')[-1]])
					if member_name:
						for member_name_part in member_name.split('.'):
							result = getattr(result, member_name_part)
				except Exception:
					if 'Condition' in location:
						raise
					continue
				_log.debug('using %s', location)
				return result
		else:
			return location  # function
	raise Exception('Unable to find function in ' + repr(args))


def unspecified(value):
	return value == unspecified


def when_unspecified(value, result):
	if value == unspecified:
		return result
	return value


class GroupIterator(object):
	def __init__(self, iterable, key=None):
		self._key_fun = key or identity
		self._key_ref = self._key_cur = self._value = unspecified
		self._iterator = iter(iterable)

	def __iter__(self):
		return self

	def __next__(self):
		return self.next()

	def next(self):
		while self._key_cur == self._key_ref:
			self._value = next(self._iterator)
			self._key_cur = self._key_fun(self._value)
		self._key_ref = self._key_cur
		return (self._key_cur, self._group_iter(self._key_ref))

	def _group_iter(self, key_ref):
		while self._key_cur == key_ref:
			yield self._value
			self._value = next(self._iterator)
			self._key_cur = self._key_fun(self._value)


def _all(iterable):
	for element in iterable:
		if not element:
			return False
	return True


def _any(iterable):
	for element in iterable:
		if element:
			return True
	return False


def _get_listified(fun):
	def _function(*args):
		return list(fun(*args))
	return _function


def _ichain(iterables):
	return itertools.chain(*iterables)


def _itemgetter(*items):
	if len(items) == 1:
		item = items[0]

		def _get(obj):
			return obj[item]
	else:
		def _get(obj):
			return tuple(imap(obj.__getitem__, items))
	return _get


def _izip_longest(*args, **kwargs):
	fillvalue = kwargs.get('fillvalue')
	counter = [len(args) - 1]

	def _sentinel():
		if not counter[0]:
			raise ZipExhausted
		counter[0] -= 1
		yield fillvalue
	fillers = itertools.repeat(fillvalue)
	iterators = lmap(lambda it: itertools.chain(it, _sentinel(), fillers), args)
	try:
		while iterators:
			yield tuple(imap(lambda it: it.next(), iterators))
	except ZipExhausted:
		pass


def _lru_cache(maxsize=128):
	def _decorating_function(user_function):
		def _fun_proxy(*args, **kargs):
			idx = None
			for (cidx, value) in enumerate(_fun_proxy.cache):
				if value[0] == (args, kargs):
					idx = cidx
			if idx is not None:
				(_, item) = _fun_proxy.cache.pop(idx)
			else:
				item = _fun_proxy.fun(*args, **kargs)
			_fun_proxy.cache.insert(0, ((args, kargs), item))
			while len(_fun_proxy.cache) > maxsize:
				_fun_proxy.cache.pop()
			return item
		(_fun_proxy.fun, _fun_proxy.cache) = (user_function, [])
		return _fun_proxy
	return _decorating_function


def _next(iterable, default=unspecified, *args):
	try:
		return iterable.next()
	except Exception:
		if not unspecified(default):
			return default
		raise


def _partial(fun, *args, **kwargs):
	def _partial_fun(*fun_args, **fun_kwargs):
		new_kwargs = dict(kwargs)
		new_kwargs.update(fun_kwargs)
		return fun(*(args + fun_args), **new_kwargs)
	return _partial_fun


def _relpath(path, start=None):
	if not path:
		raise ValueError('no path specified')
	start_list = lidfilter(os.path.abspath(start or os.path.curdir).split(os.path.sep))
	path_list = lidfilter(os.path.abspath(path).split(os.path.sep))
	common_len = len(os.path.commonprefix([start_list, path_list]))
	rel_list = [os.path.pardir] * (len(start_list) - common_len) + path_list[common_len:]
	if not rel_list:
		return os.path.curdir
	return os.path.join(*rel_list)


def _rsplit(value, sep, maxsplit=None):
	""" Split from the right side
	>>> rsplit('', None, 1)
	[]
	>>> rsplit('', '.', 1)
	['']
	>>> rsplit('abc', '.', 1)
	['abc']
	>>> rsplit('a.b.c.d.e.f.g', '.', 1)
	['a.b.c.d.e.f', 'g']
	>>> rsplit('a.b.c.d.e.f.g', '.', 2)
	['a.b.c.d.e', 'f', 'g']
	"""
	str_parts = value.split(sep)
	if (maxsplit is not None) and (len(str_parts) > 1):
		return [str.join(sep, str_parts[:-maxsplit])] + str_parts[-maxsplit:]
	return str_parts


def _sorted(unsorted_iterable, key=None, reverse=False):
	""" Sort list by either using the function key that returns
	the key to sort by - default is the identity function.
	>>> sorted([4, 3, 1, 5, 2])
	[1, 2, 3, 4, 5]
	>>> sorted([4, 3, 1, 5, 2], reverse=True)
	[5, 4, 3, 2, 1]
	>>> sorted(['spam', 'ham', 'cheese'], key=len)
	['ham', 'spam', 'cheese']
	"""
	unsorted_list = list(unsorted_iterable)
	sort_inplace(unsorted_list, key=key)
	if reverse:
		unsorted_list.reverse()
	return unsorted_list

all = resolve_fun('<builtin>:all', _all)  # >= py-2.5
any = resolve_fun('<builtin>:any', _any)  # >= py-2.5
BytesBuffer = resolve_fun('cStringIO:StringIO', 'io:BytesIO')  # < py-2.6
exit_without_cleanup = resolve_fun('os:_exit')
get_user_input = resolve_fun('<builtin-py2>:raw_input', '<builtin-py3>:input')  # < py-3.0
groupby = resolve_fun('itertools:groupby', GroupIterator)  # >= py-2.4
ichain = resolve_fun('itertools:chain.from_iterable', _ichain)  # >= py-2.6
ifilter = resolve_fun('itertools:ifilter', '<builtin-py3>:filter')  # < py-3.0
imap = resolve_fun('itertools:imap', '<builtin-py3>:map')  # < py-3.0
irange = resolve_fun('<builtin-py2>:xrange', '<builtin-py3>:range')  # < py-3.0
ismap = resolve_fun('itertools:starmap')
itemgetter = resolve_fun('operator:itemgetter', _itemgetter)  # >= py-2.4
izip_longest = resolve_fun('itertools:izip_longest', _izip_longest)  # >= py-2.6
izip = resolve_fun('itertools:izip', '<builtin-py3>:zip')  # < py-3.0
lchain = _get_listified(ichain)
lru_cache = resolve_fun('functools:lru_cache', _lru_cache)  # >= py-3.2
lsmap = _get_listified(ismap)
md5 = resolve_fun('hashlib:md5', 'md5:md5')  # >= py-2.5
next = resolve_fun('<builtin>:next', _next)  # >= py-2.6
parsedate = resolve_fun('email.utils:parsedate', 'email.Utils:parsedate')  # >= py-2.5
partial = resolve_fun('functools:partial', _partial)  # >= py-2.5
reduce = resolve_fun('<builtin-py2>:reduce', 'functools:reduce')  # < py-3.0
relpath = resolve_fun('os.path:relpath', _relpath)  # >= py-2.6
rsplit = resolve_fun('<builtin>:str.rsplit', _rsplit)  # >= py-2.4
set = resolve_fun('<builtin>:set', 'sets:Set')  # >= py-2.4
sorted = resolve_fun('<builtin>:sorted', _sorted)  # >= py-2.4
StringBuffer = resolve_fun('cStringIO:StringIO', 'io:StringIO')  # < py-2.6
unicode = resolve_fun('<builtin-py2>:unicode', '<builtin-py3>:str')  # unicode < py-3.0


try:  # pylint has issues with inheriting from something returned by resolve_fun
	import StringIO
	BytesBufferBase = StringIO.StringIO
except Exception:
	import io
	BytesBufferBase = io.BytesIO
# BytesBufferBase = resolve_fun('StringIO:StringIO', 'io:BytesIO')  # cStringIO is a final class


if sys.version_info[0] < 3:
	# moved to iterator output for < py-3.0
	lfilter = filter
	lmap = map
	lrange = range
	lzip = zip

	# bytes vs. str vs. unicode conversion
	bytes2str = identity
	str2bytes = identity

	def md5_hex(value):
		return md5(value).hexdigest()
else:
	# moved to iterator output for < py-3.0
	lfilter = _get_listified(filter)
	lmap = _get_listified(map)
	lrange = _get_listified(range)
	lzip = _get_listified(zip)

	# bytes vs. str vs. unicode conversion
	def bytes2str(value):
		return value.decode('utf-8')

	def str2bytes(value):
		return value.encode('utf-8')

	def md5_hex(value):
		return md5(str2bytes(value)).hexdigest()


if sys.version_info[0:2] < (2, 4):  # sort by key >= py-2.4
	builtin_cmp = resolve_fun('<builtin-py2>:cmp')

	def sort_inplace(unsorted_iterable, key=None):
		if key is None:
			unsorted_iterable.sort()
		else:
			unsorted_iterable.sort(lambda a, b: builtin_cmp(key(a), key(b)))
else:
	sort_inplace = list.sort


if sys.version_info[0:2] < (2, 7, 10):  # missing module or features in json < py-2.7.10
	json = __import__('python_compat_json')
else:
	import json


if sys.version_info[0:2] < (2, 7):  # missing features in tarfile < py-2.7
	tarfile = __import__('python_compat_tarfile')
else:
	import tarfile


__all__ = ['all', 'any', 'bytes2str', 'BytesBuffer', 'BytesBufferBase', 'exit_without_cleanup',
	'get_user_input', 'ichain', 'identity', 'ifilter', 'iidfilter', 'imap', 'irange', 'ismap',
	'itemgetter', 'izip', 'izip_longest', 'json', 'lchain', 'lfilter', 'lidfilter', 'lmap', 'lrange',
	'lru_cache', 'lsmap', 'lzip', 'md5', 'md5_hex', 'next', 'parsedate', 'partial', 'reduce',
	'relpath', 'resolve_fun', 'rsplit', 'set', 'sort_inplace', 'sorted', 'str2bytes', 'StringBuffer',
	'tarfile', 'unicode', 'unspecified', 'when_unspecified']


if __name__ == '__main__':
	import re, doctest
	logging.basicConfig()
	doctest.testmod()
	pattern_list = [
		r' %s,', r'[^_\'\/\.a-zA-Z]%s\(', r'[^_\'\/\.a-zA-Z]%s\.',
		r'\(%s[,\)]', r', %s[,\)]', r' = %s[,\)\n]', r'=%s[^_\'\/\.a-zA-Z]'
	]
	builtin_avoid = ['basestring', 'cmp', 'filter', 'map', 'range', 'reduce', 'xrange', 'zip']
	fun_re_list = lchain(imap(lambda pattern: imap(lambda fun, pattern=pattern:
		(fun, re.compile(pattern % fun)), __all__ + builtin_avoid), pattern_list))
	for (root, dirs, files) in os.walk('.'):
		if root.startswith('./.') or ('source_check' in root):
			continue
		for fn in ifilter(lambda fn: fn.endswith('.py'), files):
			if ('python_compat' in fn) or ('setup.py' in fn):
				continue
			fn = os.path.join(root, fn)
			tmp = open(fn).read().replace('\'zip(', '').replace('def set(', '').replace('type(range(', '')
			tmp = tmp.replace('def filter(', '').replace('def next(', '').replace('next()', '')
			tmp = tmp.replace('python_compat_popen2', '')
			needed = set()
			for (fun_name, re_matcher) in fun_re_list:
				if re_matcher.search(tmp):
					needed.add(fun_name)
			imported = set()
			for iline in ifilter(lambda line: 'python_compat ' in line, tmp.splitlines()):
				try:
					imported.update(imap(lambda i: i.split()[0].strip(), iline.split(None, 3)[3].split(',')))
				except Exception:
					raise Exception('Unable to parse %r:%r' % (fn, iline))
			if not needed and ('python_compat' in tmp):
				logging.critical('%s: python_compat import not needed!', fn)
			for feature in needed.difference(imported):
				logging.critical('%s: missing import of %s', fn, repr(feature))
			for feature in imported.difference(needed):
				logging.critical('%s: unnecessary import of %s', fn, repr(feature))
