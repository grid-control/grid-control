#-#  Copyright 2010-2015 Karlsruhe Institute of Technology
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

import os, sys, itertools

def identity(x):
	return x

try:	# itemgetter >= Python 2.4
	from operator import itemgetter
except Exception:
	def itemgetter(*items):
		if len(items) == 1:
			item = items[0]
			def g(obj):
				return obj[item]
		else:
			def g(obj):
				return tuple(imap(lambda item: obj[item], items))
		return g

try:	# str.rsplit >= Python 2.4
	rsplit = str.rsplit
except Exception:
	def rsplit(x, sep, maxsplit = None):
		""" Split from the right side
		>>> rsplit('abc', '.', 1)
		['abc']
		>>> rsplit('a.b.c.d.e.f.g', '.', 1)
		['a.b.c.d.e.f', 'g']
		>>> rsplit('a.b.c.d.e.f.g', '.', 2)
		['a.b.c.d.e', 'f', 'g']
		"""
		tmp = x.split(sep)
		if len(tmp) > 1:
			return [str.join(sep, tmp[:len(tmp)-maxsplit])] + tmp[len(tmp)-maxsplit:]
		return tmp

try:	# set >= Python 2.4
	set = set
except Exception:
	import sets
	set = sets.Set

try:	# sorted >= Python 2.4
	sorted = sorted
	def sort_inplace(unsortedList, key = identity):
		unsortedList.sort(key = key)
except Exception:
	builtin_cmp = cmp
	def sort_inplace(unsortedList, key = identity):
		unsortedList.sort(lambda a, b: builtin_cmp(key(a), key(b)))
	def sorted(unsortedList, key = None, reverse = False):
		""" Sort list by either using the function key that returns
		the key to sort by - default is the identity function.
		>>> sorted([4, 3, 1, 5, 2])
		[1, 2, 3, 4, 5]
		>>> sorted([4, 3, 1, 5, 2], reverse = True)
		[5, 4, 3, 2, 1]
		>>> sorted(['spam', 'ham', 'cheese'], key=len)
		['ham', 'spam', 'cheese']
		"""
		tmp = list(unsortedList)
		if key:
			sort_inplace(tmp, key = key)
		else:
			tmp.sort()
		if reverse:
			tmp.reverse()
		return tmp

try:	# hashlib >= Python 2.5
	import hashlib
	md5 = hashlib.md5
except Exception:
	import md5
	md5 = md5.md5

try:	# any >= Python 2.5
	any = any
except Exception:
	def any(iterable):
		for element in iterable:
			if element:
				return True
		return False

try:	# all >= Python 2.5
	all = all
except Exception:
	def all(iterable):
		for element in iterable:
			if not element:
				return False
		return True

try:	# email.utils >= Python 2.5
	from email.utils import parsedate
except ImportError:
	from email.Utils import parsedate

try:	# relpath >= Python 2.6
	relpath = os.path.relpath
except Exception:
	def relpath(path, start=None):
		if not path:
			raise ValueError("no path specified")
		start_list = lfilter(identity, os.path.abspath(start or os.path.curdir).split(os.path.sep))
		path_list = lfilter(identity, os.path.abspath(path).split(os.path.sep))
		i = len(os.path.commonprefix([start_list, path_list]))
		rel_list = [os.path.pardir] * (len(start_list)-i) + path_list[i:]
		if not rel_list:
			return os.path.curdir
		return os.path.join(*rel_list)

try:	# next >= Python 2.6
	next = next
except Exception:
	def next(it, *default):
		try:
			return it.next()
		except Exception:
			if default:
				return default[0]
			raise

try:	# io >= Python 2.6 (unicode)
	import StringIO, cStringIO
	StringBuffer = cStringIO.StringIO
	BytesBufferBase = StringIO.StringIO # its not possible to derive from cStringIO
	BytesBuffer = StringBuffer
	bytes2str = identity
	str2bytes = identity
except Exception:
	import io
	StringBuffer = io.StringIO
	BytesBufferBase = io.BytesIO
	BytesBuffer = io.BytesIO
	bytes2str = lambda x: x.decode('utf-8')
	str2bytes = lambda x: x.encode('utf-8')

try:	# logging.NullHandler >= Python 2.7
	import logging
	NullHandler = logging.NullHandler
except Exception:
	class NullHandler(logging.Handler):
		def emit(self, record):
			pass

try:	# unicode < Python 3.0
	unicode = unicode
except Exception:
	unicode = str

try:	# raw_input < Python 3.0
	user_input = raw_input
except Exception:
	user_input = input

try:	# itertools.imap < Python 3.0
	imap = itertools.imap
	lmap = map
	ismap = itertools.starmap
except Exception:
	imap = map
	lmap = lambda *args: list(imap(*args))
	ismap = itertools.starmap
lsmap = lambda *args: list(ismap(*args))

try:	# itertools.ifilter < Python 3.0
	ifilter = itertools.ifilter
	lfilter = filter
except Exception:
	ifilter = filter
	def lfilter(*args):
		return list(filter(*args))

try:	# itertools.izip < Python 3.0
	izip = itertools.izip
	lzip = zip
except Exception:
	izip = zip
	lzip = lambda *args: list(zip(*args))

try:	# xrange < Python 3.0
	irange = xrange
	lrange = range
except Exception:
	irange = range
	lrange = lambda *args: list(range(*args))

try:	# reduce < Python 3.0
	reduce = reduce
except Exception:
	from functools import reduce

try:	# functools.lru_cache >= Python 3.2
	import functools
	lru_cache = functools.lru_cache(30)
except Exception:
	def lru_cache(fun, maxsize = 30): # Implementation causes CPU performance hit to avoid I/O
		def funProxy(*args, **kargs):
			idx = None
			for (i, value) in enumerate(funProxy.cache):
				if value[0] == (args, kargs):
					idx = i
			if idx is not None:
				(key, item) = funProxy.cache.pop(idx)
			else:
				item = funProxy.fun(*args, **kargs)
			funProxy.cache.insert(0, ((args, kargs), item))
			while len(funProxy.cache) > maxsize:
				funProxy.cache.pop()
			return item
		(funProxy.fun, funProxy.cache) = (fun, [])
		return funProxy

if sys.version_info[0:2] < (2, 7, 10):	# missing features in json < Python 2.7.10
	json = __import__('python_compat_json')
else:
	import json

if sys.version_info[0:2] < (2, 7):	# missing features in json / tarfile / urllib2 < Python 2.7
	tarfile = __import__('python_compat_tarfile')
	urllib2 = __import__('python_compat_urllib2')
elif sys.version_info[0] < 3:
	import tarfile, urllib2
else:
	import tarfile
	urllib2 = None

if sys.version_info[0] < 3:	# unicode encoding <= Python 3
	md5_hex = lambda value: md5(value).hexdigest()
else:
	md5_hex = lambda value: md5(str(value).encode('utf-8')).hexdigest()

__all__ = ['BytesBuffer', 'BytesBufferBase', 'NullHandler', 'StringBuffer',
	'all', 'any', 'bytes2str', 'identity', 'itemgetter', 'lru_cache',
	'ifilter', 'imap', 'irange', 'ismap', 'izip', 'json',
	'lfilter', 'lmap', 'lrange', 'lsmap', 'lzip', 'md5', 'md5_hex',
	'next', 'parsedate', 'relpath', 'rsplit', 'set',
	'sort_inplace', 'sorted', 'str2bytes', 'tarfile', 'urllib2', 'unicode', 'user_input']

if __name__ == '__main__':
	import re, doctest, logging
	logging.basicConfig()
	doctest.testmod()
	for (root, dirs, files) in os.walk('.'):
		if root.startswith('./.') or ('source_check' in root):
			continue
		for fn in filter(lambda fn: fn.endswith('.py') and not ("python_compat" in fn), files):
			fn = os.path.join(root, fn)
			tmp = open(fn).read().replace('\'zip(', '').replace('def set(', '').replace('type(range(', '')
			tmp = tmp.replace('def filter(', '').replace('def next(', '').replace('next()', '')
			tmp = tmp.replace('python_compat_popen2', '')
			builtin_avoid = ['basestring', 'cmp', 'filter', 'map', 'range', 'reduce', 'xrange', 'zip']
			needed = set()
			for pattern in ['[^_\'\/\.a-zA-Z]%s\(', '[^_\'\/\.a-zA-Z]%s\.', '\(%s[,\)]', ', %s[,\)]', ' = %s[,\)]']:
				needed.update(filter(lambda name: re.search(pattern % name, tmp), __all__ + builtin_avoid))
			imported = set()
			for iline in filter(lambda line: 'python_compat' in line, tmp.splitlines()):
				try:
					imported.update(map(str.strip, iline.split(None, 3)[3].split(',')))
				except Exception:
					raise Exception('Unable to parse %r:%r' % (fn, iline))
			if not needed and ('python_compat' in tmp):
				logging.critical('%s: python_compat import not needed!' % fn)
			for feature in needed.difference(imported):
				logging.critical('%s: missing import of "%s"' % (fn, feature))
			for feature in imported.difference(needed):
				logging.critical('%s: unnecessary import of "%s"' % (fn, feature))
