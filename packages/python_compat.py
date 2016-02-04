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
except Exception:
	builtin_cmp = cmp
	def sorted(unsortedList, cmp = None, key = None, reverse = False):
		""" Sort list by either using the standard comparison method cmp()
		or, if supplied, the function comp.  The optional argument key
		is a function that returns the key to sort by - default is the
		identity function.

		>>> sorted([4, 3, 1, 5, 2])
		[1, 2, 3, 4, 5]

		>>> sorted([4, 3, 1, 5, 2], reverse = True)
		[5, 4, 3, 2, 1]

		>>> sorted([4, 3, 1, 5, 2], cmp=lambda a, b: -cmp(a, b))
		[5, 4, 3, 2, 1]

		>>> sorted(['spam', 'ham', 'cheese'], key=len)
		['ham', 'spam', 'cheese']

		>>> sorted(['spam', 'ham', 'cheese'], cmp=lambda a, b: -cmp(a, b), key=len)
		['cheese', 'spam', 'ham']
		"""
		tmp = list(unsortedList)
		if cmp is None:
			cmp = builtin_cmp

		if key and reverse:
			tmp.sort(lambda x, y: -cmp(key(x), key(y)))
		elif key and not reverse:
			tmp.sort(lambda x, y: cmp(key(x), key(y)))
		elif reverse:
			tmp.sort(lambda x, y: -cmp(x, y))
		elif not reverse:
			tmp.sort(cmp)
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
	StringBufferBase = StringIO.StringIO # its not possible to derive from cStringIO
except Exception:
	import io
	StringBuffer = io.StringIO
	StringBufferBase = io.StringIO

try:	# logging.NullHandler >= Python 2.7
	import logging
	NullHandler = logging.NullHandler
except Exception:
	class NullHandler(logging.Handler):
		def emit(self, record):
			pass

try:	# raw_input < Python 3.0
	user_input = raw_input
except Exception:
	user_input = input

try:	# itertools.imap < Python 3.0
	import itertools
	imap = itertools.imap
	lmap = map
except:
	imap = map
	lmap = lambda *args: list(map(*args))

try:	# itertools.ifilter < Python 3.0
	import itertools
	ifilter = itertools.ifilter
	lfilter = filter
except:
	ifilter = filter
	lfilter = lambda *args: list(filter(*args))

try:	# functools.lru_cache >= Python 3.2
	import functools
	lru_cache = functools.lru_cache
except Exception:
	def lru_cache(fun, maxsize = 10): # Implementation causes CPU performance hit to avoid I/O
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

__all__ = ['NullHandler', 'StringBuffer', 'StringBufferBase',
	'all', 'any', 'ifilter', 'imap', 'lfilter', 'lmap', 'lru_cache',
	'md5', 'next', 'parsedate', 'rsplit', 'set', 'sorted', 'user_input']

if __name__ == '__main__':
	import os, re, doctest, logging
	logging.basicConfig()
	doctest.testmod()
	for (root, dirs, files) in os.walk('.'):
		for fn in filter(lambda fn: fn.endswith('.py') and not fn.endswith("python_compat.py"), files):
			fn = os.path.join(root, fn)
			tmp = open(fn).read().replace('def set(', '').replace('def next(', '').replace('next()', '')
			needed = set(filter(lambda name: re.search('[^_\.a-zA-Z]%s\(' % name, tmp), __all__ + ['map', 'filter']))
			needed.update(filter(lambda name: re.search('\(%s\)' % name, tmp), __all__ + ['map', 'filter']))
			imported = set()
			for import_line in filter(lambda line: 'python_compat' in line, tmp.splitlines()):
				imported.update(map(str.strip, import_line.split(None, 3)[3].split(',')))
			if not needed and ('python_compat' in tmp):
				logging.critical('%s: python_compat import not needed!' % fn)
			for feature in needed.difference(imported):
				logging.critical('%s: missing import of "%s"' % (fn, feature))
			for feature in imported.difference(needed):
				logging.critical('%s: unnecessary import of "%s"' % (fn, feature))
