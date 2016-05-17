# | Copyright 2016 Karlsruhe Institute of Technology
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

import re, logging
from python_compat import imap, ismap, izip, lmap

class Table(object):
	pass


class ConsoleTable(Table):
	def __init__(self):
		Table.__init__(self)
		self._log = logging.getLogger('user')

	def _write_line(self, msg):
		self._log.info(msg)


class ParseableTable(ConsoleTable):
	def __init__(self, head, data, delimeter = '|'):
		ConsoleTable.__init__(self)
		head = list(head)
		self._delimeter = delimeter
		self._write_line(str.join(self._delimeter, imap(lambda x: x[1], head)))
		for entry in data:
			if isinstance(entry, dict):
				self._write_line(str.join(self._delimeter, imap(lambda x: str(entry.get(x[0], '')), head)))


class RowTable(ConsoleTable):
	def __init__(self, head, data, fmt = None, wrapLen = 100):
		ConsoleTable.__init__(self)
		self._log.info('')
		head = list(head)
		def getHeadName(key, name):
			return name
		maxhead = max(imap(len, ismap(getHeadName, head)))
		fmt = fmt or {}
		showLine = False
		for entry in data:
			if isinstance(entry, dict):
				if showLine:
					self._write_line(('-' * (maxhead + 2)) + '-+-' + '-' * min(30, wrapLen - maxhead - 10))
				for (key, name) in head:
					self._write_line(name.rjust(maxhead + 2) + ' | ' + str(fmt.get(key, str)(entry.get(key, ''))))
				showLine = True
			elif showLine:
				self._write_line(('=' * (maxhead + 2)) + '=+=' + '=' * min(30, wrapLen - maxhead - 10))
				showLine = False
		self._log.info('')


class ColumnTable(ConsoleTable):
	def __init__(self, head, data, fmtString = '', fmt = None, wrapLen = 100):
		ConsoleTable.__init__(self)
		self._wrapLen = wrapLen
		head = list(head)
		justFun = self._get_just_fun_dict(head, fmtString)
		# return formatted, but not yet aligned entries; len dictionary; just function
		(entries, lendict, just) = self._format_data(head, data, justFun, fmt or {})
		(headwrap, lendict) = self._wrap_head(head, lendict)

		def getKeyPaddedName(key, name):
			return (key, name.center(lendict[key]))
		headentry = dict(ismap(getKeyPaddedName, head))
		self._log.info('')
		self._print_table(headwrap, headentry, entries, just, lendict)
		self._log.info('')

	def _print_table(self, headwrap, headentry, entries, just, lendict):
		for (keys, entry) in self._wrap_formatted_data(headwrap, [headentry, '='] + entries):
			if isinstance(entry, str):
				decor = lambda x: '%s%s%s' % (entry, x, entry)
				self._write_line(decor(str.join(decor('+'), lmap(lambda key: entry * lendict[key], keys))))
			else:
				self._write_line(' %s ' % str.join(' | ', imap(lambda key: just(key, entry.get(key, '')), keys)))

	def _get_just_fun_dict(self, head, fmtString):
		justFunDict = { 'l': str.ljust, 'r': str.rjust, 'c': str.center }
		# justFun = {id1: str.center, id2: str.rjust, ...}
		def getKeyFormat(headEntry, fmtString):
			return (headEntry[0], justFunDict[fmtString])
		return dict(ismap(getKeyFormat, izip(head, fmtString)))

	def _format_data(self, head, data, justFun, fmt):
		# adjust to lendict of column (considering escape sequence correction)
		strippedlen = lambda x: len(re.sub('\033\\[\\d*(;\\d*)*m', '', x))
		just = lambda key, x: justFun.get(key, str.rjust)(x, lendict[key] + len(x) - strippedlen(x))

		def getKeyLen(key, name):
			return (key, len(name))
		lendict = dict(ismap(getKeyLen, head))

		result = []
		for entry in data:
			if isinstance(entry, dict):
				tmp = {}
				for key, _ in head:
					tmp[key] = str(fmt.get(key, str)(entry.get(key, '')))
					lendict[key] = max(lendict[key], strippedlen(tmp[key]))
				result.append(tmp)
			else:
				result.append(entry)
		return (result, lendict, just)

	def _getGoodPartition(self, keys, lendict, maxlen): # BestPartition => NP complete
		def getFitting(leftkeys):
			current = 0
			for key in leftkeys:
				if current + lendict[key] <= maxlen:
					current += lendict[key]
					yield key
			if current == 0:
				yield leftkeys[0]
		unused = list(keys)
		while len(unused) != 0:
			for key in list(getFitting(unused)): # list(...) => get fitting keys at once!
				if key in unused:
					unused.remove(key)
				yield key
			yield None

	def _wrap_head(self, head, lendict):
		def getAlignedDict(keys, lendict, maxlen):
			edges = []
			while len(keys):
				offset = 2
				(tmp, keys) = (keys[:keys.index(None)], keys[keys.index(None)+1:])
				for key in tmp:
					left = max(0, maxlen - sum(imap(lambda k: lendict[k] + 3, tmp)))
					for edge in edges:
						if (edge > offset + lendict[key]) and (edge - (offset + lendict[key]) < left):
							lendict[key] += edge - (offset + lendict[key])
							left -= edge - (offset + lendict[key])
							break
					edges.append(offset + lendict[key])
					offset += lendict[key] + 3
			return lendict

		# Wrap and align columns
		def getHeadKey(key, name):
			return key
		def getPaddedKeyLen(key, length):
			return (key, length + 2)
		headwrap = list(self._getGoodPartition(ismap(getHeadKey, head),
			dict(ismap(getPaddedKeyLen, lendict.items())), self._wrapLen))
		lendict = getAlignedDict(headwrap, lendict, self._wrapLen)
		return (headwrap, lendict)

	# Wrap rows
	def _wrap_formatted_data(self, headwrap, entries):
		for idx, entry in enumerate(entries):
			def doEntry(entry):
				tmp = []
				for key in headwrap:
					if key is None:
						yield (tmp, entry)
						tmp = []
					else:
						tmp.append(key)
			if not isinstance(entry, str):
				for x in doEntry(entry):
					yield x
				if (idx != 0) and (idx != len(entries) - 1):
					if None in headwrap[:-1]:
						yield list(doEntry('~'))[0]
			else:
				yield list(doEntry(entry))[0]
