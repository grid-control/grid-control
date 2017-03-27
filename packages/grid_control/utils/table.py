# | Copyright 2016-2017 Karlsruhe Institute of Technology
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
from python_compat import imap, ismap, izip, json, lmap


class Table(object):
	pass


class ConsoleTable(Table):
	wraplen = 100
	table_mode = 'default'

	def __init__(self):
		Table.__init__(self)
		self._log = logging.getLogger('console')

	def create(cls, head, data, align_str='', fmt_dict=None, title=None, pivot=False):
		if ConsoleTable.table_mode == 'ParseableTable':
			return ParseableTable(head, data, '|')
		if ConsoleTable.table_mode == 'Pivot':
			pivot = not pivot
		if pivot:
			return RowTable(head, data, fmt_dict, ConsoleTable.wraplen, title=title)
		return ColumnTable(head, data, align_str, fmt_dict, ConsoleTable.wraplen, title=title)
	create = classmethod(create)

	def _write_line(self, msg):
		self._log.info(msg)


class JSONTable(ConsoleTable):
	def __init__(self, head, data):
		ConsoleTable.__init__(self)
		self._write_line(json.dumps({'data': data, 'header': head}, sort_keys=True))


class ParseableTable(ConsoleTable):
	def __init__(self, head, data, delimeter='|'):
		ConsoleTable.__init__(self)
		head = list(head)
		self._delimeter = delimeter
		self._write_line(str.join(self._delimeter, imap(lambda x: x[1], head)))
		for entry in data:
			if isinstance(entry, dict):
				self._write_line(str.join(self._delimeter, imap(lambda x: str(entry.get(x[0], '')), head)))


class UserConsoleTable(ConsoleTable):
	def __init__(self, title=None):
		ConsoleTable.__init__(self)
		self._write_line('')
		if title:
			title_len = 0
			for line in title.splitlines():
				self._write_line(line)
				title_len = max(title_len, len(line))
			self._write_line('=' * title_len)
			self._write_line('')


class ColumnTable(UserConsoleTable):
	def __init__(self, head, data, align_str='', fmt_dict=None, wrap_len=100, title=None):
		UserConsoleTable.__init__(self, title)
		self._wrap_len = wrap_len
		head = list(head)
		just_fun = self._get_just_fun_dict(head, align_str)
		# return formatted, but not yet aligned entries; len dictionary; just function
		(entries, lendict, just) = self._format_data(head, data, just_fun, fmt_dict or {})
		(headwrap, lendict) = self._wrap_head(head, lendict)

		def _get_key_padded_name(key, name):
			return (key, name.center(lendict[key]))
		headentry = dict(ismap(_get_key_padded_name, head))
		self._print_table(headwrap, headentry, entries, just, lendict)
		self._write_line('')

	def _format_data(self, head, data, just_fun, fmt_dict):
		# adjust to lendict of column (considering escape sequence correction)
		def _get_key_len(key, name):
			return (key, len(name))

		def _just(key, value):
			return just_fun.get(key, str.rjust)(value, lendict[key] + len(value) - _stripped_len(value))

		def _stripped_len(value):
			return len(re.sub('\033\\[\\d*(;\\d*)*m', '', value))

		lendict = dict(ismap(_get_key_len, head))

		result = []
		for entry in data:
			if isinstance(entry, dict):
				tmp = {}
				for key, _ in head:
					tmp[key] = str(fmt_dict.get(key, str)(entry.get(key, '')))
					lendict[key] = max(lendict[key], _stripped_len(tmp[key]))
				result.append(tmp)
			else:
				result.append(entry)
		return (result, lendict, _just)

	def _get_good_partition(self, keys, lendict, maxlen):  # BestPartition => NP complete
		def _get_fitting_keys(leftkeys):
			current = 0
			for key in leftkeys:
				if current + lendict[key] <= maxlen:
					current += lendict[key]
					yield key
			if current == 0:
				yield leftkeys[0]
		unused = list(keys)
		while len(unused) != 0:
			for key in list(_get_fitting_keys(unused)):  # list(...) => get fitting keys at once!
				if key in unused:
					unused.remove(key)
				yield key
			yield None

	def _get_just_fun_dict(self, head, align_str):
		just_fun_dict = {'l': str.ljust, 'r': str.rjust, 'c': str.center}
		# just_fun = {id1: str.center, id2: str.rjust, ...}

		def _get_key_format(head_entry, align_str):
			return (head_entry[0], just_fun_dict[align_str])
		return dict(ismap(_get_key_format, izip(head, align_str)))

	def _print_table(self, headwrap, headentry, entries, just, lendict):
		for (keys, entry) in self._wrap_formatted_data(headwrap, [headentry, '='] + entries):
			if isinstance(entry, str):
				def _decor(value):
					return '%s%s%s' % (entry, value, entry)
				value = _decor(str.join(_decor('+'), lmap(lambda key: entry * lendict[key], keys)))
			else:
				value = ' %s ' % str.join(' | ', imap(lambda key: just(key, entry.get(key, '')), keys))
			self._write_line(value)

	def _wrap_formatted_data(self, headwrap, entries):  # Wrap rows
		for idx, entry in enumerate(entries):
			def _process_entry(entry):
				tmp = []
				for key in headwrap:
					if key is None:
						yield (tmp, entry)
						tmp = []
					else:
						tmp.append(key)
			if not isinstance(entry, str):
				for proc_entry in _process_entry(entry):
					yield proc_entry
				if idx not in (0, len(entries) - 1):
					if None in headwrap[:-1]:
						yield list(_process_entry('~'))[0]
			else:
				yield list(_process_entry(entry))[0]

	def _wrap_head(self, head, lendict):
		def _get_aligned_dict(keys, lendict, maxlen):
			edges = []
			while len(keys):
				offset = 2
				(tmp, keys) = (keys[:keys.index(None)], keys[keys.index(None) + 1:])
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

		def _get_head_key(key, name):  # Wrap and align columns
			return key

		def _get_padded_key_len(key, length):
			return (key, length + 2)
		headwrap = list(self._get_good_partition(ismap(_get_head_key, head),
			dict(ismap(_get_padded_key_len, lendict.items())), self._wrap_len))
		lendict = _get_aligned_dict(headwrap, lendict, self._wrap_len)
		return (headwrap, lendict)


class RowTable(UserConsoleTable):
	def __init__(self, head, data, fmt_dict=None, wrap_len=100, title=None):
		UserConsoleTable.__init__(self, title)
		head = list(head)

		def _get_header_name(key, name):
			return name

		maxhead = max(imap(len, ismap(_get_header_name, head)))
		fmt_dict = fmt_dict or {}
		show_line = False
		for entry in data:
			if isinstance(entry, dict):
				if show_line:
					self._write_line(('-' * (maxhead + 2)) + '-+-' + '-' * min(30, wrap_len - maxhead - 10))
				for (key, name) in head:
					value = str(fmt_dict.get(key, str)(entry.get(key, '')))
					self._write_line(name.rjust(maxhead + 2) + ' | ' + value)
				show_line = True
			elif show_line:
				self._write_line(('=' * (maxhead + 2)) + '=+=' + '=' * min(30, wrap_len - maxhead - 10))
				show_line = False
		self._write_line('')
