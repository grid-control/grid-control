# | Copyright 2010-2017 Karlsruhe Institute of Technology
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

import os
from grid_control.utils.file_tools import SafeFile
from python_compat import imap, json, lmap, sort_inplace


def filter_lumi_filter(run_list, run_lumi_range_list):
	""" Filter run_lumi_range_list for entries that contain the given runs
	>>> test_run_lumi_range_list = [([1, None], [2, None]), ([4, 1], [4, None]), ([5, 1], [None,3])]
	>>> format_lumi(filter_lumi_filter([2,3,6], test_run_lumi_range_list))
	['1:MIN-2:MAX', '5:1-9999999:3']
	>>> format_lumi(filter_lumi_filter([2,3,6], [([1, 1], [2, 2]), ([3, 1], [5, 2]), ([5, 2], [7,3])]))
	['1:1-2:2', '3:1-5:2', '5:2-7:3']
	"""
	for run_lumi_range in run_lumi_range_list:
		(run_start, run_end) = (run_lumi_range[0][0], run_lumi_range[1][0])
		for run in run_list:
			if (run_start is None) or (run >= run_start):
				if (run_end is None) or (run <= run_end):
					yield run_lumi_range
					break


def format_lumi(run_lumi_range_list):
	""" Check if run_lumi_range_list selects the given run/lumi
	>>> format_lumi(imap(parse_lumi_from_str, ['1', '1-', '-1', '1-2']))
	['1:MIN-1:MAX', '1:MIN-9999999:MAX', '1:MIN-1:MAX', '1:MIN-2:MAX']
	>>> format_lumi(imap(parse_lumi_from_str, ['1:5', '1:5-', '-1:5', '1:5-2:6']))
	['1:5-1:5', '1:5-9999999:MAX', '1:MIN-1:5', '1:5-2:6']
	>>> format_lumi(imap(parse_lumi_from_str, ['1-:5', ':5-1', ':5-:6']))
	['1:MIN-9999999:5', '1:5-1:MAX', '1:5-9999999:6']
	>>> format_lumi(imap(parse_lumi_from_str, ['1:5-2', '1-2:5']))
	['1:5-2:MAX', '1:MIN-2:5']
	"""
	def _format_run_lumi_range(run_lumi_range):
		(run_lumi_start, run_lumi_end) = run_lumi_range

		def _if_none(value, default):
			if value is None:
				return default
			return value
		return '%s:%s-%s:%s' % (
			_if_none(run_lumi_start[0], '1'), _if_none(run_lumi_start[1], 'MIN'),
			_if_none(run_lumi_end[0], '9999999'), _if_none(run_lumi_end[1], 'MAX'))
	if run_lumi_range_list:
		return lmap(_format_run_lumi_range, run_lumi_range_list)
	return ''


def merge_lumi_list(run_lumi_range_list):
	""" Merge consecutive lumi sections
	>>> merge_lumi_list([([1, 11], [1, 20]), ([1, 1], [1, 10]), ([1, 22], [1, 30])])
	[([1, 1], [1, 20]), ([1, 22], [1, 30])]
	>>> merge_lumi_list([([1, 1], [2, 2]), ([2, 3], [2, 10]), ([2, 11], [4, 30])])
	[([1, 1], [4, 30])]
	"""
	sort_inplace(run_lumi_range_list, key=lambda run_lumi_range: tuple(run_lumi_range[0]))
	idx = 0
	while idx < len(run_lumi_range_list) - 1:
		(end_run, end_lumi) = run_lumi_range_list[idx][1]
		(start_next_run, start_next_lumi) = run_lumi_range_list[idx + 1][0]
		if (end_run == start_next_run) and (end_lumi == start_next_lumi - 1):
			run_lumi_range_list[idx] = (run_lumi_range_list[idx][0], run_lumi_range_list[idx + 1][1])
			del run_lumi_range_list[idx + 1]
		else:
			idx += 1
	return run_lumi_range_list


def parse_lumi_filter(lumi_str):
	if lumi_str == '':
		return None

	run_lumi_range_list = []
	from grid_control.config import ConfigError
	for token in imap(str.strip, lumi_str.split(',')):
		token = lmap(str.strip, token.split('|'))
		if True in imap(str.isalpha, token[0].lower().replace('min', '').replace('max', '')):
			if len(token) == 1:
				token.append('')
			try:
				json_fn = os.path.normpath(os.path.expandvars(os.path.expanduser(token[0].strip())))
				run_lumi_range_list.extend(parse_lumi_from_json(SafeFile(json_fn).read_close(), token[1]))
			except Exception:
				raise ConfigError('Could not process lumi filter file: %r (filter: %r)' % tuple(token))
		else:
			try:
				run_lumi_range_list.append(parse_lumi_from_str(token[0]))
			except Exception:
				raise ConfigError('Could not process lumi filter expression:\n\t%s' % token[0])
	return merge_lumi_list(run_lumi_range_list)


def parse_lumi_from_json(data, select=''):
	run_dict = json.loads(data)
	run_range = lmap(_parse_lumi_int, select.split('-') + [''])[:2]
	for run in imap(int, run_dict.keys()):
		if (run_range[0] and run < run_range[0]) or (run_range[1] and run > run_range[1]):
			continue
		for lumi_range in run_dict[str(run)]:
			yield ([run, lumi_range[0]], [run, lumi_range[1]])


def parse_lumi_from_str(run_lumi_range_str):
	""" Parse user supplied lumi info into easier to handle format
	>>> lmap(parse_lumi_from_str, ['1-', '-1', '1-2'])
	[([1, None], [None, None]), ([None, None], [1, None]), ([1, None], [2, None])]
	>>> lmap(parse_lumi_from_str, ['1:5', '1:5-', '-1:5', '1:5-2:6'])
	[([1, 5], [1, 5]), ([1, 5], [None, None]), ([None, None], [1, 5]), ([1, 5], [2, 6])]
	>>> lmap(parse_lumi_from_str, ['1-:5', ':5-1', ':5-:6'])
	[([1, None], [None, 5]), ([None, 5], [1, None]), ([None, 5], [None, 6])]
	>>> lmap(parse_lumi_from_str, ['1', '1:5-2', '1-2:5'])
	[([1, None], [1, None]), ([1, 5], [2, None]), ([1, None], [2, 5])]
	"""
	def _parse_run_lumi(run_lumi_str):
		if ':' in run_lumi_str:
			return lmap(_parse_lumi_int, run_lumi_str.split(':'))
		else:
			return [_parse_lumi_int(run_lumi_str), None]
	if '-' in run_lumi_range_str:
		return tuple(imap(_parse_run_lumi, run_lumi_range_str.split('-')))
	else:
		tmp = _parse_run_lumi(run_lumi_range_str)
		return (tmp, tmp)


def select_lumi(run_lumi, run_lumi_range_list):
	""" Check if run_lumi_range_list selects the given run/lumi
	>>> select_lumi((1,2), [([1, None], [2, None])])
	True
	>>> select_lumi((1,2), [([1, 3], [5, 12])])
	False
	>>> select_lumi((2,1), [([1, 3], [5, 12])])
	True
	>>> select_lumi((9,2), [([3, 23], [None, None])])
	True
	"""
	(run, lumi) = run_lumi
	for (run_lumi_range_start, run_lumi_range_end) in run_lumi_range_list:
		(run_start, lumi_start) = run_lumi_range_start
		(run_end, lumi_end) = run_lumi_range_end
		if (run_start is None) or (run >= run_start):
			if (run_end is None) or (run <= run_end):
				# At this point, run_lumi is contained in the selected run
				if (run_start is not None) and (run > run_start):
					lumi_start = None
				if (lumi_start is None) or (lumi >= lumi_start):
					if (run_end is not None) and (run < run_end):
						lumi_end = None
					if (lumi_end is None) or (lumi <= lumi_end):
						return True
	return False


def select_run(run, run_lumi_range_list):
	""" Check if run_lumi_range_list selects the given run/lumi
	>>> select_run(1, [([1, None], [2, None])])
	True
	>>> select_run(2, [([1, 3], [5, 12])])
	True
	>>> select_run(6, [([1, 3], [5, 12])])
	False
	>>> select_run(9, [([3, 23], [None, None])])
	True
	"""
	for (run_lumi_range_start, run_lumi_range_end) in run_lumi_range_list:
		(run_start, run_end) = (run_lumi_range_start[0], run_lumi_range_end[0])
		if (run_start is None) or (run >= run_start):
			if (run_end is None) or (run <= run_end):
				return True
	return False


def str_lumi(run_lumi_range_list):
	return str.join(',', format_lumi(run_lumi_range_list))


def _parse_lumi_int(value):
	if value.strip().upper() not in ['', 'MAX', 'MIN']:
		return int(value)
