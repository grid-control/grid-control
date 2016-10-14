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

import os
from python_compat import imap, json, lmap, sort_inplace


def filter_lumi_filter(runs, lumifilter):
	""" Filter lumifilter for entries that contain the given runs
	>>> test_lumifilter = [([1, None], [2, None]), ([4, 1], [4, None]), ([5, 1], [None,3])]
	>>> format_lumi(filter_lumi_filter([2,3,6], test_lumifilter))
	['1:MIN-2:MAX', '5:1-9999999:3']
	>>> format_lumi(filter_lumi_filter([2,3,6], [([1, 1], [2, 2]), ([3, 1], [5, 2]), ([5, 2], [7,3])]))
	['1:1-2:2', '3:1-5:2', '5:2-7:3']
	"""
	for filter_entry in lumifilter:
		(sel_start, sel_end) = (filter_entry[0][0], filter_entry[1][0])
		for run in runs:
			if (sel_start is None) or (run >= sel_start):
				if (sel_end is None) or (run <= sel_end):
					yield filter_entry
					break


def format_lumi(lumifilter):
	""" Check if lumifilter selects the given run/lumi
	>>> format_lumi(imap(parse_lumi_from_str, ['1', '1-', '-1', '1-2']))
	['1:MIN-1:MAX', '1:MIN-9999999:MAX', '1:MIN-1:MAX', '1:MIN-2:MAX']
	>>> format_lumi(imap(parse_lumi_from_str, ['1:5', '1:5-', '-1:5', '1:5-2:6']))
	['1:5-1:5', '1:5-9999999:MAX', '1:MIN-1:5', '1:5-2:6']
	>>> format_lumi(imap(parse_lumi_from_str, ['1-:5', ':5-1', ':5-:6']))
	['1:MIN-9999999:5', '1:5-1:MAX', '1:5-9999999:6']
	>>> format_lumi(imap(parse_lumi_from_str, ['1:5-2', '1-2:5']))
	['1:5-2:MAX', '1:MIN-2:5']
	"""
	def _format_range(run_lumi_range):
		(start, end) = run_lumi_range

		def _if_none(value, default):
			if value is None:
				return default
			return value
		start = [_if_none(start[0], '1'), _if_none(start[1], 'MIN')]
		end = [_if_none(end[0], '9999999'), _if_none(end[1], 'MAX')]
		return str.join('-', imap(lambda x: '%s:%s' % tuple(x), (start, end)))
	if lumifilter:
		return lmap(_format_range, lumifilter)
	return ''


def merge_lumi_list(rlrange):
	""" Merge consecutive lumi sections
	>>> merge_lumi_list([([1, 11], [1, 20]), ([1, 1], [1, 10]), ([1, 22], [1, 30])])
	[([1, 1], [1, 20]), ([1, 22], [1, 30])]
	>>> merge_lumi_list([([1, 1], [2, 2]), ([2, 3], [2, 10]), ([2, 11], [4, 30])])
	[([1, 1], [4, 30])]
	"""
	sort_inplace(rlrange, key=lambda lumi_start_end: tuple(lumi_start_end[0]))
	idx = 0
	while idx < len(rlrange) - 1:
		(end_run, end_lumi) = rlrange[idx][1]
		(start_next_run, start_next_lumi) = rlrange[idx + 1][0]
		if (end_run == start_next_run) and (end_lumi == start_next_lumi - 1):
			rlrange[idx] = (rlrange[idx][0], rlrange[idx + 1][1])
			del rlrange[idx + 1]
		else:
			idx += 1
	return rlrange


def parse_lumi_filter(lumiexpr):
	if lumiexpr == '':
		return None

	lumis = []
	from grid_control.config import ConfigError
	for token in imap(str.strip, lumiexpr.split(',')):
		token = lmap(str.strip, token.split('|'))
		if True in imap(str.isalpha, token[0].lower().replace('min', '').replace('max', '')):
			if len(token) == 1:
				token.append('')
			try:
				json_fn = os.path.normpath(os.path.expandvars(os.path.expanduser(token[0].strip())))
				json_fp = open(json_fn)
				lumis.extend(parse_lumi_from_json(json_fp.read(), token[1]))
				json_fp.close()
			except Exception:
				raise ConfigError('Could not process lumi filter file: %r (filter: %r)' % tuple(token))
		else:
			try:
				lumis.append(parse_lumi_from_str(token[0]))
			except Exception:
				raise ConfigError('Could not process lumi filter expression:\n\t%s' % token[0])
	return merge_lumi_list(lumis)


def parse_lumi_from_json(data, select=''):
	runs = json.loads(data)
	run_range = lmap(_parse_lumi_int, select.split('-') + [''])[:2]
	for run in imap(int, runs.keys()):
		if (run_range[0] and run < run_range[0]) or (run_range[1] and run > run_range[1]):
			continue
		for lumi in runs[str(run)]:
			yield ([run, lumi[0]], [run, lumi[1]])


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


def select_lumi(run_lumi, lumifilter):
	""" Check if lumifilter selects the given run/lumi
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
	for (sel_start, sel_end) in lumifilter:
		(sel_start_run, sel_start_lumi) = sel_start
		(sel_end_run, sel_end_lumi) = sel_end
		if (sel_start_run is None) or (run >= sel_start_run):
			if (sel_end_run is None) or (run <= sel_end_run):
				# At this point, run_lumi is contained in the selected run
				if (sel_start_run is not None) and (run > sel_start_run):
					sel_start_lumi = None
				if (sel_start_lumi is None) or (lumi >= sel_start_lumi):
					if (sel_end_run is not None) and (run < sel_end_run):
						sel_end_lumi = None
					if (sel_end_lumi is None) or (lumi <= sel_end_lumi):
						return True
	return False


def select_run(run, lumifilter):
	""" Check if lumifilter selects the given run/lumi
	>>> select_run(1, [([1, None], [2, None])])
	True
	>>> select_run(2, [([1, 3], [5, 12])])
	True
	>>> select_run(6, [([1, 3], [5, 12])])
	False
	>>> select_run(9, [([3, 23], [None, None])])
	True
	"""
	for (sel_start, sel_end) in lumifilter:
		(sel_start_run, sel_end_run) = (sel_start[0], sel_end[0])
		if (sel_start_run is None) or (run >= sel_start_run):
			if (sel_end_run is None) or (run <= sel_end_run):
				return True
	return False


def str_lumi(lumifilter):
	return str.join(',', format_lumi(lumifilter))


def _parse_lumi_int(value):
	if value.strip().upper() not in ['', 'MAX', 'MIN']:
		return int(value)
