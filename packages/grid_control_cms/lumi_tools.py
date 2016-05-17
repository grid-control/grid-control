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

def makeint(x):
	if x.strip().upper() not in ['', 'MAX', 'MIN']:
		return int(x)


def parseLumiFromJSON(data, select = ''):
	runs = json.loads(data)
	rr = lmap(makeint, select.split('-') + [''])[:2]
	for run in imap(int, runs.keys()):
		if (rr[0] and run < rr[0]) or (rr[1] and run > rr[1]):
			continue
		for lumi in runs[str(run)]:
			yield ([run, lumi[0]], [run, lumi[1]])


def keyLumi(a):
	return tuple(a[0])


def mergeLumi(rlrange):
	""" Merge consecutive lumi sections
	>>> mergeLumi([([1, 11], [1, 20]), ([1, 1], [1, 10]), ([1, 22], [1, 30])])
	[([1, 1], [1, 20]), ([1, 22], [1, 30])]
	>>> mergeLumi([([1, 1], [2, 2]), ([2, 3], [2, 10]), ([2, 11], [4, 30])])
	[([1, 1], [4, 30])]
	"""
	sort_inplace(rlrange, keyLumi)
	i = 0
	while i < len(rlrange) - 1:
		(end_run, end_lumi) = rlrange[i][1]
		(start_next_run, start_next_lumi) = rlrange[i+1][0]
		if (end_run == start_next_run) and (end_lumi == start_next_lumi - 1):
			rlrange[i] = (rlrange[i][0], rlrange[i + 1][1])
			del rlrange[i+1]
		else:
			i += 1
	return rlrange


def parseLumiFromString(rlrange):
	""" Parse user supplied lumi info into easier to handle format
	>>> lmap(parseLumiFromString, ['1', '1-', '-1', '1-2'])
	[([1, None], [1, None]), ([1, None], [None, None]), ([None, None], [1, None]), ([1, None], [2, None])]
	>>> lmap(parseLumiFromString, ['1:5', '1:5-', '-1:5', '1:5-2:6'])
	[([1, 5], [1, 5]), ([1, 5], [None, None]), ([None, None], [1, 5]), ([1, 5], [2, 6])]
	>>> lmap(parseLumiFromString, ['1-:5', ':5-1', ':5-:6'])
	[([1, None], [None, 5]), ([None, 5], [1, None]), ([None, 5], [None, 6])]
	>>> lmap(parseLumiFromString, ['1:5-2', '1-2:5'])
	[([1, 5], [2, None]), ([1, None], [2, 5])]
	"""
	def parseRunLumi(rl):
		if ':' in rl:
			return lmap(makeint, rl.split(':'))
		else:
			return [makeint(rl), None]
	if '-' in rlrange:
		return tuple(imap(parseRunLumi, rlrange.split('-')))
	else:
		tmp = parseRunLumi(rlrange)
		return (tmp, tmp)


def parseLumiFilter(lumiexpr):
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
				lumis.extend(parseLumiFromJSON(json_fp.read(), token[1]))
				json_fp.close()
			except Exception:
				raise ConfigError('Could not process lumi filter file: %r (filter: %r)' % tuple(token))
		else:
			try:
				lumis.append(parseLumiFromString(token[0]))
			except Exception:
				raise ConfigError('Could not process lumi filter expression:\n\t%s' % token[0])
	return mergeLumi(lumis)


def filterLumiFilter(runs, lumifilter):
	""" Filter lumifilter for entries that contain the given runs
	>>> formatLumi(filterLumiFilter([2,3,6], [([1, None], [2, None]), ([4, 1], [4, None]), ([5, 1], [None,3])]))
	['1:MIN-2:MAX', '5:1-9999999:3']
	>>> formatLumi(filterLumiFilter([2,3,6], [([1, 1], [2, 2]), ([3, 1], [5, 2]), ([5, 2], [7,3])]))
	['1:1-2:2', '3:1-5:2', '5:2-7:3']
	"""
	for filterEntry in lumifilter:
		(sel_start, sel_end) = (filterEntry[0][0], filterEntry[1][0])
		for run in runs:
			if (sel_start is None) or (run >= sel_start):
				if (sel_end is None) or (run <= sel_end):
					yield filterEntry
					break


def selectLumi(run_lumi, lumifilter):
	""" Check if lumifilter selects the given run/lumi
	>>> selectLumi((1,2), [([1, None], [2, None])])
	True
	>>> selectLumi((1,2), [([1, 3], [5, 12])])
	False
	>>> selectLumi((2,1), [([1, 3], [5, 12])])
	True
	>>> selectLumi((9,2), [([3, 23], [None, None])])
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


def formatLumi(lumifilter):
	""" Check if lumifilter selects the given run/lumi
	>>> formatLumi(imap(parseLumiFromString, ['1', '1-', '-1', '1-2']))
	['1:MIN-1:MAX', '1:MIN-9999999:MAX', '1:MIN-1:MAX', '1:MIN-2:MAX']
	>>> formatLumi(imap(parseLumiFromString, ['1:5', '1:5-', '-1:5', '1:5-2:6']))
	['1:5-1:5', '1:5-9999999:MAX', '1:MIN-1:5', '1:5-2:6']
	>>> formatLumi(imap(parseLumiFromString, ['1-:5', ':5-1', ':5-:6']))
	['1:MIN-9999999:5', '1:5-1:MAX', '1:5-9999999:6']
	>>> formatLumi(imap(parseLumiFromString, ['1:5-2', '1-2:5']))
	['1:5-2:MAX', '1:MIN-2:5']
	"""
	def formatRange(rlrange):
		(start, end) = rlrange
		default = lambda x, d: (x, d)[x is None]
		start = [default(start[0], '1'), default(start[1], 'MIN')]
		end = [default(end[0], '9999999'), default(end[1], 'MAX')]
		return str.join('-', imap(lambda x: '%s:%s' % tuple(x), (start, end)))
	if lumifilter:
		return lmap(formatRange, lumifilter)
	return ''


def strLumi(lumifilter):
	return str.join(',', formatLumi(lumifilter))


if __name__ == '__main__':
	import doctest
	doctest.testmod()
