import os
from grid_control import ConfigError

def makeint(x):
	if (x == '') or (x.upper() in ['MAX', 'MIN']):
		return None
	return int(x)


def parseLumiFromJSON(data, select = ''):
	runs = eval(data)
	all = []
	rr = [None, None]
	if '-' in select:
		rr = map(makeint, select.split('-'))
	for run in map(int, runs.keys()):
		if (rr[0] and run < rr[0]) or (rr[1] and run > rr[1]):
			continue
		for lumi in runs[str(run)]:
			all.append(([run, lumi[0]], [run, lumi[1]]))
	return all


def cmpLumi(a, b):
	(start_a_run, start_a_lumi) = a[0]
	(start_b_run, start_b_lumi) = b[0]
	if start_a_run == start_b_run:
		return cmp(start_a_lumi, start_b_lumi)
	else:
		return cmp(start_a_run, start_b_run)


def mergeLumi(rlrange):
	""" Merge consecutive lumi sections
	>>> mergeLumi([([1, 11], [1, 20]), ([1, 1], [1, 10]), ([1, 22], [1, 30])])
	[([1, 1], [1, 20]), ([1, 22], [1, 30])]
	>>> mergeLumi([([1, 1], [2, 2]), ([2, 3], [2, 10]), ([2, 11], [4, 30])])
	[([1, 1], [4, 30])]
	"""
	rlrange.sort(cmpLumi)
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
	>>> map(parseLumiFromString, ['1', '1-', '-1', '1-2'])
	[([1, None], [1, None]), ([1, None], [None, None]), ([None, None], [1, None]), ([1, None], [2, None])]
	>>> map(parseLumiFromString, ['1:5', '1:5-', '-1:5', '1:5-2:6'])
	[([1, 5], [1, 5]), ([1, 5], [None, None]), ([None, None], [1, 5]), ([1, 5], [2, 6])]
	>>> map(parseLumiFromString, ['1-:5', ':5-1', ':5-:6'])
	[([1, None], [None, 5]), ([None, 5], [1, None]), ([None, 5], [None, 6])]
	>>> map(parseLumiFromString, ['1:5-2', '1-2:5'])
	[([1, 5], [2, None]), ([1, None], [2, 5])]
	"""
	def parseRunLumi(rl):
		if ':' in rl:
			return map(makeint, rl.split(':'))
		else:
			return [makeint(rl), None]
	if '-' in rlrange:
		return tuple(map(parseRunLumi, rlrange.split('-')))
	else:
		tmp = parseRunLumi(rlrange)
		return (tmp, tmp)


def parseLumiFilter(lumiexpr):
	if lumiexpr == '':
		return None

	lumis = []
	for token in map(str.strip, lumiexpr.split(',')):
		token = map(str.strip, token.split('|'))
		if os.path.exists(token[0]):
			try:
				if len(token) == 1:
					token.append('')
				lumis.extend(parseLumiFromJSON(open(token[0]).read(), token[1]))
			except:
				raise ConfigError('Could not process lumi filter file:\n%s' % token)
		else:
			try:
				lumis.append(parseLumiFromString(token[0]))
			except:
				raise ConfigError('Could not process lumi filter expression:\n%s' % token[0])
	return mergeLumi(lumis)


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
		if (sel_start_run == None) or (run >= sel_start_run):
			if (sel_end_run == None) or (run <= sel_end_run):
				# At this point, run_lumi is contained in the selected run
				if (sel_start_run != None) and (run > sel_start_run):
					sel_start_lumi = None
				if (sel_start_lumi == None) or (lumi >= sel_start_lumi):
					if (sel_end_run != None) and (run < sel_end_run):
						sel_end_lumi = None
					if (sel_end_lumi == None) or (lumi <= sel_end_lumi):
						return True
	return False


def formatLumi(lumifilter):
	""" Check if lumifilter selects the given run/lumi
	>>> formatLumi(map(parseLumiFromString, ['1', '1-', '-1', '1-2']))
	['1:MIN-1:MAX', '1:MIN-9999999:MAX', '1:MIN-1:MAX', '1:MIN-2:MAX']
	>>> formatLumi(map(parseLumiFromString, ['1:5', '1:5-', '-1:5', '1:5-2:6']))
	['1:5-1:5', '1:5-9999999:MAX', '1:MIN-1:5', '1:5-2:6']
	>>> formatLumi(map(parseLumiFromString, ['1-:5', ':5-1', ':5-:6']))
	['1:MIN-9999999:5', '1:5-1:MAX', '1:5-9999999:6']
	>>> formatLumi(map(parseLumiFromString, ['1:5-2', '1-2:5']))
	['1:5-2:MAX', '1:MIN-2:5']
	"""
	def formatRange(rlrange):
		(start, end) = rlrange
		default = lambda x, d: (x, d)[x == None]
		start = [default(start[0], '1'), default(start[1], 'MIN')]
		end = [default(end[0], '9999999'), default(end[1], 'MAX')]
		return str.join("-", map(lambda x: "%s:%s" % tuple(x), (start, end)))
	if lumifilter:
		return map(formatRange, lumifilter)
	return ''


if __name__ == '__main__':
	import doctest
	doctest.testmod()
