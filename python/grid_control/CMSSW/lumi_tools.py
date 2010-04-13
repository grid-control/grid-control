import os
from grid_control import ConfigError

def parseLumiFromJSON(data):
	# Parsing is trivial :)
	runs = eval(data)
	# Merge consecutive lumi sections
	all = []
	rkeys = runs.keys()
	rkeys.sort()
	for run in rkeys:
		lumis = runs[run]
		lumis.sort()
		for i in range(len(lumis) - 1):
			if (lumis[i][1] == lumis[i+1][0] - 1):
				lumis[i][1] = lumis[i+1][1]
				lumis[i+1] = None
				lumis.sort()
		for lumi in filter(lambda x: x, lumis):
			all.append(([int(run), int(lumi[0])], [int(run), int(lumi[1])]))
	return all


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
	def makeint(x):
		if x == '':
			return None
		return int(x)
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
		if os.path.exists(token):
			try:
				lumis.extend(parseLumiFromJSON(open(token).read()))
			except:
				raise ConfigError('Could not process lumi filter file:\n %s' % token)
		else:
			try:
				lumis.append(parseLumiFromString(token))
			except:
				raise ConfigError('Could not process lumi filter expression:\n%s' % token)

	# Sort lumi ranges
	def cmpLumi(a,b):
		(start_a_run, start_a_lumi) = a[0]
		(start_b_run, start_b_lumi) = b[0]
		if start_a_run == start_b_run:
			return cmp(start_a_lumi, start_b_lumi)
		else:
			return cmp(start_a_run, start_b_run)
	lumis.sort(cmpLumi)
	return lumis


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
	['1', '1-', '-1', '1-2']
	>>> formatLumi(map(parseLumiFromString, ['1:5', '1:5-', '-1:5', '1:5-2:6']))
	['1:5', '1:5-', '-1:5', '1:5-2:6']
	>>> formatLumi(map(parseLumiFromString, ['1-:5', ':5-1', ':5-:6']))
	['1-:5', ':5-1', ':5-:6']
	>>> formatLumi(map(parseLumiFromString, ['1:5-2', '1-2:5']))
	['1:5-2:MAX', '1:1-2:5']
	"""
	def formatRunLumi(run_lumi):
		(run, lumi) = run_lumi
		if run and lumi:
			return "%s:%s" % tuple(run_lumi)
		elif run:
			return "%s" % run
		elif lumi:
			return ":%s" % lumi
		return ""
	def formatRange(rlrange):
		(start, end) = rlrange
		if start == end:
			return formatRunLumi(start)
		else:
			if end[0] and not end[1] and start[0] and start[1]:
				end = [end[0], 'MAX']
			if start[0] and not start[1] and end[0] and end[1]:
				start = [start[0], 1]
			return str.join("-", map(formatRunLumi, (start, end)))
	return map(formatRange, lumifilter)


if __name__ == '__main__':
	import doctest
	doctest.testmod()
