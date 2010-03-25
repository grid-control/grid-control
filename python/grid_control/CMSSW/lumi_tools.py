def parseLumiFilter(lumifilter):
	""" Parse user supplied lumi info into easier to handle format
	>>> parseLumiFilter("1-2,1:3-2,3-4:3")
	[([1, None], [2, None]), ([1, 3], [2, None]), ([3, None], [4, 3])]
	>>> parseLumiFilter("5:6-7:8,9:10-")
	[([5, 6], [7, 8]), ([9, 10], [None, None])]
	"""
	if lumifilter == '':
		return None
	tmp = []
	for token in map(str.strip, lumifilter.split(",")):
		def mysplit(x, sep):
			if x == None:
				return (None, None)
			parts = map(str.strip, x.split(sep))
			a = None
			if len(parts) > 0 and parts[0] != "":
				a = parts[0]
			b = None
			if len(parts) > 1 and parts[1] != "":
				b = parts[1]
			if len(parts) > 2:
				raise
			return (a, b)
		def makeint(x):
			if x:
				return int(x)
			return x
		tmp.append(tuple(map(lambda x: map(makeint, mysplit(x, ":")), mysplit(token, "-"))))
	def cmpLumi(a,b):
		(start_a_run, start_a_lumi) = a[0]
		(start_b_run, start_b_lumi) = b[0]
		if start_a_run == start_b_run:
			return cmp(start_a_lumi, start_b_lumi)
		else:
			return cmp(start_a_run, start_b_run)
	tmp.sort(cmpLumi)
	return tmp


def selectLumi(run_lumi, lumifilter):
	""" Check if lumifilter selects the given run/lumi
	>>> selectLumi((1,2), [([1, None], [2, None])])
	True
	>>> selectLumi((1,2), [([1, 3], [5, 12])])
	False
	>>> selectLumi((9,2), [([3, 23], [None, None])])
	False
	>>> selectLumi((9,26), [([3, 23], [None, None])])
	True
	"""
	(run, lumi) = run_lumi
	for (sel_start, sel_end) in lumifilter:
		(sel_start_run, sel_start_lumi) = sel_start
		(sel_end_run, sel_end_lumi) = sel_end
		if (sel_start_run == None) or (run >= sel_start_run):
			if (sel_start_lumi == None) or (lumi >= sel_start_lumi):
				if (sel_end_run == None) or (run <= sel_end_run):
					if (sel_end_lumi == None) or (lumi <= sel_end_lumi):
						return True
	return False


def formatLumi(lumifilter):
	""" Check if lumifilter selects the given run/lumi
	>>> formatLumi([([1, None], [2, None]), ([1, 3], [2, None]), ([3, None], [4, 3])])
	['1-2', '1:3-2:9999', '3:1-4:3']
	>>> formatLumi([([5, 6], [7, 8]), ([9, 1], [None, None])])
	['5:6-7:8', '9:1-']
	"""
	def fmt(run_lumi):
		def fmtRunLumi(run_lumi, ldef):
			f = lambda x, default: str((x, default)[x == None])
			(r, l) = run_lumi
			if r or l:
				return f(r, '') + ":" + f(l, ldef)
			return ''
		if not (run_lumi[0][1] or run_lumi[1][1]):
			return "%d-%d" % (run_lumi[0][0], run_lumi[1][0])
		return fmtRunLumi(run_lumi[0], 1) + "-" + fmtRunLumi(run_lumi[1], 9999)
	return map(fmt, lumifilter)


if __name__ == '__main__':
	import doctest
	doctest.testmod()
