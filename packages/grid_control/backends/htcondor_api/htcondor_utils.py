
def chmodNumPerms(args = ''):
	perm = 0
	if 'r' in args:
		perm += 4
	if 'w' in args:
		perm += 2
	if 'x' in args:
		perm += 1
	return perm

def parseKWListIter(kwListIter, jobDelimeter = lambda line: not line):
	"""Parse an iterator of blobs of 'key = value' lines as provided by condor"""
	infoMaps    = []
	parseBuffer = {}
	for line in kwListIter:
		line = line.strip()
		if parseBuffer and jobDelimeter(line):
			infoMaps.append(parseBuffer)
			parseBuffer = {}
			continue
		try:
			key, val = [ obj.strip().replace('"','') for obj in line.split('=',1) ]
			parseBuffer[key] = val
		except ValueError as err:
			pass
	infoMaps.append(parseBuffer)
	return [ iMap for iMap in infoMaps if iMap ]

def singleQueryCache(defReturnItem = None, maxFuncFails = 10, functionFailureItem = None):
	"""Lightweight function memoization for a single query call with limited retries"""
	def SQ_decorator(function):
		def SQ_proxy(*args, **kwargs):
			try:
				return SQ_proxy._store[1]
			except IndexError:
				SQ_proxy._store[0] += 1
				if SQ_proxy._store[0] > maxFuncFails:
					SQ_proxy._store.append(defReturnItem)
				else:
					funcReturn = SQ_proxy.function(*args, **kwargs)
					if funcReturn != functionFailureItem:
						SQ_proxy._store[1] = funcReturn
			try:
				return SQ_proxy._store[1]
			except IndexError:
				return defReturnItem
		SQ_proxy._store   = [0] # failcount, retItem
		SQ_proxy.function = function
		return SQ_proxy
	return SQ_decorator
