import os, csv, gzip
from python_compat import *
from grid_control import utils
from psource_base import ParameterSource, ParameterMetadata, ParameterInfo
from psource_meta import ForwardingParameterSource

# Reader for grid-control dump files
class GCDumpParameterSource(ParameterSource):
	def __init__(self, fn):
		ParameterSource.__init__(self)
		fp = gzip.open(fn, 'rb')
		keyline = fp.readline().lstrip('#').strip()
		self.keys = []
		if keyline:
			self.keys = eval(keyline)
		def parseLine(line):
			if not line.startswith('#'):
				pNumStr, stored = map(str.strip, line.split('\t', 1))
				return ('!' in pNumStr, int(pNumStr.rstrip('!')), map(eval, stored.split('\t')))
		self.values = map(parseLine, fp.readlines())

	def getMaxParameters(self):
		return len(self.values)

	def fillParameterKeys(self, result):
		result.extend(map(lambda k: ParameterMetadata(k, untracked = False), self.keys))

	def fillParameterInfo(self, pNum, result):
		result[ParameterInfo.ACTIVE] = not self.values[pNum][0]
		result.update(filter(lambda (k, v): v != None, zip(self.keys, self.values[pNum][2])))

	def write(cls, fn, pa):
		fp = gzip.open(fn, 'wb')
		keys = sorted(filter(lambda p: p.untracked == False, pa.getJobKeys()))
		fp.write('# %s\n' % keys)
		maxN = pa.getMaxJobs()
		if maxN:
			log = None
			for jobNum in range(maxN):
				del log
				log = utils.ActivityLog('Writing parameter dump [%d/%d]' % (jobNum + 1, maxN))
				meta = pa.getJobInfo(jobNum)
				if meta.get(ParameterInfo.ACTIVE, True):
					fp.write('%d\t%s\n' % (jobNum, str.join('\t', map(lambda k: repr(meta.get(k, '')), keys))))
				else:
					fp.write('%d!\t%s\n' % (jobNum, str.join('\t', map(lambda k: repr(meta.get(k, '')), keys))))
	write = classmethod(write)


# Reader for CSV files
#class CSVParameter(GCDumpParameterSource):
#	pass
