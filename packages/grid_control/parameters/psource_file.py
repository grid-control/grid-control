import os, csv, gzip
from psource_base import ParameterSource, ParameterMetadata, ParameterInfo
from psource_meta import ForwardingParameterSource

# Reader for grid-control dump files
class GCDumpParameterSource(ParameterSource):
	def __init__(self, fn):
		ParameterSource.__init__(self)
		fp = gzip.open(fn, 'rb')
		self.keys = eval(fp.readline().lstrip('#'))
		def parseLine(line):
			if not line.startswith('#'):
				jobNum, stored = map(str.strip, line.split('\t', 1))
				return ('!' in jobNum, int(jobNum.rstrip('!')), map(eval, stored.split('\t')))
		self.values = map(parseLine, fp.readlines())

	def getMaxJobs(self):
		return len(self.values)

	def fillParameterKeys(self, result):
		result.extend(map(ParameterMetadata, self.keys))

	def fillParameterInfo(self, pNum, result):
		result[ParameterInfo.ACTIVE] = not self.values[pNum][0]
		result['MY_JOBID'] = self.values[pNum][1]
		result.update(filter(lambda (k, v): v != None, zip(self.keys, self.values[pNum][2])))

	def write(cls, fn, plugin):
		fp = gzip.open(fn, 'wb')
		keys = filter(lambda p: p.untracked == False, plugin.getJobKeys())
		fp.write('# %s\n' % keys)
		maxN = plugin.getMaxJobs()
		if maxN:
			for jobNum in range(maxN):
				meta = plugin.getJobInfo(jobNum)
				if meta[ParameterInfo.ACTIVE]:
					fp.write('%d\t%s\n' % (jobNum, str.join('\t', map(lambda k: repr(meta.get(k, '')), keys))))
				else:
					fp.write('%d!\t%s\n' % (jobNum, str.join('\t', map(lambda k: repr(meta.get(k, '')), keys))))
	write = classmethod(write)


# Reader for quick resync state
class GCCacheParameterSource(ForwardingParameterSource):
	def __init__(self, fn, plugin):
		ForwardingParameterSource.__init__(self, plugin)
		fp = gzip.open(fn, 'r')
		self.maxN = int(fp.readline())
		if not self.maxN:
			self.maxN = None
		mapInfo = filter(lambda x: x, map(str.strip, fp.readline().split(',')))
		self.mapJob2PID = dict(map(lambda x: tuple(map(lambda y: int(y), x.split(':'))), mapInfo))

	def getMaxJobs(self):
		return self.maxN

	def fillParameterInfo(self, pNum, result):
		if (pNum < self.plugin.getMaxJobs()) or (self.plugin.getMaxJobs() == None):
			self.plugin.fillParameterInfo(pNum, result)
		else:
			result[ParameterInfo.ACTIVE] = False

	def write(cls, fn, plugin):
		fp = gzip.open(fn, 'w')
		fp.write('%d\n' % max(0, plugin.getMaxJobs()))
		data = filter(lambda (jobNum, pNum): jobNum != pNum, plugin.mapJob2PID.items())
		data = map(lambda (jobNum, pNum): '%d:%d' % (jobNum, pNum), data)
		fp.write('%s\n' % str.join(',', data))
	write = classmethod(write)


# Reader for CSV files
class CSVParameter(ParameterSource):
	pass
