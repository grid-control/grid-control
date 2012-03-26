import os, csv, gzip
from plugin_base import ParameterPlugin, ParameterMetadata, ParameterInfo

# Reader for grid-control dump files
class GCDumpParaPlugin(ParameterPlugin):
	def __init__(self, fn):
		ParameterPlugin.__init__(self)
		fp = gzip.open(fn, 'rb')
		self.keys = eval(fp.readline().lstrip('#'))
		def parseLine(line):
			if not line.startswith('#'):
				jobNum, stored = map(str.strip, line.split('\t', 1))
				return ('!' in jobNum, int(jobNum.rstrip('!')), stored.split('\t'))
		self.values = map(parseLine, fp.readlines())

	def getMaxJobs(self):
		return len(self.values)

	def getParameterNames(self, result):
		result.update(map(ParameterMetadata, self.keys))

	def getParameters(self, pNum, result):
		result[ParameterInfo.ACTIVE] = not self.values[pNum][0]
		result['MY_JOBID'] = self.values[pNum][1]
		result.update(filter(lambda (k, v): v != None, zip(self.keys, self.values[pNum][2])))

	def write(cls, fn, plugin):
		fp = gzip.open(fn, 'wb')
		keys = filter(lambda p: p.transient == False, plugin.getParameterNamesSet())
		fp.write('# %s\n' % keys)
		for idx, meta in enumerate(plugin.getAllJobInfos()):
			if meta[ParameterInfo.ACTIVE]:
				fp.write('%d\t%s\n' % (idx, str.join('\t', map(lambda k: str(meta.get(k, '')), keys))))
			else:
				fp.write('%d!\t%s\n' % (idx, str.join('\t', map(lambda k: str(meta.get(k, '')), keys))))
	write = classmethod(write)


# Reader for quick resync state
class GCCacheParaPlugin(ParameterPlugin):
	def __init__(self, fn, plugin):
		ParameterPlugin.__init__(self)
		self.plugin = plugin
		fp = gzip.open(fn, 'r')
		self.maxN = int(fp.readline())
		if not self.maxN:
			self.maxN = None
		mapInfo = filter(lambda x: x, map(str.strip, fp.readline().split(',')))
		self.mapJob2PID = dict(map(lambda x: tuple(map(lambda y: int(y), x.split(':'))), mapInfo))

	def getMaxJobs(self):
		return self.maxN

	def getParameterNames(self, result):
		self.plugin.getParameterNames(result)

	def getParameters(self, pNum, result):
		if pNum < self.plugin.getMaxJobs():
			self.plugin.getParameters(pNum, result)
		else:
			result[ParameterInfo.ACTIVE] = False

	def getPNumIntervention(self):
		(result_redo, result_disable, result_sChange) = ParameterPlugin.getPNumIntervention(self)
		(plugin_redo, plugin_disable, plugin_sChange) = self.plugin.getPNumIntervention()
		result_redo.update(plugin_redo)
		result_disable.update(plugin_disable)
		return (result_redo, result_disable, result_sChange or plugin_sChange)

	def resolveDeps(self):
		return self.plugin.resolveDeps()

	def write(cls, fn, plugin):
		fp = gzip.open(fn, 'w')
		fp.write('%d\n' % max(0, plugin.getMaxJobs()))
		data = filter(lambda (jobNum, pNum): jobNum != pNum, plugin.mapJob2PID.items())
		data = map(lambda (jobNum, pNum): '%d:%d' % (jobNum, pNum), data)
		fp.write('%s\n' % str.join(',', data))
	write = classmethod(write)


# Reader for CSV files
class CSVParameter(ParameterPlugin):
	pass
