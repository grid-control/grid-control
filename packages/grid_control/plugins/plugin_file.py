import os, csv
from plugin_base import *

# Reader for grid-control dump files
class GCDumpParaPlugin(ParameterPlugin):
	def __init__(self, fn):
		ParameterPlugin.__init__(self)
		fp = open(fn)
		self.keys = eval(fp.readline().lstrip('#'))
		def parseLine(line):
			if not line.startswith('#'):
				jobNum, stored = map(str.strip, line.split('\t', 1))
				return (int(jobNum), eval(stored))
		self.values = map(parseLine, fp.readlines())

	def getMaxJobs(self):
		return len(self.values)

	def getParameters(self, pNum, result):
		result.transient['MY_JOBID'] = self.values[pNum][0]
		result.store.update(filter(lambda (k, v): v != None, zip(self.keys, self.values[pNum][1])))

	def write(cls, fn, plugin):
		fp = open(fn, 'w')
		keys = list(plugin.getParameterNames()[0])
		fp.write('# %s\n' % keys)
		for idx, meta in enumerate(plugin.getAllJobInfos()):
			fp.write('%d\t%s\n' % (idx, map(lambda k: meta.store.get(k, None), keys)))
	write = classmethod(write)

# Reader for CSV files
class CSVParameter(ParameterPlugin):
	pass
