import os, csv
from grid_control import utils
from plugin_meta import *

# Reader for CSV files
class CSVParameter(ZipLongestParameter):
	def __init__(self, fileName, plugins = None):
		ZipLongestParameter.__init__(self, plugins)
		self.fileName = fileName
		self.transformQueue = []

		self.dialect = 'excel'
		if os.path.exists(fileName):
			try:
				sniffed = csv.Sniffer().sniff(open(self.fileName).read(1024))
				csv.register_dialect('sniffed', sniffed)
			except:
				pass

	# In case there are no plugins loaded, queue all scheduled transformations
	def setTransform(self, where, what):
		if self.plugins:
			ZipLongestParameter.setTransform(self, where, what)
		else:
			self.transformQueue.append((where, what))

	def loadFile(self):
		self.plugins = []
		reader = csv.reader(open(self.fileName, 'r'), dialect = self.dialect)
		# Instantiate plugins as defined in file header
		for idx, col in enumerate(next(reader)):
			if col.startswith('!'):
				(plugin, args, kargs) = col[1:].split('|')
				(args, kargs) = map(eval, (args, kargs))
				self.plugins.append(ParameterPlugin.open(plugin, *args, **kargs))
			else:
				self.plugins.append(VerbatimParameter(col))

		# Schedule all queued transformations
		self.transformQueue.reverse()
		for (where, what) in self.transformQueue:
			ZipLongestParameter.setTransform(self, where, what)
		self.transformQueue = []

		for pset in reader:
			# Read, parse and yield the parameter information
			result = []
			for (p, v) in zip(self.plugins, pset):
				data = p.readData(utils.parseType(v))
				if data:
					result.append(data)
			yield result
		self.plugins = []

	def getParameters(self):
		# Either use given parameter plugins or read from file
		if self.plugins:
			return ZipLongestParameter.getParameters(self)
		else:
			return self.loadFile()

	# Save data to file
	def saveFile(self):
		writer = csv.writer(open(self.fileName, 'w'), dialect = self.dialect)

		header = self.getHeader()
		writer.writerow(utils.flatten(map(lambda (plugin, head): head, header)))
		for meta in self.getParameterMetadata():
			data = dict(self.writeData(meta))
			row = map(lambda (plugin, head): data.get(plugin, tuple(['']*len(head))), header)
			writer.writerow(utils.flatten(row))
