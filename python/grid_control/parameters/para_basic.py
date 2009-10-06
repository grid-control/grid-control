from para_base import ParaMod
import csv

class SimpleParaMod(ParaMod):
	def __init__(self, config):
		ParaMod.__init__(self, config)
		self.paraValues = config.get('ParaMod', 'parameter values')
		self.paraName = config.get('ParaMod', 'parameter name', 'PARAMETER').strip()

	def getParams(self):
		# returns list of dictionaries
		return map(lambda x: {self.paraName: x}, map(str.strip, self.paraValues.split()))


class LinkedParaMod(SimpleParaMod):
	def __init__(self, config):
		SimpleParaMod.__init__(self, config)

	def getParams(self):
		for value in filter(lambda x: x != '', map(str.strip, self.paraValues.split('\n'))):
			yield dict(zip(map(str.strip, self.paraName.split(":")), map(str.strip, value.split(":"))))


class FileParaMod(ParaMod):
	def __init__(self, config):
		ParaMod.__init__(self, config)
		self.path = config.getPath('ParaMod', 'parameter source')
		sniffed = csv.Sniffer().sniff(open(self.path).read(1024))
		csv.register_dialect('sniffed', sniffed)
		self.dialect = config.get('ParaMod', 'parameter source dialect', 'sniffed')

	def getParams(self):
		def cleanupDict(d):
			# strip all key value entries
			tmp = tuple(map(lambda item: map(str.strip, item), d.items()))
			# filter empty parameters
			return filter(lambda (k, v): k != '', tmp)
		tmp = list(csv.DictReader(open(self.path), dialect = self.dialect))
		return map(lambda d: dict(cleanupDict(d)), tmp)
