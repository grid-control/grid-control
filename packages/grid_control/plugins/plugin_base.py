import itertools, operator
from python_compat import *
from grid_control import AbstractError, AbstractObject

# Init phase => yield sequentially all parameters, save in file
# Run phase => construct plugin from header, allow random access to parameters

# * Metadata format: (plugin reference, parameter key, parameter data, requirements)
# * Processed format: ({processed variables}, [requirements])

# Fast and small parameter data container
class ParameterMetadata(tuple):
	__slots__ = ()
	plugin = property(lambda x: x.__getitem__(0))
	key    = property(lambda x: x.__getitem__(1))
	data   = property(lambda x: x.__getitem__(2))
	reqs   = property(lambda x: x.__getitem__(3))


# Base class for parameter plugins
class ParameterPlugin(AbstractObject):
	def __init__(self, *args, **kargs):
		self.args = args
		self.kargs = kargs
		self.varNames = None
		self.translators = []

	# Modules can use this function to schedule parameter transformations
	def setTransform(self, what, where = lambda x: True):
		if where(self):
			try:
				self.translators.extend(what)
			except:
				self.translators.append(what)

	# Schedule parameter transformation on the data part of the meta data
	def setDataTransform(self, what, where = lambda x: True):
		self.setTransform(lambda (p, k, d, r): (p, k, what(d), r), where)

	# Apply the scheduled transformations sequentially
	def applyTransform(self, meta):
		return reduce(lambda x, y: y(x), self.translators, meta)

	# Return requirements for parameter set
	def getRequirements(self, pset):
		return []

	# Get list of processed parameter names
	def getProcessedNames(self):
		return set(reduce(operator.add, map(lambda (d, r): d.keys(), self.getProcessedParameters())))

	# Get parameters in processed form
	def getProcessedParameters(self):
		for meta in self.getParameterMetadata():
			result = ParameterMetadata(self.applyTransform(meta))
			yield (result.data, result.reqs)

	# Get parameters together with metadata
	# (<plugin>, <key>, <data = parameter set>, <requirements>)
	def getParameterMetadata(self):
		for pset in self.getParameters():
			yield ParameterMetadata([self, None, pset, self.getRequirements(pset)])

	# User friendly method to yield parameters (without any metadata)
	def getParameters(self):
		raise AbstractError

	# Go through all parameter sets and collect parameter names
	def getParameterNames(self):
		if self.varNames == None:
			varList = map(lambda meta: meta.data.keys(), self.getParameterMetadata())
			self.varNames = set(reduce(operator.add, varList))
		return self.varNames

	# Data serialization functions - override as needed
	#  Get data in header (plugin, [col1, col2,...])
	def getHeader(self):
		return (self, self.getParameterNames())
	#  Write data (col1, col2,...)
	def writeData(self, meta):
		def getWriteValue(name):
			value = meta.data.get(name)
			if value != None:
				return str(value)
			return ''
		return tuple(map(getWriteValue, self.getParameterNames()))
	#  Read data back into the plugin
	def readData(self, data):
		params = dict(filter(lambda (k, v): v != '', zip(self.getParameterNames(), [data])))
		if params:
			return ParameterMetadata([self, None, params, self.getRequirements(params)])
		else:
			return None
ParameterPlugin.dynamicLoaderPath()


# A persistent class is reconstructed when reading data from file
class PersistentParameter(ParameterPlugin):
	def getHeader(self):
		data = [self.__class__.__name__, self.args, self.kargs]
		return (self, ['!%s' % str.join('|', map(str, data))])


# Base class for indexed parameters where only a key is saved
class IndexedParameter(PersistentParameter):
	def getParameterMetadata(self):
		def expandIndex(key):
			data = self.getByIndex(key)
			return ParameterMetadata([self, key, data, self.getRequirements(data)])
		return map(expandIndex, itertools.count())

	# Main function which retrieves the data corresponding to a certain key
	def getByIndex(self, key):
		raise AbstractError

	def writeData(self, meta):
		return meta.key

	def readData(self, data):
		if data != '':
			idx = int(data)
			data = self.getByIndex(idx)
			return ParameterMetadata([self, idx, data, self.getRequirements(data)])
		else:
			return None


# Plugin for verbatim parameters and file I/O
class VerbatimParameter(ParameterPlugin):
	def __init__(self, varName, varValues = None):
		ParameterPlugin.__init__(self, [varName], varValues)
		(self.varNames, self.varValues) = ([varName], varValues)

	def getParameters(self):
		def varCleaner(data):
			if data:
				return {self.varNames[0]: data}
			return {}
		return map(varCleaner, self.varValues)


# Simple variable transformer
class varTransform(object):
	def __init__(self, oldKey, newKey, fun = lambda x: x):
		(self.oldKey, self.newKey, self.fun) = (oldKey, newKey, fun)

	def __call__(self, data):
		tmp = data.get(self.oldKey)
		if tmp != None:
			data.pop(self.oldKey)
			data[self.newKey] = self.fun(tmp)
		return data
