import itertools
from python_compat import *
from plugin_base import *

# Meta processing of parameter plugins
# Aggregates and propagates results and changes to plugins
class MetaParameter(ParameterPlugin):
	def __init__(self, *args, **kargs):
		ParameterPlugin.__init__(self, *args, **kargs)
		self.plugins = args[0]

	def setTransform(self, what, where = lambda x: True):
		for plugin in self.plugins:
			plugin.setTransform(what, where)

	def applyTransform(self, meta):
		reqs = []
		data = {}
		for subMeta in meta.data:
			reqs.extend(subMeta.reqs)
			data.update(subMeta.plugin.applyTransform(subMeta).data)
		return ParameterMetadata([self, None, data, reqs])

	# Return requirements for parameter set
	def getRequirements(self, pset):
		result = []
		for meta in pset:
			result.extend(meta.reqs)
		return result

	def getParameters(self):
		tmp = map(lambda x: x.getParameterMetadata(), self.plugins)
		for plist in self.tool(*tmp):
			yield list(plist)

	def getParameterNames(self):
		if self.varNames == None:
			allVars = map(lambda x: x.getParameterNames(), self.plugins)
			self.varNames = set(utils.flatten(varFlat))
		return self.varNames

	# For data serialization the plugin information is flattend
	def getHeader(self):
		header = []
		for p in self.plugins:
			head = p.getHeader()
			# Flatten header information
			if isinstance(head, list):
				header.extend(head)
			else:
				header.append(p.getHeader())
		return header

	def writeData(self, meta):
		result = []
		for subMeta in meta.data:
			data = subMeta.plugin.writeData(subMeta)
			if isinstance(data, list):
				result.extend(data)
			else:
				result.append((subMeta.plugin, data))
		return result

	def tool(self, *args):
		raise AbstractError


# Chain parameter plugins
class ChainParameter(MetaParameter):
	tool = itertools.chain


# Zip an array of parameter plugins together
# (yield up to the shortest plugin)
class ZipParameter(MetaParameter):
	tool = itertools.izip


# Zip an array of parameter plugins together
# (yield up to the longest plugin)
class ZipLongestParameter(MetaParameter):
	def tool(self, *args):
		plugins = list(args)
		# Yield as long as there is a single plugin with data
		while len(plugins) > 0:
			result = []
			for p in plugins[:]:
				try:
					result.append(next(p))
				except StopIteration:
					# Remove finished generator from plugin list
					plugins.remove(p)
			if len(result) > 0:
				yield result


# Permutation parameter plugins
class PermuteParameter(MetaParameter):
	def getParameters(self):
		def permute(args):
			for meta in args[0].getParameterMetadata():
				if len(args) > 1:
					for base in permute(args[1:]):
						yield base + [meta]
				elif len(args) == 1:
					yield [meta]
		return permute(self.plugins)


# Connect two plugins with matching variable
# Requires a list with plugins and a list with
# comparator objects (returns match between metadata)
# Returns only complete matches between plugins
class ConnectParameters(MetaParameter):
	def __init__(self, *args, **kargs):
		MetaParameter.__init__(self, *args, **kargs)
		(self.plugins, connectors) = args
		def tryMatchObj(x):
			try:
				return x.match
			except:
				return x
		self.matchList = map(tryMatchObj, connectors)
		if len(self.plugins) - 1 != len(self.matchList):
			raise RuntimeError('Invalid parameters %s' % repr(args))

	def tool(self, *args):
		for main in self.plugins[0].getParameterMetadata():
			iters = map(lambda x: x.getParameterMetadata(), self.plugins[1:])
			tmp = map(lambda (f, meta): f(main, meta), zip(self.matchList, iters))
			if not None in tmp:
				yield tmp + [main]


# Connector between processed variables
class ProcessedConnector(object):
	def __init__(self, var, eq = str.__eq__):
		self.var = var
		self.eq = lambda x, y: eq(str(x), str(y))

	def match(self, main, other):
		(x1, x2, data, x4) = main.plugin.applyTransform(main)
		searchValue = data.get(self.var)
		for meta in other:
			(y1, y2, lookup, y4) = meta.plugin.applyTransform(meta)
			if self.eq(searchValue, lookup.get(self.var)):
				return meta
		return None
