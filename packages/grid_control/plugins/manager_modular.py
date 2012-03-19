import os, re, shlex
from plugin_base import *
from plugin_meta import *
from plugin_file import *
from manager_base import BasicPluginManager
from grid_control import AbstractObject, QM, utils

def frange(start, end = None, num = None, steps = None, format = "%g"):
	if (end == None) and (num == None):
		raise exceptions.ConfigError("frange: No exit condition!")
	if (end != None) and (num != None) and (steps != None):
		raise exceptions.ConfigError("frange: Overdetermined parameters!")
	if (end != None) and (num != None) and (steps == None):
		steps = (end - start) / (num - 1)
		num -= 1
	if (end != None) and (num == None):
		steps = QM(steps, steps, 1)
		num = int(1 + (end - start) / steps)
	result = map(lambda i: start + QM(steps, steps, 1) * i, range(num)) + QM(end, [end], [])
	return map(lambda x: format % x, result)


def parseTuples(s):
	def funCycle():
		while True:
			yield lambda entry: map(str.strip, shlex.split(entry))
			yield lambda entry: [tuple(map(str.strip, entry.split(',')))]
	for (fun, entry) in zip(funCycle(), re.split('\(([^\)]*)\)*', s)):
		for result in fun(entry):
			yield result


class ModularPluginManager(BasicPluginManager):
	def __init__(self, config, section, pExpr = None):
		BasicPluginManager.__init__(self, config, section)
		if pExpr == None:
			self.pExpr = config.get(section, 'parameters', '')
		else:
			self.pExpr = pExpr
		if self.pExpr == '':
			return
		# Get parameters from config file
		optsParam = filter(lambda k: not k.endswith(' type'), config.getOptions('parameters'))
		optsParam = filter(lambda k: k not in ['repeat', 'parameters'], optsParam)
		varSource = dict(map(lambda k: (list(parseTuples(k))[0], config.get('parameters', k)), optsParam))
		# Find out how to parse parameters:
		optsType = filter(lambda k: k.endswith(' type'), config.getOptions('parameters'))
		varType = dict(map(lambda k: (list(parseTuples(k))[0], config.get('parameters', k)), optsType))
		# Apply parsing to parameters
		parseDict = {
			'verbatim': lambda value: value,
			'lines': lambda value: value.splitlines(),
			'dict': lambda value: utils.parseDict(value)[0],
			'default': lambda value: list(parseTuples(value)),
			'binning': lambda value: zip(list(parseTuples(value)), list(parseTuples(value))[1:]),
			'expr': lambda value: eval(QM(value == '', 'None', value)),
		}
		self.varStore = {}
		for pkey in varSource:
			ptype = QM('=>' in varSource[pkey], 'dict', varType.get(pkey, 'default'))
			varSource[pkey] = parseDict[ptype](varSource[pkey])
			if isinstance(pkey, tuple):
				entries = map(lambda v: dict(zip(pkey, v)), varSource[pkey])
				for k in pkey:
					parser = parseDict[varType.get(k, 'verbatim')]
					self.varStore[k] = map(lambda e: parser(e.get(k)), entries)
			elif isinstance(varSource[pkey], dict):
				parser = parseDict[varType.get(pkey, 'verbatim')]
				varSource[pkey] = dict(map(lambda (k, v): (k, parser(v)), varSource[pkey].items()))
		self.varStore.update(varSource)

	def getSource(self, doInit, doResync):
		# Wrap plugin factory functions
		userFun = dict(ParameterPlugin.rawManagerMap)
		def createWrapper(fun):
			return lambda *args: fun(self.varStore, *args)
		for fun in ParameterPlugin.varManagerMap:
			userFun[fun] = createWrapper(ParameterPlugin.varManagerMap[fun])
		# Add plugin
		if self.pExpr:
			try:
				self.plugins.append(eval(self.pExpr, userFun))
			except NameError:
				raise
			except:
				raise
		return BasicPluginManager.getSource(self, doInit, doResync)
