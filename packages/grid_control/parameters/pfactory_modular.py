import os, re, shlex
from config_param import ParameterConfig
from psource_base import *
from psource_basic import *
from psource_meta import *
from psource_file import *
from pfactory_base import BasicParameterFactory
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


class ModularParameterFactory(BasicParameterFactory):
	def __init__(self, config, sections):
		BasicParameterFactory.__init__(self, config, sections)
		self.paramConfig = ParameterConfig(config, sections)
		self.pExpr = config.get(sections, 'parameters', '')


	def _getUserSource(self, pExpr):
		# Wrap plugin factory functions
		def createWrapper(cls):
			def wrapper(*args):
				try:
					return cls.create(self.paramConfig, *args)
				except:
					raise RethrowError('Error while creating %s with arguments "%s"' % (cls, args))
			return wrapper
		userFun = dict(map(lambda (key, cls): (key, createWrapper(cls)), ParameterSource.managerMap.items()))

		# Add plugin
		if pExpr:
			try:
				return eval(pExpr, userFun)
			except NameError:
				utils.eprint('Available functions: %s' % userFun.keys())
				raise
			except:
				utils.eprint('Available functions: %s' % userFun.keys())
				raise


	def _getRawSource(self, parent):
		userSrc = self._getUserSource(self.pExpr)
		if userSrc:
			return BasicParameterFactory._getRawSource(self, ZipLongParameterSource(parent, userSrc))
		return BasicParameterFactory._getRawSource(self, parent)
