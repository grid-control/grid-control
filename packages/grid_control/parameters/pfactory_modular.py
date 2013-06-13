import os, re, shlex
from psource_base import ParameterSource
from psource_meta import ZipLongParameterSource
from psource_file import *
from pfactory_base import BasicParameterFactory
from grid_control import AbstractObject, QM, utils, RethrowError

# Parameter factory which evaluates a parameter module string
class ModularParameterFactory(BasicParameterFactory):
	def __init__(self, config, sections):
		BasicParameterFactory.__init__(self, config, sections)
		self.pExpr = self.paramConfig.get('parameters', '')


	def _getUserSource(self, pExpr, parent):
		if not pExpr:
			return parent
		# Wrap plugin factory functions
		def createWrapper(cls):
			def wrapper(*args):
				try:
					return cls.create(self.paramConfig, *args)
				except:
					raise RethrowError('Error while creating %s with arguments "%s"' % (cls, args))
			return wrapper
		userFun = dict(map(lambda (key, cls): (key, createWrapper(cls)), ParameterSource.managerMap.items()))
		try:
			source = eval(pExpr, userFun)
		except:
			utils.eprint('Available functions: %s' % userFun.keys())
			raise
		return ZipLongParameterSource(parent, source)


	def _getRawSource(self, parent):
		return BasicParameterFactory._getRawSource(self, self._getUserSource(self.pExpr, parent))
