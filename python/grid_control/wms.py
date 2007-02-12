# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

import os
from grid_control import AbstractObject, RuntimeError, enumerate

class WMS(AbstractObject):
	reqTypes = ('MEMBER', 'WALLTIME', 'STORAGE')
	_reqTypeDict = {}
	for id, reqType in enumerate(reqTypes):
		_reqTypeDict[reqType] = id
		locals()[reqType] = id


	def __init__(self, config, module):
		self.config = config
		self.module = module
		self.workDir = config.getPath('global', 'workdir')


	def _formatRequirement(self, type, *args):
		if type == self.MEMBER:
			return self.memberReq(*args)
		elif type == self.WALLTIME:
			return self.wallTimeReq(*args)
		elif type == self.STORAGE:
			return self.storageReq(args)
		else:
			raise RuntimeError('unknown request type %d' % type)


	def formatRequirements(self, reqs):
		return str.join(' && ', map(lambda x: self._formatRequirement(*x), reqs))


	def memberReq(self, *args):
		raise RuntimeError('memberReq is abstract')


	def wallTimeReq(self, *args):
		raise RuntimeError('wallTimeReq is abstract')
