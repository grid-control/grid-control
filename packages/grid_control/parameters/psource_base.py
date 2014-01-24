import time
from python_compat import *
from grid_control import AbstractError, LoadableObject, APIError, utils, QM

class ParameterInfo:
	reqTypes = ('ACTIVE', 'HASH', 'REQS')
	for idx, reqType in enumerate(reqTypes):
		locals()[reqType] = idx


class ParameterMetadata(str):
	def __new__(cls, value, untracked = False):
		obj = str.__new__(cls, value)
		obj.untracked = untracked
		return obj

	def __repr__(self):
		return "'%s'" % QM(self.untracked, '!%s' % self, self)


class ParameterSource(LoadableObject):
	def create(cls, pconfig, *args, **kwargs):
		return cls(*args, **kwargs)
	create = classmethod(create)

	def __init__(self):
		self.resyncInfo = None
		self.resyncTime = -1 # Default - always resync
		self.resyncLast = None

	def getMaxParameters(self):
		return None

	def fillParameterKeys(self, result):
		raise AbstractError

	def fillParameterInfo(self, pNum, result):
		raise AbstractError

	def resyncCreate(self):
		return (set(), set(), False) # returns two sets of parameter ids and boolean (redo, disable, sizeChange)

	def resyncSetup(self, interval = None, force = None, info = None):
		self.resyncInfo = info # User override for base resync infos
		if interval != None:
			self.resyncTime = interval # -1 == always, 0 == disabled, >0 == time in sec between resyncs
			self.resyncLast = time.time()
		if force == True:
			self.resyncLast = None # Force resync on next attempt

	def resyncEnabled(self):
		if (self.resyncLast == None) or (self.resyncTime == -1):
			return True
		if self.resyncTime > 0:
			if time.time() - self.resyncLast > self.resyncTime:
				return True
		return False

	def resyncFinished(self):
		self.resyncLast = time.time()

	def resync(self): # needed when parameter values do not change but if meaning / validity of values do
		if self.resyncEnabled() and self.resyncInfo:
			return self.resyncInfo
		return self.resyncCreate()

	def show(self, level = 0, other = ''):
		utils.vprint(('\t' * level) + self.__class__.__name__ + QM(other, ' [%s]' % other, ''), 1)

	def getHash(self):
		raise AbstractError

ParameterSource.registerObject()
ParameterSource.managerMap = {}
