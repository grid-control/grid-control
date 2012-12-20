from python_compat import *
from grid_control import AbstractError, AbstractObject, APIError, utils, QM

class ParameterInfo:
	reqTypes = ('ACTIVE', 'HASH', 'REQS')
	for idx, reqType in enumerate(reqTypes):
		locals()[reqType] = idx


class ParameterMetadata(str):
	def __new__(cls, value, untracked = False):
		obj = str.__new__(cls, value)
		obj.untracked = untracked
		return obj


class ParameterSource(AbstractObject):
	def __init__(self):
		self.mapJob2PID = {}
		self.resetParameterIntervention()

	def create(cls, pconfig, *args, **kwargs):
		return cls(*args, **kwargs)
	create = classmethod(create)

	def show(self, level = 0, other = ''):
		utils.vprint(('\t' * level) + self.__class__.__name__ + QM(other, ' [%s]' % other, ''), 1)

	def getMaxJobs(self):
		return None

	def fillParameterInfo(self, pNum, result):
		raise AbstractError

	def getJobInfo(self, jobNum):
		if jobNum == None:
			raise APIError('Unable to process jobNum None!')
		paramID = self.mapJob2PID.get(jobNum, jobNum)
		meta = {} # Speed and memory usage of dict is _much_ better than custom object
		meta[ParameterInfo.ACTIVE] = True
		meta[ParameterInfo.REQS] = []
		meta['MY_JOBID'] = jobNum
		meta['PARAM_ID'] = paramID
		self.fillParameterInfo(paramID, meta)
		return meta

	def fillParameterKeys(self, result):
		raise AbstractError

	def getJobKeys(self):
		result = map(lambda k: ParameterMetadata(k, untracked=True), ['MY_JOBID', 'PARAM_ID'])
		self.fillParameterKeys(result)
		return result

	def getParameterIntervention(self):
		return self.intervention

	def resetParameterIntervention(self):
		self.intervention = (set(), set(), False)

	def getJobIntervention(self):
		(redo, disable, sizeChange) = self.getParameterIntervention()
		self.resetParameterIntervention()
		redo = redo.difference(disable)
		if redo or disable:
			mapPID2Job = dict(map(lambda (k, v): (v, k), self.mapJob2PID.items()))
			translate = lambda pNum: mapPID2Job.get(pNum, pNum)
			return (sorted(map(translate, redo)), sorted(map(translate, disable)), sizeChange)
		if sizeChange:
			return (set(), set(), sizeChange)
		return None

	def doResync(self, old): # This function is _VERY_ time critical!
		from psource_meta import ChainParameterSource
		from psource_basic import InternalParameterSource
		mapJob2PID = {}
		def translatePlugin(plugin): # Reduces plugin output to essential information for diff
			keys_store = sorted(filter(lambda k: k.untracked == False, plugin.getJobKeys()))
			def translateEntry(meta): # Translates parameter setting into hash
				tmp = md5()
				for k in keys_store:
					tmp.update(k)
					value = meta.get(k, '')
					if isinstance(value, str):
						tmp.update(value)
					else:
						tmp.update(str(value))
				return { ParameterInfo.HASH: tmp.digest(), ParameterInfo.ACTIVE: meta[ParameterInfo.ACTIVE],
					'PARAM_ID': meta['PARAM_ID'], 'MY_JOBID': meta['MY_JOBID'] }
			if plugin.getMaxJobs():
				for jobNum in range(plugin.getMaxJobs()):
					yield translateEntry(plugin.getJobInfo(jobNum))
		params_old = list(translatePlugin(old))
		params_new = list(translatePlugin(self))
		redo = set()
		def sameParams(paramsAdded, paramsMissing, paramsSame, oldParam, newParam):
			if not oldParam[ParameterInfo.ACTIVE]:
				redo.add(newParam['PARAM_ID'])
			mapJob2PID[oldParam['MY_JOBID']] = newParam['PARAM_ID']
		(pAdded, pMissing, pSame) = utils.DiffLists(params_old, params_new,
			lambda a, b: cmp(a[ParameterInfo.HASH], b[ParameterInfo.HASH]), sameParams)
		# Construct complete parameter space plugin with missing parameter entries and intervention state
		# NNNNNNNNNNNNN OOOOOOOOO | source: NEW (==self) and OLD (==from file)
		# <same><added> <missing> | same: both in NEW and OLD, added: only in NEW, missing: only in OLD
		oldMaxJobs = old.getMaxJobs()
		for (idx, meta) in enumerate(pAdded):
			mapJob2PID[oldMaxJobs + idx] = meta['PARAM_ID']
		disable = set()
		newMaxJobs = self.getMaxJobs()
		for (idx, meta) in enumerate(pMissing):
			if meta[ParameterInfo.ACTIVE]:
				meta[ParameterInfo.ACTIVE] = False
				disable.add(newMaxJobs + idx)
			mapJob2PID[meta['MY_JOBID']] = newMaxJobs + idx
		result = self
		if pMissing:
			def genMissingEntry(short):
				tmp = old.getJobInfo(short['PARAM_ID'])
				tmp[ParameterInfo.ACTIVE] = False
				return tmp
			missingInfos = map(genMissingEntry, pMissing)
			result = ChainParameterSource(self, InternalParameterSource(missingInfos, old.getParameterNamesSet()))
		result.mapJob2PID = mapJob2PID
		if redo or disable:
			result.intervention = (redo, disable, oldMaxJobs != newMaxJobs)
		return result

ParameterSource.dynamicLoaderPath()
ParameterSource.managerMap = {}
