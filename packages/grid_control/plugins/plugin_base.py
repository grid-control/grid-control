from python_compat import *
from grid_control import AbstractError, AbstractObject, utils

class ParameterInfo:
	reqTypes = ('ACTIVE', 'HASH', 'REQS')
	for idx, reqType in enumerate(reqTypes):
		locals()[reqType] = idx


class ParameterMetadata(str):
	def __new__(cls, value, transient = False):
		obj = str.__new__(cls, value)
		obj.transient = transient
		return obj


class ParameterPlugin(AbstractObject):
	def __init__(self):
		self.mapJob2PID = {}
		self.intervention = (set(), set(), False)

	def getMaxJobs(self):
		return None

	def getPNumIntervention(self):
		tmp = self.intervention
		self.intervention = (set(), set(), False)
		return tmp

	def resolveDeps(self):
		return map(lambda k: '0%s' % k, self.getParameterNamesSet())

	def getParameterNamesSet(self):
		result = set(map(lambda k: ParameterMetadata(k, transient=True), ['MY_JOBID', 'PARAM_ID']))
		self.getParameterNames(result)
		return result

	def getParameterNames(self, result):
		if self.getMaxJobs() == None:
			info = self.getJobInfo(None)
			result.update(map(ParameterMetadata, info.keys()))
		else:
			for info in self.getAllJobInfos():
				result.update(map(ParameterMetadata, info.keys()))

	def getParameters(self, pNum, result):
		raise AbstractError

	def getJobInfo(self, jobNum):
		meta = {} # Speed and memory usage of dict is _much_ better than custom object
		meta[ParameterInfo.ACTIVE] = True
		meta[ParameterInfo.REQS] = []
		paramID = self.mapJob2PID.get(jobNum, jobNum)
		meta['MY_JOBID'] = jobNum
		self.getParameters(paramID, meta)
		meta['PARAM_ID'] = paramID
		return meta

	def getAllJobInfos(self):
		for x in range(max(0, self.getMaxJobs())):
			yield self.getJobInfo(x)

	def doResync(self, old): # This function is _VERY_ time critical!
		from plugin_meta import ChainParaPlugin
		from plugin_basic import InternalPlugin
		mapJob2PID = {}
		def translatePlugin(plugin, prune = False):
			keys_store = sorted(filter(lambda k: k.transient == False, plugin.getParameterNamesSet()))
			def translateEntry(meta):
				tmp = md5()
				for k in keys_store:
					tmp.update(k)
					if isinstance(meta[k], str):
						tmp.update(meta[k])
					else:
						tmp.update(str(meta[k]))
				return { ParameterInfo.HASH: tmp.digest(), ParameterInfo.ACTIVE: meta[ParameterInfo.ACTIVE],
					'PARAM_ID': meta['PARAM_ID'], 'MY_JOBID': meta['MY_JOBID'] }
			return map(translateEntry, plugin.getAllJobInfos())
		params_old = translatePlugin(old)
		params_new = translatePlugin(self, True)
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
		for (idx, meta) in enumerate(pAdded): # Append new parameter entries
			mapJob2PID[oldMaxJobs + idx] = meta['PARAM_ID']
		disable = set()
		newMaxJobs = self.getMaxJobs()
		for (idx, meta) in enumerate(pMissing): # Missing 
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
			result = ChainParaPlugin(self, InternalPlugin(missingInfos, old.getParameterNamesSet()))
		result.mapJob2PID = mapJob2PID
		if redo or disable:
			result.intervention = (redo, disable, oldMaxJobs != newMaxJobs)
		return result

	def getIntervention(self):
		(redo, disable, sizeChange) = self.getPNumIntervention()
		redo = redo.difference(disable)
		if redo or disable:
			mapPID2Job = dict(map(lambda (k, v): (v, k), self.mapJob2PID.items()))
			translate = lambda pNum: mapPID2Job.get(pNum, pNum)
			return (map(translate, redo), map(translate, disable), sizeChange)
		if sizeChange:
			return (set(), set(), sizeChange)
		return None
ParameterPlugin.dynamicLoaderPath()
ParameterPlugin.rawManagerMap = {}
ParameterPlugin.varManagerMap = {}
