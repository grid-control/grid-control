from python_compat import *
from grid_control import AbstractError, AbstractObject, utils

# Fast and small parameter data container
class ParameterMetadata:
	def __init__(self):
		(self.store, self.transient, self.reqs) = ({}, {}, [])


class ParameterPlugin(AbstractObject):
	def __init__(self):
		self.jobMap = {}
		self.intervention = None

	def getMaxJobs(self):
		return None

	def getIntervention(self):
		tmp = self.intervention
		self.intervention = None
		return tmp

	def getParameterDeps(self):
		return []

	def getParameterNames(self):
		(result_store, result_transient) = (set(), set())
		if self.getMaxJobs() == None:
			info = self.getJobInfo(None)
			return (info.store.keys(), info.transient.keys())
		for info in self.getAllJobInfos():
			result_store.update(info.store.keys())
			result_transient.update(info.transient.keys())
		return (list(result_store), list(result_transient))

	def getParameters(self, pNum, result):
		raise AbstractError

	def getJobInfo(self, jobNum):
		meta = ParameterMetadata()
		paramID = self.jobMap.get(jobNum, jobNum)
		meta.transient['PARAM_ID'] = paramID
		meta.transient['MY_JOBID'] = jobNum
		self.getParameters(paramID, meta)
		return meta

	def getAllJobInfos(self):
		for x in range(max(0, self.getMaxJobs())):
			yield self.getJobInfo(x)

	def resync(self, old):
		from plugin_meta import ChainParaPlugin
		from plugin_basic import InternalPlugin
		jobMap = {}
		def cmpParams(a, b):
			return cmp(a.store, b.store)
		def sameParams(paramsAdded, paramsMissing, paramsSame, oldParam, newParam):
			jobMap[oldParam.transient['MY_JOBID']] = newParam.transient['PARAM_ID']
		(pAdded, pMissing, pSame) = utils.DiffLists(old.getAllJobInfos(), self.getAllJobInfos(), cmpParams, sameParams)
		# Construct complete parameter space plugin with missing psets and necessary intervention state
		for (idx, meta) in enumerate(pAdded):
			jobMap[self.getMaxJobs() + idx] = meta.transient['PARAM_ID']
		disable = []
		for (idx, meta) in enumerate(pMissing):
			disable.append(idx + self.getMaxJobs())
			jobMap[meta.transient['MY_JOBID']] = idx + self.getMaxJobs()
		result = self
		if pMissing:
			result = ChainParaPlugin([self, InternalPlugin(pMissing, old.getParameterNames())])
		result.jobMap = jobMap
		if disable:
			result.intervention = ([], disable)
		return result
ParameterPlugin.dynamicLoaderPath()
ParameterPlugin.rawManagerMap = {}
ParameterPlugin.varManagerMap = {}
