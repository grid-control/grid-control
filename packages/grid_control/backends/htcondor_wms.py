# -*- coding: utf-8 -*-

# core modules
import sys

# standard modules
import itertools
import logging

# GC modules
import utils
import json
from abstract     import LoadableObject
from wms          import BasicWMS
from grid_control import Job

# HTC modules
from htcondor_api.htcondor_schedd import HTCScheddFactory
from htcondor_api.wmsid           import HTCJobID

"""
HTCondor backend core
 This module provides the backend for HTCondor pools of version 8+.

 The 'HTCondor' class provides a GC compatible WMS-backend and
handles interaction with all other components.

Conventions:
  o Job Identification/accounting
Internal components uniquely identify jobs via blobs of
  jobData = (int(jobNum), str(taskID), int(clusterID), int(processID))
while job activities are directed via job data maps of
  {ScheddURI : [jobData,jobData,jobData,...] }

The <WMSID> of GC is formatted as WMSID.<WMSName>.<RAWID>, with
the <RAWID> formatted as <poolURI>.<TaskID>.<JobNum>.<ClusterID>.<ProcID>, e.g.
   WMSID.HTCondor.ssh://ekpcms5:22/opt/htc/.6fe1e996253f.325.2031.13

   o Files
Since the HTC backends operates on remote machines, it may use both
Sandbox and Staging folders. On local machines, the Staging folder is
a link copy. For convention with other backends, these correspond to:
  Sandbox: The folder where GC works on
  Staging: The folder where HTCondor works on
"""

class HTCondor(BasicWMS):
	""""
	Backend for HTCondor 8+
	"""
	_statusMap = {
		'0' : ( Job.SUBMITTED, 'Unexpanded' )  ,# unexpanded    never been run
		'1' : ( Job.SUBMITTED, 'InQueue'    )  ,# idle          waiting for assignment to a node
		'2' : ( Job.RUNNING  , 'Running'    )  ,# running       executed on a node
		'3' : ( Job.FAILED   , 'Removed'    )  ,# removed       removed from queue
		'4' : ( Job.DONE     , 'Completed'  )  ,# completed     exited with any code
		'5' : ( Job.WAITING  , 'Frozen'     )  ,# on hold       temporarily (?) frozen
		'6' : ( Job.RUNNING  , 'Running'    )  ,# transferring  transferring input/output
		'7' : ( Job.QUEUED   , 'Suspended'  )  ,# suspended     activity is paused
	}
	_queueQueryMapDef = {
			'clusterId' : [ 'ClusterId'] ,
			'processId' : [ 'ProcId' ],
			'jobNum'    : [ 'GcJobNum' ] ,
			'wmsID'     : [ 'GcID' ],
			'state'     : [ 'JobStatus' ],
			'hold'      : [ 'HoldReasonCode' ],
			'holdSub'   : [ 'HoldReasonSubCode' ],
			'queuetime' : [ 'QDate' ],
			'donetime'  : [ 'CompletionDate' ],
			'host'      : [ 'RemoteHost', 'LastRemoteHost' ],
			}
	# Initialization
	def __init__(self, config, wmsName):
		self._initLogger()
		BasicWMS.__init__(self, config, wmsName, 'HTCondor')
		# setup the connection to pools and their interfaces
		self._sandboxDir  = config.getPath('sandbox path', config.getWorkPath('sandbox.%s'%wmsName), mustExist = False)
		self._initPoolInterfaces(config)
		self._jobSettings = {
			"Universe" : config.get("Universe", "vanilla"),
			"ClassAd" : config.getList("append ClassAd", []),
			"JDL" : config.getList("append JDL", []),
			}

	@classmethod
	def _initLogger(self):
		self._logger = logging.getLogger('backend.%s' % self.__class__.__name__)
		self._log = self._logger.log

	def _initPoolInterfaces(self, config):
		"""
		Establish adapters and connection details for pool and schedd
		"""
		poolConfig = {}
		for poolConfigFileName in config.getList('poolConfig', onChange = None):
			try:
				confFile = open(poolConfigFileName, 'r')
				poolConfig.update(json.load(confFile)) # TODO: json
				confFile.close()
			except Exception:
				raise RethrowError('Failed to parse pool configuration file!')
		self._jobFeatureMap = poolConfig.get('jobFeatureMap',{})
		self._queueQueryMap = poolConfig.get('queueQueryMap',{})
		self._niceName      = poolConfig.get('NiceName', '<POOLNAME>')
		if config.get('ScheddURI',''):
			self._schedd = HTCScheddFactory(config.get('ScheddURI',''), parentPool=self)
		else:
			self._schedd = self._getDynamicSchedd(poolConfig)
			config.set('ScheddURI', self._schedd.getURI())
			self._log(logging.INFO1,'Using Schedd %s (none defined)'%(self._schedd.getURI()))
		self._log(logging.INFO1,'Connected to Schedd %s'%(self._schedd.getURI()))

	def _getDynamicSchedd(self, poolConfig):
		"""
		Pick a schedd based on best guess
		"""
		self._log(logging.DEBUG1,'Selecting Schedd from Pool (none explicitly defined)')
		candidateURIList = []
		candidateURIList.extend(poolConfig.get('ScheddURIs',[]))
		candidateURIList.append('localhost://')
		self._log(logging.DEBUG3,'Selecting Schedd from URI list: %s'%(','.join(candidateURIList)))
		for scheddCandidate in candidateURIList:
			try:
				candidate = HTCScheddFactory(scheddCandidate, parentPool=self)
				if candidate.getCanSubmit():
					return candidate
			except NotImplementedError:
				continue
		raise ConfigError('Unable to guess valid HTCondor Schedd.')

	# Information interfaces
	def getTimings(self):
		return self._schedd.getTimings()

	# path functions shared with schedds
	def getJobCfgPath(self, jobNum = "%d"):
		cfgName = 'job_%s.var' % jobNum
		return os.path.join(config.getWorkPath('jobs'), cfgName), cfgName

	def getSandboxPath(self, subdirToken=""):
		sandpath = os.path.join(self._sandboxDir, str(subdirToken), '' )
		if not os.path.exists(sandpath):
			try:
				os.makedirs(sandpath)
			except Exception:
				raise RethrowError('Error accessing or creating sandbox directory:\n	%s' % sandpath)
		return sandpath

	# Primary backend actions
	def submitJobs(self, jobNumList, task):
		requestLen = len(jobNumList)
		activity = utils.ActivityLog('Submitting jobs... (--%)')
		while jobNumList:
			jobSubmitNumList = jobNumList[-self._schedd.getSubmitScale():]
			del(jobNumList[-self._schedd.getSubmitScale():])
			activity = utils.ActivityLog('Submitting jobs... (%2d%)'%(100*(requestLen-len(jobNumList))/requestLen))
			for jobNum in jobSubmitNumList:
				self._writeJobConfig(
					self._getJobCfgPath(jobNum),
					jobNum,
					task
					)
			rawJobInfoMaps = self._schedd.submitJobs(
				jobSubmitNumList, 
				task,
				self._getQueryArgs()
				)
			# Yield (jobNum, wmsId, other data) per job
			jobInfoMaps = self._digestQueueInfoMap(rawJobInfoMaps)
			for htcID in jobInfoMaps:
				yield (
					htcID.jobNum,
					htcID.rawID,
					jobInfoMaps[htcID]
					)
		del(activity)

	def checkJobs(self, wmsJobIdList):
		activity   = utils.ActivityLog('Checking jobs...')
		assert bool(filter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList))), 'Bug! Got jobs at Schedd %s, but servicing only Schedd %s' % (scheddURI, self._schedd.getURI())
		rawJobInfoMaps = self._schedd.checkJobs(
			self._splitGcRequests(wmsJobIdList),
			self._getQueryArgs()
			)
		# Yield (jobNum, wmsId, state, other data) per active jobs
		jobInfoMaps = self._digestQueueInfoMap(rawJobInfoMaps)
		for htcID in jobInfoMaps:
			yield (
				htcID.jobNum,
				htcID.rawID,
				self._statusMap[jobInfoMaps[htcID]['state']],
				jobInfoMaps[htcID]
				)
		del(activity)

	def _getJobsOutput(self, wmsJobIdList):
		activity   = utils.ActivityLog('Fetching jobs...')
		assert bool(filter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList))), 'Bug! Got jobs at Schedd %s, but servicing only Schedd %s' % (scheddURI, self._schedd.getURI())
		returnedJobs = self._schedd.getJobsOutput(
			self._splitGcRequests(wmsJobIdList)
			)
		# Yield (jobNum, outputPath) per retrieved job
		for htcID in returnedJobs:
			yield (
				htcID.jobNum,
				self.getSandboxPath(htcID.jobNum)
				)
		del activity
	
	def cancelJobs(self, wmsJobIdList):
		activity   = utils.ActivityLog('Canceling jobs...')
		assert bool(filter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList))), 'Bug! Got jobs at Schedd %s, but servicing only Schedd %s' % (scheddURI, self._schedd.getURI())
		canceledJobs = self._schedd.cancelJobs(
			self._splitGcRequests(wmsJobIdList)
			)
		# Yield ( jobNum, wmsID) for canceled jobs
		for htcJobID in canceledJobs:
			yield (
				htcJobID.jobNum,
				self._createGcId(htcJobID)
				)
		del activity

	# GC/WMS/Job ID converters
	def _createGcId(self, htcJobID):
		"""Create a GcId for a given htcJobID"""
		return self._createId(htcJobID.rawID)
	def _splitGcId(self, gcId):
		"""Split a GcId, returning wmsName and htcJobID"""
		wmsName, rawId = self._splitId(gcId)
		return (wmsName,) + HTCJobID(rawID = rawId)
	def _splitGcRequests(self, gcRequests):
		"""Process sequence of (jobNum, GcId), returning sequence of htcIDs"""
		return [ HTCJobID( rawID = self._splitGcId(gcId)[1] ) for jobNum, gcId in jobNumGcIdList ]
	def _getJobDataMap(self, jobNumGcIdList):
		"""Process list of (jobNum, GcId), returning {ScheddURI : [(jobNum, taskID, clusterID, processID),...] }"""
		scheddJobMap = {}
		for jobNum, GcId in jobNumGcIdList:
			_, scheddURI, taskID, clusterID, processID = _splitGcId(GcId)
			scheddJobMap.setdefault(scheddURI, []).append(tuple(jobNum, taskID, clusterID, processID))
		return scheddJobMap

	# Queue queries and status processing
	def _getQueueQueryMap(self):
		qqM = {}
		qqM.update(self._queueQueryMapDef)
		qqM.update(self._queueQueryMap)
	def _getQueryArgs(self):
		"""ClassAd names to query Condor with"""
		qqm = self._getQueueQueryMap(self)
		return utils.flatten(qqM.values())
	def _digestQueueInfoMap(self, queueInfoMap):
		qqm = self._getQueueQueryMap(self)
		infoDict = {}
		for gcKey, queryArgList in qqM.items():
			for queryArg in queryArgList:
				if queryArg in rawDict:
					infoDict[gcKey] = rawDict[queryArg]
					break
		return infoDict
