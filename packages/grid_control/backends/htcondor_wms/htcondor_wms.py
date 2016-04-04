# | Copyright 2014-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

# -*- coding: utf-8 -*-

# core modules
import os, logging
from grid_control import utils
from grid_control.backends.htcondor_wms.htcondor_schedd import HTCScheddFactory
from grid_control.backends.htcondor_wms.wmsid import HTCJobID
from grid_control.backends.wms import BackendError, BasicWMS
from grid_control.config import ConfigError
from grid_control.job_db import Job
from python_compat import json, lfilter

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
	configSections = BasicWMS.configSections + ['htcondor']
	"""
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
			'jobNum'    : [ 'GcJobNum' ],
			'wmsID'     : [ 'GcID' ],
			'rawID'     : [ 'rawID' ],
			'state'     : [ 'JobStatus' ],
			'hold'      : [ 'HoldReasonCode' ],
			'holdSub'   : [ 'HoldReasonSubCode' ],
			'queuetime' : [ 'QDate' ],
			'donetime'  : [ 'CompletionDate' ],
			'host'      : [ 'RemoteHost', 'LastRemoteHost' ],
			}
	_jobFeatureMapDef = {
		'CPUS'   : ['request_cpus'],
		'MEMORY' : ['request_memory', '%dMB'],
		}
	# Initialization
	def __init__(self, config, wmsName):
		self._initLogger()
		BasicWMS.__init__(self, config, wmsName)
		# setup the connection to pools and their interfaces
		self._sandboxDir  = config.getPath('sandbox path', config.getWorkPath('sandbox.%s'%wmsName), mustExist = False)
		self._initPoolInterfaces(config)
		self._jobSettings = {
			"Universe" : config.get("universe", "vanilla"),
			"ClassAd" : config.getList("append info", []),
			"JDL" : config.getList("append opts", []),
			}

	def _initLogger(cls):
		cls._logger = logging.getLogger('backend.%s' % cls.__name__)
		cls._log = cls._logger.log
	_initLogger = classmethod(_initLogger)

	def _initPoolInterfaces(self, config):
		"""
		Establish adapters and connection details for pool and schedd
		"""
		poolConfig = {}
		for poolConfigFileName in config.getList('poolConfig', [], onChange = None):
			try:
				self._log(logging.DEBUG1,"Reading pool config '%s'"%poolConfigFileName)
				confFile = open(poolConfigFileName, 'r')
				poolConfig.update(json.load(confFile)) # TODO: json
				confFile.close()
			except Exception:
				raise BackendError('Failed to parse pool configuration file!')
		self._jobFeatureMap = poolConfig.get('jobFeatureMap',{})
		self._queueQueryMap = poolConfig.get('queueQueryMap',{})
		self._niceName      = poolConfig.get('NiceName', '<POOLNAME>')
		cfgScheddURI = config.get('ScheddURI','', onChange = changeOnlyUnset)
		if cfgScheddURI:
			self._schedd = HTCScheddFactory(cfgScheddURI, parentPool=self)
		else:
			self._schedd = self._getDynamicSchedd(poolConfig)
			config.set('ScheddURI', self._schedd.getURI())
			self._log(logging.INFO1,'Using Schedd %s (none explicitly defined)'%(self._schedd.getURI()))
		self._log(logging.INFO1,'Connected to Schedd %s'%(self._schedd.getURI()))

	def _getDynamicSchedd(self, poolConfig):
		"""
		Pick a schedd based on best guess
		"""
		candidateURIList = []
		candidateURIList.extend(poolConfig.get('ScheddURIs',[]))
		candidateURIList.append('localhost://')
		self._log(logging.DEBUG1,"Checking Schedd URI list: '%s'"%("','".join(candidateURIList)))
		for scheddCandidate in candidateURIList:
			try:
				self._log(logging.DEBUG2,"Testing Schedd URI '%s'" % scheddCandidate)
				candidate = HTCScheddFactory(scheddCandidate, parentPool=self)
				if candidate.getCanSubmit():
					return candidate
				else:
					self._log(logging.DEBUG3,"Reached schedd, but cannot submit." % scheddCandidate)
			except ValueError:
				continue
		raise ConfigError('Unable to guess valid HTCondor Schedd.')

	# Information interfaces
	def getTimings(self):
		return self._schedd.getTimings()

	# path functions shared with schedds
	def getJobCfgPath(self, jobNum = "%d"):
		cfgName = 'job_%s.var' % jobNum
		return os.path.join(self.config.getWorkPath('jobs'), cfgName), cfgName

	def getSandboxPath(self, subdirToken=""):
		sandpath = os.path.join(self._sandboxDir, str(subdirToken), '' )
		if not os.path.exists(sandpath):
			try:
				os.makedirs(sandpath)
			except Exception:
				raise BackendError('Error accessing or creating sandbox directory:\n	%s' % sandpath)
		return sandpath

	# Primary backend actions
	def submitJobs(self, jobNumList, task):
		requestLen = len(jobNumList)
		activity = utils.ActivityLog('Submitting jobs... (--%)')
		while jobNumList:
			jobSubmitNumList = jobNumList[-self._schedd.getSubmitScale():]
			del(jobNumList[-self._schedd.getSubmitScale():])
			activity = utils.ActivityLog('Submitting jobs... (%2d%%)'%(100*(requestLen-len(jobNumList))/requestLen))
			for jobNum in jobSubmitNumList:
				self._writeJobConfig(
					self.getJobCfgPath(jobNum)[0],
					jobNum,
					task, {}
					)
			rawJobInfoMaps = self._schedd.submitJobs(
				jobSubmitNumList, 
				task,
				self._getQueryArgs()
				)
			# Yield (jobNum, wmsId, other data) per jobZ
			jobInfoMaps = self._digestQueueInfoMaps(rawJobInfoMaps)
			for htcID in jobInfoMaps:
				yield (
					htcID.gcJobNum,
					self._createGcId(htcID),
					jobInfoMaps[htcID]
					)
		del(activity)

	def checkJobs(self, wmsJobIdList):
		if not len(wmsJobIdList):
			raise StopIteration
		activity   = utils.ActivityLog('Checking jobs...')
		assert not bool(lfilter( lambda htcid: htcid.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList))), 'Bug! Got jobs at Schedds %s, but servicing only Schedd %s' % (lfilter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList)), self._schedd.getURI())
		rawJobInfoMaps = self._schedd.checkJobs(
			self._splitGcRequests(wmsJobIdList),
			self._getQueryArgs()
			)
		# Yield (jobNum, wmsId, state, other data) per active jobs
		jobInfoMaps = self._digestQueueInfoMaps(rawJobInfoMaps)
		for htcID in jobInfoMaps:
			yield (
				htcID.gcJobNum,
				self._createGcId(htcID),
				self._statusMap[jobInfoMaps[htcID]['state']][0],
				jobInfoMaps[htcID]
				)
		del(activity)

	def _getJobsOutput(self, wmsJobIdList):
		if not len(wmsJobIdList):
			raise StopIteration
		activity   = utils.ActivityLog('Fetching jobs...')
		assert not bool(lfilter( lambda htcid: htcid.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList))), 'Bug! Got jobs at Schedds %s, but servicing only Schedd %s' % (lfilter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList)), self._schedd.getURI())
		returnedJobs = self._schedd.getJobsOutput(
			self._splitGcRequests(wmsJobIdList)
			)
		# Yield (jobNum, outputPath) per retrieved job
		for htcID in returnedJobs:
			yield (
				htcID.gcJobNum,
				self.getSandboxPath(htcID.gcJobNum)
				)
		del activity
	
	def cancelJobs(self, wmsJobIdList):
		if not len(wmsJobIdList):
			raise StopIteration
		activity   = utils.ActivityLog('Canceling jobs...')
		assert not bool(lfilter( lambda htcid: htcid.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList))), 'Bug! Got jobs at Schedds %s, but servicing only Schedd %s' % (lfilter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(wmsJobIdList)), self._schedd.getURI())
		canceledJobs = self._schedd.cancelJobs(
			self._splitGcRequests(wmsJobIdList)
			)
		# Yield ( jobNum, wmsID) for canceled jobs
		for htcJobID in canceledJobs:
			yield (
				htcJobID.gcJobNum,
				self._createGcId(htcJobID)
				)
		del activity

	# GC/WMS/Job ID converters
	def _createGcId(self, htcID):
		"""Create a GcId for a given htcID"""
		return self._createId(htcID.rawID)
	def _splitGcId(self, gcId):
		"""Split a GcId, returning wmsName and htcJobID"""
		wmsName, rawId = self._splitId(gcId)
		return (wmsName, HTCJobID(rawID = rawId))
	def _splitGcRequests(self, jobNumGcIdList):
		"""Process sequence of (GcId, jobNum), returning sequence of htcIDs"""
		return [ self._splitGcId(gcId)[1] for gcId, jobNum in jobNumGcIdList ]
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
		return qqM
	def _getQueryArgs(self):
		"""ClassAd names to query Condor with"""
		qqM = self._getQueueQueryMap()
		return utils.flatten(qqM.values())
	def _digestQueueInfoMaps(self, queueInfoMaps):
		result = {}
		for htcID in queueInfoMaps:
			result[htcID] = self._digestQueueInfoMap(queueInfoMaps[htcID])
		return result
	def _digestQueueInfoMap(self, queueInfoMap):
		"""
		Translate a query info maps into a GC information map
		
		 Picks the most relevant keys to find the best match for
		a single GC key our of multiple HTC keys.
		"""
		qqM = self._getQueueQueryMap()
		infoDict = {}
		for gcKey, queryArgList in qqM.items():
			for queryArg in queryArgList:
				if queryArg in queueInfoMap:
					infoDict[gcKey] = queueInfoMap[queryArg]
					break
		return infoDict

	
	def jdlRequirementMap(self):
		jrm = {}
		jrm.update(self._jobFeatureMapDef)
		jrm.update(self._jobFeatureMap)
		for key in jrm:
			if isinstance(jrm[key], str):
				jrm[key] = [jrm[key]]
			if len(jrm[key]) == 1:
				jrm[key].append('%s')
		return jrm
	jdlRequirementMap = property(jdlRequirementMap)
