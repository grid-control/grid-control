# -*- coding: utf-8 -*-

# core modules
import sys

# standard modules
import itertools
import logging

# GC modules
import utils
from condor_schedd import HTCScheddFactory

"""
HTCondor backend core
 This module provides the backend for HTCondor pools of version 8+.

 The 'HTCondorPool' class provides a GC compatible WMS-backend and
handles interaction with all other components.

Conventions:
  o Job Identification/accounting
Internal components uniquely identify jobs via blobs of
  jobData = (int(jobNum), str(taskID), int(clusterID), int(processID))
while job activities are directed via job data maps of
  {ScheddURI : [jobData,jobData,jobData,...] }

The <WMSID> of GC is formatted as WMSID.<WMSName>.<RAWID>, with
the <RAWID> formatted as <poolURI>.<TaskID>.<ClusterID>.<ProcID>, e.g.
   WMSID.HTCondorPool.ssh://ekpcms5:80/opt/htc/.6fe1e996253f.2031.13

   o Files
Since the HTC backends operates on remote machines, it may use both
Sandbox and Staging folders. On local machines, the Staging folder is
a link copy. For convention with other backends, these correspond to:
  Sandbox: The folder where GC works on
  Staging: The folder where HTCondor works on
"""

#### ::TODO::
# Public Interfaces:
#    Auxiliary:
#      getSites
# Unhacks:
#    jobData dict-class

import os
import sys

import logging

from utils import makeEnum, ensureDirExists, ActivityLog
from grid_control import Job

import sys, os
import re
import glob
import popen2
import time

import tempfile
from grid_control import utils, QM, ProcessHandler, Job
from wms import WMS, BasicWMS, RethrowError

class HTCondorPool(BasicWMS):
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
		BasicWMS.__init__(self, config, wmsName, 'condor')
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
		rawDict = {}
		for poolConfigFileName in config.getList('poolConfig', onChange = None):
			confFile = open(poolConfigFileName, 'r')
			rawDict.update(eval(confFile.read()))
			confFile.close()
		self._jobFeatureMap = rawDict.get('jobFeatureMap',{})
		self._queueQueryMap = rawDict.get('queueQueryMap',{})
		self._niceName      = rawDict.get('NiceName', '<POOLNAME>')
		if config.get('ScheddURI',''):
			self._schedd = HTCScheddFactory(config.get('ScheddURI',''), parentPool=self)
		else:
			self._schedd = self._getDynamicSchedd(config)
			config.set('ScheddURI', self._schedd.getURI())
			self._log(logging.INFO1,'Using Schedd %s (none defined)'%(self._schedd.getURI()))
		self._log(logging.INFO1,'Connected to Schedd %s'%(self._schedd.getURI()))

	def _getDynamicSchedd(self, config):
		"""
		Pick a schedd based on best guess
		"""
		candidateURIList = []
		candidateURIList.extend(config.getList('ScheddURIs',[]))
		candidateURIList.extend('localhost://')
		self._log(logging.INFO2,'Selecting Schedd from URI list: %s'%(','.join(candidateURIList)))
		for scheddCandidate in candidateURIList:
			try:
				candidate = HTCScheddFactory(config.get('ScheddURI',''), parentPool=self)
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
		cfgName = 'job_%s.var' % str(jobNum)
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
					self._getJobCfgPath(jobNum)
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
			for jobData in jobInfoMaps:
				yield (
					jobData[0],
					jobInfoMaps[jobData]['wmsID'],
					jobInfoMaps[jobData]
					)
		del(activity)

	def checkJobs(self, wmsJobIdList):
		activity   = utils.ActivityLog('Checking jobs...')
		for scheddURI, jobDataList in self._getJobDataMap(wmsJobIdList).items():
			if scheddURI != self._schedd.getURI():
				self._log(logging.DEFAULT_VERBOSITY,'Bug! Got jobs at Schedd %s, but servicing only Schedd %s'%(scheddURI, self._schedd.getURI()))
				if utils.getUserBool('Do you wish to ignore these jobs and continue?', False):
					continue
				else:
					sys.exit(1)
			rawjobInfoMaps = self._schedd.checkJobs(
				jobDataList,
				self._getQueryArgs()
				)
			# Yield (jobNum, wmsId, state, other data) per active jobs
			jobInfoMaps = self._digestQueueInfoMap(rawjobInfoMaps)
			for jobData in jobInfoMaps:
				yield (
					jobData[0],
					jobInfoMaps[jobData]['wmsID'],
					self._statusMap[jobInfoMaps[jobData]['state']],
					jobInfoMaps[jobData]
					)
		del(activity)

	def _getJobsOutput(self, wmsJobIdList):
		activity   = utils.ActivityLog('Fetching jobs...')
		for scheddURI, jobDataList in self._getJobDataMap(wmsJobIdList).items():
			if scheddURI != self._schedd.getURI():
				self._log(logging.INFO1,'Bug! Got jobs at Schedd %s, but servicing only Schedd %s'%(scheddURI, self._schedd.getURI()))
				continue
			returnedJobs = self._schedd.getJobsOutput(
				jobDataList
				)
			# Yield (jobNum, outputPath) per retrieved job
			for jobData in returnedJobs:
				yield jobData[0], self.getSandboxPath(jobData[0]
		del activity
	
	def cancelJobs(self, wmsJobIdList):
		activity   = utils.ActivityLog('Canceling jobs...')
		for scheddURI, jobDataList in self._getJobDataMap(wmsJobIdList).items():
			if scheddURI != self._schedd.getURI():
				continue
			canceledJobs = self._schedd.cancelJobs(
				jobDataList
				)
			# Yield ( jobNum, wmsID) for canceled jobs
			for jobData in canceledJobs:
				yield (
					jobData[0],
					self._createGcId(scheddURI, jobData[1], jobData[2], jobData[3])
					)
		del(activity)

	# TODO: move me to schedd
	def _getBaseJDLData(self):
		"""Create the jdl"""
		jdlData = {}
		jdlData['Head'] = {
			'Executable'   : os.path.join('%(stageDir)s', '%(executable)s'),
			}
		jdlData['PerJob'] = {
			'+GcJobNum'       : '%(jobNum)s',
			}
		return jdlData

	# GC/WMS/Job ID converters
	def _createGcId(self, scheddURI, taskId, clusterID, processID):
		"""Create a GcId"""
		return self._createId('%s.%s.%s'%(scheddURI, taskId, clusterID, processID))
	def _splitGcId(self, gcId):
		"""Split a GcId, returning (wmsName, scheddURI,taskID,clusterID,processID)"""
		wmsName, wmsId = self._splitId(gcId)
		return (wmsName,) + tuple(wmsId.rsplit('.',3))
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
