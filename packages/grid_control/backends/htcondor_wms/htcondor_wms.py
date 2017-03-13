# | Copyright 2014-2017 Karlsruhe Institute of Technology
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
from grid_control.backends.htcondor_wms.htcondor_schedd import HTCScheddFactory
from grid_control.backends.htcondor_wms.wmsid import HTCJobID
from grid_control.backends.wms import BackendError, BasicWMS
from grid_control.config import ConfigError
from grid_control.job_db import Job
from grid_control.utils import ensure_dir_exists
from grid_control.utils.activity import Activity
from python_compat import json, lchain, lfilter


"""
HTCondor backend core
 This module provides the backend for HTCondor pools of version 8+.

 The 'HTCondor' class provides a GC compatible WMS-backend and
handles interaction with all other components.

Conventions:
  o Job Identification/accounting
Internal components uniquely identify jobs via blobs of
  jobData = (int(jobnum), str(task_id), int(clusterID), int(processID))
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
	config_section_list = BasicWMS.config_section_list + ['htcondor']
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
			'jobnum'    : [ 'GcJobNum' ],
			'wms_id'     : [ 'GcID' ],
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
	def __init__(self, config, name):
		self._initLogger()
		BasicWMS.__init__(self, config, name)
		# setup the connection to pools and their interfaces
		self._sandboxDir  = config.get_path('sandbox path', config.get_work_path('sandbox.%s' % name), must_exist = False)
		self._initPoolInterfaces(config)
		self._jobSettings = {
			"Universe" : config.get("universe", "vanilla"),
			"ClassAd" : config.get_list("append info", []),
			"JDL" : config.get_list("append opts", []),
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
		for poolConfigFileName in config.get_list('poolConfig', [], on_change = None):
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
		cfgScheddURI = config.get('ScheddURI','', on_change = changeOnlyUnset)
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
	def get_interval_info(self):
		return self._schedd.get_interval_info()

	# path functions shared with schedds
	def getJobCfgPath(self, jobnum = "%d"):
		cfgName = 'job_%s.var' % jobnum
		return os.path.join(self.config.get_work_path('jobs'), cfgName), cfgName

	def getSandboxPath(self, subdirToken=""):
		sandpath = os.path.join(self._sandboxDir, str(subdirToken), '' )
		return ensure_dir_exists(sandpath, 'sandbox directory', BackendError)

	# Primary backend actions
	def submit_jobs(self, jobnum_list, task):
		requestLen = len(jobnum_list)
		activity = Activity('Submitting jobs (--%)')
		while jobnum_list:
			jobSubmitNumList = jobnum_list[-self._schedd.getSubmitScale():]
			del jobnum_list[-self._schedd.getSubmitScale():]
			activity = Activity('Submitting jobs (%2d%%)'%(100*(requestLen-len(jobnum_list))/requestLen))
			for jobnum in jobSubmitNumList:
				self._write_job_config(
					self.getJobCfgPath(jobnum)[0],
					jobnum,
					task, {}
					)
			rawJobInfoMaps = self._schedd.submit_jobs(
				jobSubmitNumList, 
				task,
				self._getQueryArgs()
				)
			# Yield (jobnum, gc_id, other data) per jobZ
			jobInfoMaps = self._digestQueueInfoMaps(rawJobInfoMaps)
			for htcID in jobInfoMaps:
				yield (
					htcID.gcJobNum,
					self._createGcId(htcID),
					jobInfoMaps[htcID]
					)
		activity.finish()

	def check_jobs(self, gc_id_jobnum_list):
		if not len(gc_id_jobnum_list):
			raise StopIteration
		activity   = Activity('Checking jobs')
		assert not bool(lfilter( lambda htcid: htcid.scheddURI != self._schedd.getURI(), self._splitGcRequests(gc_id_jobnum_list))), 'Bug! Got jobs at Schedds %s, but servicing only Schedd %s' % (lfilter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(gc_id_jobnum_list)), self._schedd.getURI())
		rawJobInfoMaps = self._schedd.check_jobs(
			self._splitGcRequests(gc_id_jobnum_list),
			self._getQueryArgs()
			)
		# Yield (jobnum, gc_id, state, other data) per active jobs
		jobInfoMaps = self._digestQueueInfoMaps(rawJobInfoMaps)
		for htcID in jobInfoMaps:
			yield (
				htcID.gcJobNum,
				self._createGcId(htcID),
				self._statusMap[jobInfoMaps[htcID]['state']][0],
				jobInfoMaps[htcID]
				)
		activity.finish()

	def _get_jobs_output(self, gc_id_jobnum_list):
		if not len(gc_id_jobnum_list):
			raise StopIteration
		activity   = Activity('Fetching jobs')
		assert not bool(lfilter( lambda htcid: htcid.scheddURI != self._schedd.getURI(), self._splitGcRequests(gc_id_jobnum_list))), 'Bug! Got jobs at Schedds %s, but servicing only Schedd %s' % (lfilter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(gc_id_jobnum_list)), self._schedd.getURI())
		returnedJobs = self._schedd.getJobsOutput(
			self._splitGcRequests(gc_id_jobnum_list)
			)
		# Yield (jobnum, path_output) per retrieved job
		for htcID in returnedJobs:
			yield (
				htcID.gcJobNum,
				self.getSandboxPath(htcID.gcJobNum)
				)
		activity.finish()
	
	def cancel_jobs(self, gc_id_jobnum_list):
		if not len(gc_id_jobnum_list):
			raise StopIteration
		activity   = Activity('Canceling jobs')
		assert not bool(lfilter( lambda htcid: htcid.scheddURI != self._schedd.getURI(), self._splitGcRequests(gc_id_jobnum_list))), 'Bug! Got jobs at Schedds %s, but servicing only Schedd %s' % (lfilter( lambda itr: itr.scheddURI != self._schedd.getURI(), self._splitGcRequests(gc_id_jobnum_list)), self._schedd.getURI())
		canceledJobs = self._schedd.cancel_jobs(
			self._splitGcRequests(gc_id_jobnum_list)
			)
		# Yield ( jobnum, wms_id) for canceled jobs
		for htcJobID in canceledJobs:
			yield (
				htcJobID.gcJobNum,
				self._createGcId(htcJobID)
				)
		activity.finish()

	# GC/WMS/Job ID converters
	def _createGcId(self, htcID):
		"""Create a GcId for a given htcID"""
		return self._create_gc_id(htcID.rawID)
	def _splitGcId(self, gcId):
		"""Split a GcId, returning wms_name and htcJobID"""
		wms_name, rawId = self._split_gc_id(gcId)
		return (wms_name, HTCJobID(rawID = rawId))
	def _splitGcRequests(self, jobnumGcIdList):
		"""Process sequence of (GcId, jobnum), returning sequence of htcIDs"""
		return [ self._splitGcId(gcId)[1] for gcId, jobnum in jobnumGcIdList ]
	def _getJobDataMap(self, jobnumGcIdList):
		"""Process list of (jobnum, GcId), returning {ScheddURI : [(jobnum, task_id, clusterID, processID),...] }"""
		scheddJobMap = {}
		for jobnum, GcId in jobnumGcIdList:
			_, scheddURI, task_id, clusterID, processID = _splitGcId(GcId)
			scheddJobMap.setdefault(scheddURI, []).append(tuple(jobnum, task_id, clusterID, processID))
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
		return lchain(qqM.values())
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
