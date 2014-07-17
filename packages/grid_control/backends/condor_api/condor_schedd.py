# -*- coding: utf-8 -*-

# core modules
import sys
import os

# standard modules
import logging
import re

# GC modules
import utils
import htcUtils
from htcondor_wms import HTCJobID

from python_compat import md5
from process_adapter import ProcessAdapterFactory

"""
This module provides adapter classes for uniformly issuing GC commands to HTCondor Schedds.
"""

def HTCScheddFactory(URI, **kwargs):
	"""
	Return an interface for the GC-Schedd operations
	
	Required:
	URI string
	       The URI of the Schedd to connect to
	"""
	adapter, scheme = ProcessAdapterFactory(URI, externalSchemes=["spool"])
	if not adapter:
		raise NotImplementedError("Schedd interfacing via methods of scheme '%s' has not been implemented yet." % scheme)
	for HTCSchedd in [ HTCScheddLocal, HTCScheddSSH ]:
		if adapter.getType() in HTCSchedd.adapterTypes:
			return HTCSchedd(URI = URI, adapter = adapter, **kwargs)

class HTCScheddBase(LoadableObject):
	"""
	Base Interface for interactions with a Schedd
	"""
	adapterTypes = []
	_submitScale = 0
	_adapterMaxWait   = 10
	def __init__(self, URI="", adapter = None, parentPool = None):
		"""
		Optional:
		URI string
		       URI from which to construct an adapter if none given
		adapter ProcessAdapterInterface
		       adapter to use for issuing schedd commands
		parentPool HTCondorWMS
		       pool WMS the schedd belongs to
		"""
		self._initLogger()
		self._log(logging.INFO1, "Establishing HTC Schedd adapter of type %s" % self.__class__.__name__)
		if adapter:
			self._adapter = adapter
		else:
			self._adapter, _ = ProcessAdapterFactory(URI, externalSchemes=["spool"])
		self._URI = URI or self._adapter.getURI()
		assert self._adapter != None, "Bug! Schedd initialization with invalid adapter data."
		assert adapter.getType() in self.adapterTypes, "Bug! Got adapter of type '%s', expected '%s'" % (adapter.getType(), "' or '".join(self.adapterType))
		self.parentPool = parentPool

	def getDomain(self):
		return self._adapter.getDomain()
	def getURI(self):
		return self._URI

	# public interfaces for HTC Pool/WMS
	def submitJobs(self, jobNumList, task, queryArguments):
		"""
		Submit a batch of jobs from the sandbox
		
		Returns:
		JobInfoMaps  { HTCJobID : InfoData,...]
		       Sequence of per job information
		"""
		raise AbstractError

	def checkJobs(self, htcIDs, queryArguments):
		"""
		Get the status of a number of jobs
		
		Rquired:
		htcIDs [HTCJobID, ...]
		
		Returns:
		JobInfoMapMaps  { HTCJobID : InfoData,...]
		       Sequence of per checked job information maps
		"""
		raise AbstractError

	def getJobsOutput(self, htcIDs):
		"""
		Return output of a finished job to the sandbox
		
		Rquired:
		htcIDs [HTCJobID, ...]
		
		Returns:
		ReturnedJobs  [HTCJobID,...]
		       Sequence of retrieved jobs
		"""
		raise AbstractError

	def cancelJobs(self, htcIDs):
		"""
		Cancel/Abort/Delete a number of jobs
		
		Rquired:
		htcIDs [HTCJobID, ...]
		
		Returns:
		ReturnedJobs  [HTCJobID,...]
		       Sequence of removed jobs"""
		raise AbstractError

	def getTimings(self):
		"""Return suggested Idle/Active polling interval"""
		return (60,10)

	def getCanSubmit(self):
		"""Return whether submission to this Schedd is possible"""
		return False
	
	def getSubmitScale(self):
		"""Return number of jobs to submit as one batch"""
		return self._submitScale

	def getHTCVersion(self):
		"""Return the version of the attached HTC installation as tuple(X,Y,Z)"""
		raise AbstractError

	# internal interfaces for HTC Schedds
	def getStagingDir(self, htcID = None, taskID = None):
		"""Return path in the Schedd domain where HTC picks up and returns files"""
		raise AbstractError
	def cleanStagingDir(self, htcID = None, taskID = None):
		"""Clean path in the Schedd domain where HTC picks up and returns files"""
		raise AbstractError

	def _getBaseJDLData(self, task, queryArguments):
		"""Create a sequence of attribute for a submission JDL"""
		jdlData = [
			'+submitTool             = "GridControl (version %s)"' % utils.getVersion(),
			'should_transfer_files   = YES',
			'when_to_transfer_output = ON_EXIT',
			'periodic_remove         = (JobStatus == 5 && HoldReasonCode != 16)',
			'environment             = CONDOR_WMS_DASHID=https://%s:/$(Cluster).$(Process)' % self.wmsName,
			'Universe                = %s' % self.parentPool._jobSettings["Universe"],	# TODO: Unhack me
			'+GcID                   = %s' % self.parentPool._createGcId(HTCJobID('$(GcJobNum)', self.URI, task.taskID, '$(Cluster)', '$(Process)', typed = False)),
			'+GcJobNumToWmsID        = $(GcJobNum)@$(Cluster).$(Process)',
			'+GcJobNumToGcID         = $(GcJobNum)@$(GcID)',
			'arguments               = $(GcJobNum)',
			'Log                     = "%s/gcJobs.log"' % self.getStagingDir(),
			]
		for key in queryArguments:
			try:
				# is this a match string? '+JOB_GLIDEIN_Entry_Name = "$$(GLIDEIN_Entry_Name:Unknown)"' -> MATCH_GLIDEIN_Entry_Name = "CMS_T2_DE_RWTH_grid-ce2" && MATCH_EXP_JOB_GLIDEIN_Entry_Name = "CMS_T2_DE_RWTH_grid-ce2"
				matchKey=re.match("(?:MATCH_EXP_JOB_|MATCH_|JOB_)(.*)",key).group(1)
				jdlData['Head']['+JOB_%s'%matchKey] = "$$(%s:Unknown)"%matchKey
			except AttributeError:
				pass
		for line in self.parentPool._jobSettings["ClassAd"]:
			jdlData.append( '+' + line )
		for line in self.parentPool._jobSettings["JDL"]:
			jdlData.append( line )
		return jdlData

	# GC internals
	@classmethod
	def _initLogger(self):
		self._logger = logging.getLogger('backend.htcschedd.%s' % self.__class__.__name__)
		self._log = self._logger.log


# Schedd interfaced via python
def HTCScheddPyBase(HTCScheddBase):
	def __init__(self, **kwArgs):
		raise NotImplementedError

# Schedd interfaced via CLI
def HTCScheddCLIBase(HTCScheddBase):
	# public interfaces for HTC Pool/WMS
	def submitJobs(self, jobNumList, task, queryArguments):
		jdlFilePath = self._prepareSubmit(task, jobNumList, queryArguments)
		submitProc = self._condor_submit(jdlFilePath)
		if submitProc.wait(timeout = self._adapterMaxWait):
			submitProc.logError(self.parentPool.errorLog, brief=True)
			return []
		queryInfoMaps = htcUtils.parseKWListIter(submitProc.getOutput())
		return self._digestQueryInfoMap(queryInfoMaps, queryArguments)

	def checkJobs(self, htcIDs, queryArguments):
		queryProc = self._condor_q(self, htcIDs, queryAttributes = queryArguments)
		if queryProc.wait(timeout = self._adapterMaxWait):
			queryProc.logError(self.parentPool.errorLog, brief=True)
			return []
		queryInfoMaps = htcUtils.parseKWListIter(queryProc.getOutput())
		return self._digestQueryInfoMap(queryInfoMaps, queryArguments)

	def _digestQueryInfoMap(self, queryInfoMaps, queryArguments):
		"""Digest raw queryInfoMaps to maps of HTCjobID : infoMap"""
		dataMap  = {}
		for infoMap in queryInfoMaps:
			htcID = self.parentPool.splitGcId(infoMap['GcID'])[1]
			dataMap[htcID] = {}
			for key in infoMap:
				if key in queryArguments:
					dataMap[htcID][key] = infoMap[key]
		return dataMap

	def cancelJobs(self, htcIDs):
		rmProc = self._condor_rm(self, htcIDs)
		if rmProc.wait(timeout = self._adapterMaxWait):
			rmProc.logError(self.parentPool.errorLog, brief=True)
			return []
		# Parse raw output of type "Job <ClusterID>.<ProcID> marked for removal"
		rmList = []
		for line in rmProc.getOutput():
			try:
				clusterID, procID = re.match('Job (\d*\.\d*)', line).groups()
				rmList.append((int(clusterID), int(procID)))
			except AttributeError:
				if line:
					self._log(logging.INFO3, "Failed to parse condor_rm output '%s'" % line)
		rmIDList = []
		for htcID in htcIDs:
			try:
				rmList.remove((htcID.clusterID,htcID.procID))
				rmIDList.append(htcID)
			except ValueError:
				pass
		return rmIDList

	@singleQueryCache(defReturnItem = (0,0,0))
	def getHTCVersion(self):
		"""Return the version of the attached HTC installation as tuple(X,Y,Z)"""
		raise AbstractError
		verProc = self._adapter.LoggedExecute("condor_version")
		if verProc.wait(timeout = self._adapterMaxWait):
			subProc.logError(self.parentPool.errorLog, brief=True)
		for line in verProc.getOutput():
			try:
				return re.match("$CondorVersion:*?(\d)\.(\d)\.(\d)").groups()
			except AttributeError:
				continue
		return None

	def _prepareSubmit(self, task, jobNumList):
		raise AbstractError

	def _condor_submit(self, jdlFilePath):
		subProc = self._adapter.LoggedExecute(
			"condor_submit",
			"%s" % (
				jdlFilePath
				)
			)
		return subProc

	def _condor_q(self, htcIDs, queryAttributes = []):
		qqProc = self._adapter.LoggedExecute(
			"condor_q",
			"%s -userlog '%s' -attributes '%s' -long" % (
				' '.join([ '%d.%d'%(htcID.clusterID, htcID.procID) for htcID in htcIDs ]),
				os.path.join(self.getStagingDir(htcIDs[0]),'gcJobs.log'),
				','.join(queryAttributes)
				)
			)
		return qqProc

	def _condor_history(self, htcIDs):
		raise AbstractError

	def _condor_rm(self, htcIDs):
		rmProc = self._adapter.LoggedExecute(
			"condor_rm",
			"%s" % (
				' '.join([ '%d.%d'%(htcID.clusterID, htcID.procID) for htcID in htcIDs ])
				)
			)
		return rmProc

	def _condor_transfer_data(self, jdlFilePath):
		raise AbstractError

# Schedd on the same host
class HTCScheddLocal(HTCScheddCLIBase):
	adapterTypes = ["local"]
	_submitScale = 10
	_adapterMaxWait   = 30

	def getTimings(self):
		return (20,5)

	def getJobsOutput(self, htcIDs):
		return htcIDs

	def _stageTaskFiles(self, jobNumList, task):
		return jobNumList

	def _prepareSubmit(self, task, jobNumList, queryArguments):
		jdlFilePath = os.path.join(self.parentPool.getSandboxPath(), 'htc-%s.schedd-%s.jdl' % (self.parentPool.wmsName,md5(self.getURI).hexdigest()))
		utils.safeWrite(
			os.open(jdlFilePath, 'w'),
			self._getJDLData(task, jobNumList, queryArguments)
			)
		return jdlFilePath

	def _getJDLData(self, task, jobNumList, queryArguments):
		jdlData = HTCScheddCLIBase._getBaseJDLData(self, task, jobNumList, queryArguments)
		jdlData.extend([
			'Executable              = "%s"' % self.parentPool._getSandboxFilesIn(task)[0][1],
			])
		if self.pool.getProxy().getAuthFile():
			jdlData.extend([
			'use_x509userproxy       = True',
			'x509userproxy           = "%s"' % self.pool.getProxy().getAuthFile(),
			])
		for jobNum in jobNumList:
			jobStageDir = self.getStagingDir(htcID = HTCJobID(jobNum, task.taskID, 0, 0))
			jdlData.extend([
			'initialdir              = "%s"' % jobStageDir,
			'Output                  = "%s/gs.stdout"' % jobStageDir,
			'Error                   = "%s/gs.stderr"' % jobStageDir,
			# HACK: ignore executable (In[0]) stdout (Out[0]) and stderr (Out[1])
			'transfer_input_files    = %s' % '","'.join(
				[ src for descr, src, trg in self.parentPool._getSandboxFilesIn(task)[1:]]
				+
				[ self.parentPool.getJobCfgPath(jobNum)[0] ]
				),
			'transfer_output_files   = "%s"' % ",".join(
				[ src for descr, src, trg in self._getSandboxFilesOut(task)[2:] ]
				),
			])
		return jdlData

	def getStagingDir(self, htcID = None, taskID = None):
		try:
			return self.parentPool.getSandboxPath(htcID.jobNum)
		except TypeError:
			return self.parentPool.getSandboxPath()
	def cleanStagingDir(self, htcID = None, taskID = None):
		pass

# Remote schedd interfaced via local HTC
class HTCScheddSpool(HTCScheddLocal):
	adapterTypes = ["local"]
	_submitScale = 10
	_adapterMaxWait   = 30
	def getTimings(self):
		return (30,5)

	def getJobsOutput(self, htcIDs):
		self._condor_transfer_data(htcIDs)
		if submitProc.wait(timeout = self._adapterMaxWait):
			submitProc.logError(self.parentPool.errorLog, brief=True)
			return []
		return HTCScheddLocal.getJobsOutput(self, htcIDs)

	def getScheddName(self):
		return self.getURI().split('spool://')[1]

	def _condor_submit(self, jdlFilePath):
		subProc = self._adapter.LoggedExecute(
			"condor_submit",
			"%s -name '%s'" % (
				jdlFilePath,
				self.getScheddName(),
				)
			)
		return subProc

	def _condor_q(self, htcIDs, queryAttributes = []):
		qqProc = self._adapter.LoggedExecute(
			"condor_q",
			"%s -userlog '%s' -attributes '%s' -long  -name '%s'" % (
				' '.join([ '%d.%d'%(htcID.clusterID, htcID.procID) for htcID in htcIDs ]),
				os.path.join(self.getStagingDir(taskID = htcIDs[0].gctaskID),'gcJobs.log'),
				','.join(queryAttributes),
				self.getScheddName(),
				)
			)
		return qqProc

	def _condor_rm(self, jobDataList):
		rmProc = self._adapter.LoggedExecute(
			"condor_rm",
			"%s -name '%s'" % (
				' '.join([ '%d.%d' % (htcID.clusterID, htcID.procID) for htcID in htcIDs ]),
				self.getScheddName(),
				)
			)
		return rmProc

	def _condor_transfer_data(self, jobDataList):
		trdProc = self._adapter.LoggedExecute(
			"condor_transfer_data",
			"%s -name '%s'" % (
				' '.join([ '%d.%d'%(htcID.clusterID, htcID.procID) for htcID in htcIDs ]),
				self.getScheddName(),
				)
			)
		return trdProc


# Remote schedd interfaced via ssh
class HTCScheddSSH(HTCScheddCLIBase):
	adapterTypes        = ["ssh","gsissh"]
	_submitScale        = 20
	_adapterMaxWait     = 30
	def __init__(self, URI="", adapter = None, parentPool = None):
		HTCScheddCLIBase.__init__(self, URI="", adapter = None, parentPool = None)
		self._stageDirCache = {}

	def getTimings(self):
		return (60,10)

	def getJobsOutput(self, htcIDs):
		retrievedJobs = []
		for index, htcID in enumerate(htcIDs):
			self._log(logging.DEBUG3, "Retrieving job files (%d/%d): %s" %( index, len(htcIDs), jobData[0]) )
			getProcess = self._adapter.LoggedGet(self.getStagingDir(htcID), self.parentPool.getSandboxPath(htcID.jobNum))
			if getProcess.wait(timeout = self._adapterMaxWait):
				getProcess.logError(self.parentPool.errorLog, brief=True)
				self._log(logging.INFO1, "Retrieval failed for job %d." %(jobData[0]) )
			else:
				retrievedJobs.append(htcID)
			try:
				self.cleanStagingDir(htcID = htcID)
			except RuntimeError as err:
				self._log( logging.DEFAULT_VERBOSITY, err.message )
		# clean up task dir if no job(dir)s remain
		try:
			statProcess = self._adapter.LoggedExecute('find %s -maxdepth 1 -type d | wc -l' % self.getStagingDir( taskID = htcIDs[0].gctaskID) )
			if statProcess.wait(timeout = self._adapterMaxWait):
				statProcess.logError(self.parentPool.errorLog, brief=True)
				raise RuntimeError('Failed to check remote dir for cleanup : %s @ %s' % (self.getStagingDir( taskID = htcIDs[0].gctaskID) ))
			elif (int(checkProcess.getOutput()) == 1):
				self.cleanStagingDir(taskID = htcIDs[0].gctaskID)
		except RuntimeError as err:
			self._log( logging.DEFAULT_VERBOSITY, err.message )
		return retrievedJobs

	def _prepareSubmit(self, task, jobNumList, queryArguments):
		localJdlFilePath = os.path.join(self.parentPool.getSandboxPath(), 'htc-%s.schedd-%s.jdl' % (self.parentPool.wmsName,md5(self.getURI).hexdigest()))
		readyJobNumList  = self._stageSubmitFiles(task, jobNumList)
		utils.safeWrite(
			os.open(localJdlFilePath, 'w'),
			self._getJDLData(task, jobNumList, queryArguments)
			)
		return jdlFilePath

	def _getJDLData(self, task, jobNumList, queryArguments):
		taskFiles, proxyFile, jobFileMap = self._getSubmitFileMap(task, jobNumList)
		jdlData = HTCScheddCLIBase._getBaseJDLData(self, task, jobNumList, queryArguments)
		jdlData.extend([
			'Executable              = "%s"' % taskFiles[0][2],
			])
		if proxyFile:
			jdlData.extend([
			'use_x509userproxy       = True',
			'x509userproxy           = "%s"' % proxyFile[2],
			])
		for jobNum in jobNumList:
			jobStageDir = self.getStagingDir(jobData = (jobNum, task.taskID, 0, 0))
			jdlData.extend([
			'initialdir              = "%s"' % jobStageDir,
			'Output                  = "%s/gs.stdout"' % jobStageDir,
			'Error                   = "%s/gs.stderr"' % jobStageDir,
			# HACK: ignore executable (In[0]) stdout (Out[0]) and stderr (Out[1])
			'transfer_input_files    = %s' % '","'.join(
				[ schd for descr, gc, schd in taskFiles[1:] + jobFileMap[jobNum] ]
				),
			'transfer_output_files   = "%s"' % ",".join(
				[ src for descr, src, trg in self._getSandboxFilesOut(task)[2:] ]
				),
			])
		return jdlData

	# internal interfaces for HTC Pool/Schedds
	def _getSubmitFileMap(self, task, jobNumList):
		"""
		Get listed files for submission
		
		Returns:
		taskFiles           iterable as (descr, gcPath, scheddPath)
		       files shared by all jobs
		jobsFileMap         map of jobNum to iterable as (descr, gcPath, scheddPath)
		       files per individual job
		"""
		taskFiles = []
		taskFiles.extend(
			map(
				lambda (desrc, path, base): (descr, path, os.path.join(self.getStagingDir(taskID = task.taskID), base) ),
				self.parentPool._getSandboxFilesIn(task)
				)
			)
		proxyFile = ()
		if self.pool.getProxy():
			('User Proxy', self.pool.getProxy().getAuthFile(), os.path.join(self.getStagingDir(taskID = task.taskID), os.path.basename(self.pool.getProxy().getAuthFile())))
		jobFileMap = {}
		for jobNum in jobNumList:
			jcFull, jcBase = self.getJobCfgPath(jobNum)
			jobsFileMap[jobNum] = ('Job Config %d' % jobNum, jcFull, os.path.join(self.getStagingDir(taskID = task.taskID), jcBase))
		return taskFiles, proxyFile, jobFileMap

	def _stageSubmitFiles(self, task, jobNumList):
		"""
		Stage submission files at scheduler.
		"""
		taskFiles, proxyFile, jobFileMap = self._getSubmitFileMap(task, jobNumList)
		self._log(logging.DEBUG1, "Staging task files.")
		stagedJobs = []
		if proxyFile:
			taskFiles.append(proxyFile)
		for index, fileInfoBlob in enumerate(taskFiles):
			self._log(logging.DEBUG3, "Staging task files (%d/%d): %s" %( index, len(taskFiles), fileInfoBlob[0]) )
			putProcess = self._adapter.LoggedPut(fileInfoBlob[1], fileInfoBlob[2])
			if putProcess.wait(timeout = self._adapterMaxWait):
				putProcess.logError(self.parentPool.errorLog, brief=True)
				self._log(logging.INFO1, "Staging failure. Aborting submit." %(fileInfoBlob[0]) )
				return stagedJobs
		for jobNum, jobFiles in jobFileMap:
			try:
				for fileInfoBlob in jobFiles:
					self._log(logging.DEBUG3, "Staging job files: %s" %(fileInfoBlob[0]) )
					putProcess = self._adapter.LoggedPut(fileInfoBlob[1], fileInfoBlob[2])
					if putProcess.wait(timeout = self._adapterMaxWait):
						putProcess.logError(self.parentPool.errorLog, brief=True)
						try:
							self.cleanStagingDir( htcID = HTCJobID(jobNum, task.taskID))
						except RuntimeError as err:
							self._log( logging.INFO1, err.message )
						raise RuntimeError
			except RuntimeError:
				continue
			else:
				stagedJobs.append(jobNum)
		return stagedJobs

	def _getStagingToken(self, htcID = None, taskID = None):
		"""Construct the key for a staging directory"""
		try:
			return 'taskID.%s/job_%s' % ( htcID.gctaskID, htcID.jobNum )
		except TypeError:
			if taskID:
				return 'taskID.%s' % taskID
		return ''
	_getStagingDirToken = lru_cache(_getStagingDirToken, 31)
	def getStagingDir(self, htcID = None, taskID = None):
		token = self._getStagingToken(htcID = htcID, taskID = taskID)
		try:
			return self._stageDirCache[token]
		except KeyError:
			stageDirBase = os.path.join('GC.work', token, '')
		stageDirPath = self._adapter.getDomainAbsPath(stageDirBase)
		# -m 744 -> rwxr--r--
		mkdirProcess = self._adapter.LoggedExecute("mkdir -m 744 -p", stageDirPath )
		if mkdirProcess.wait(timeout = self._adapterMaxWait):
			mkdirProcess.logError(self.parentPool.errorLog, brief=True)
			raise RuntimeError('Failed to create remote dir : %s @ %s' % (stageDirPath, self.getDomain()))
		self._stageDirCache[token] = stageDirPath
		return stageDirPath

	def cleanStagingDir(self, htcID = None, taskID = None):
		token        = self._getStagingToken(htcID = htcID, taskID = taskID)
		try:
			stageDirPath = self.getStagingDir(htcID = htcID, taskID = taskID)
		except RuntimeError:
			return
		rmdirProcess = self._adapter.LoggedExecute("rm -rf", stageDirPath )
		if rmdirProcess.wait(timeout = self._adapterMaxWait):
			rmdirProcess.logError(self.parentPool.errorLog, brief=True)
			raise RuntimeError('Failed to clean remote dir : %s @ %s' % (stageDirPath, self.getDomain()))
		del(self._stageDirCache[token])
