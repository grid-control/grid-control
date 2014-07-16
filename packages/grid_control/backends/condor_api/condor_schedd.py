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
	for HTCSchedd in [ HTCScheddLocal, HTCScheddSSH ]
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
		JobInfoMaps  { jobData : InfoData,...]
		       Sequence of per job information
		"""
		raise AbstractError

	def checkJobs(self, jobDataList, queryArguments):
		"""
		Get the status of a number of jobs
		
		Rquired:
		jobDataList [jobData, ...]
		
		Returns:
		JobInfoMapMaps  { jobData : InfoData,...]
		       Sequence of per checked job information maps
		"""
		raise AbstractError

	def getJobsOutput(self, jobDataList):
		"""
		Return output of a finished job to the sandbox
		
		Rquired:
		jobDataList [jobData, ...]
		
		Returns:
		ReturnedJobs  [jobData,...]
		       Sequence of retrieved jobs
		"""
		raise AbstractError

	def cancelJobs(self, jobDataList):
		"""
		Cancel/Abort/Delete a number of jobs
		
		Rquired:
		jobDataList [jobData, ...]
		
		Returns:
		ReturnedJobs  [jobData,...]
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
	def getStagingDir(self, jobData = None, taskID = None):
		"""Return path in the Schedd domain where HTC picks up and returns files"""
		raise AbstractError
	def cleanStagingDir(self, jobData = None, taskID = None):
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
			'Universe                = %s' % self.parentPool._jobSettings["Universe"]	# TODO: Unhack me
			'+GcID                   = %s' % self._createGcId(self.URI, task.taskID, '$(Cluster)', '$(Process)'),
			'+GcJobNumToWmsID        = $(GcJobNum)@$(Cluster).$(Process)',
			'+GcJobNumToGcID         = $(GcJobNum)@%s' % self._createGcId(self.URI, task.taskID, '$(Cluster)', '$(Process)'),
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
			return = []
		infoMaps = htcUtils.parseKWListIter(submitProc.getOutput())
		return self._digestQueryInfoMap(infoMaps, queryArguments)

	def checkJobs(self, jobDataList, queryArguments):
		queryProc = self._condor_q(self, jobDataList, queryAttributes = queryArguments)
		if queryProc.wait(timeout = self._adapterMaxWait):
			queryProc.logError(self.parentPool.errorLog, brief=True)
			return []
		infoMaps = htcUtils.parseKWListIter(queryProc.getOutput())
		return self._digestQueryInfoMap(infoMaps, queryArguments)

	def _digestQueryInfoMap(self, infoMaps, queryArguments):
		dataMap  = {}
		for infoMap in infoMaps:
			gcID = self.parentPool.splitGcId(infoMap['GcID'])
			dataMap[gcID] = {}
			for key in infoMap:
				if key in queryArguments:
					dataMap[gcID][key] = infoMap[key]
		return infoMaps

	def cancelJobs(self, jobDataList):
		rmProc = self._condor_rm(self, jobDataList)
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
		rmDataList = []
		for jobData in jobDataList:
			try:
				rmList.remove((jobData[2],jobData[3]))
				rmDataList.append(jobData)
			except ValueError:
				pass
		return rmDataList

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

	def _condor_q(self, jobDataList, queryAttributes = []):
		qqProc = self._adapter.LoggedExecute(
			"condor_q",
			"%s -userlog '%s' -attributes '%s' -long" % (
				' '.join([ '%d.%d'%(obj[2], obj[3]) for obj in jobDataList ]),
				os.path.join(self.getStagingDir(jobDataList[0]),'gcJobs.log'),
				','.join(queryAttributes)
				)
			)
		return qqProc

	def _condor_history(self, jobDataList):
		raise AbstractError

	def _condor_rm(self, jobDataList):
		rmProc = self._adapter.LoggedExecute(
			"condor_rm",
			"%s" % (
				' '.join([ '%d.%d'%(obj[2], obj[3]) for obj in jobDataList ])
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

	def getJobsOutput(self, jobDataList):
		return jobDataList

	def _stageTaskFiles(self, jobNumList, tasks):
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
			)]
		for jobNum in jobNumList:
			jobStageDir = self.getStagingDir(jobData = (jobNum, task.taskID, 0, 0))
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
				[ src for descr, src, trg in self._getSandboxFilesOut(task)[2:] ) ]
				),
			)]
		return jdlData

	def getStagingDir(self, jobData = None, taskID = None):
		try:
			return self.parentPool.getSandboxPath(jobData[0])
		except TypeError:
			return self.parentPool.getSandboxPath()
	def cleanStagingDir(self, jobData = None, taskID = None):
		pass

# Remote schedd interfaced via local HTC
class HTCScheddSpool(HTCScheddLocal):
	adapterTypes = ["local"]
	_submitScale = 10
	_adapterMaxWait   = 30
	def getTimings(self):
		return (30,5)

	def getJobsOutput(self, jobDataList):
		self._condor_transfer_data(job1DataList)
		if submitProc.wait(timeout = self._adapterMaxWait):
			submitProc.logError(self.parentPool.errorLog, brief=True)
			return = []
		return HTCScheddLocal.getJobsOutput(self, jobDataList)

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

	def _condor_q(self, jobDataList, queryAttributes = []):
		qqProc = self._adapter.LoggedExecute(
			"condor_q",
			"%s -userlog '%s' -attributes '%s' -long  -name '%s'" % (
				' '.join([ '%d.%d'%(obj[2], obj[3]) for obj in jobDataList ]),
				os.path.join(self.getStagingDir(jobDataList[0]),'gcJobs.log'),
				','.join(queryAttributes),
				self.getScheddName(),
				)
			)
		return qqProc

	def _condor_rm(self, jobDataList):
		rmProc = self._adapter.LoggedExecute(
			"condor_rm",
			"%s -name '%s'" % (
				' '.join([ '%d.%d'%(obj[2], obj[3]) for obj in jobDataList ]),
				self.getScheddName(),
				)
			)
		return rmProc

	def _condor_transfer_data(self, jobDataList):
		trdProc = self._adapter.LoggedExecute(
			"condor_transfer_data",
			"%s -name '%s'" % (
				' '.join([ '%d.%d'%(obj[2], obj[3]) for obj in jobDataList ]),
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

	def getJobsOutput(self, jobDataList):
		retrievedJobs = []
		for index, jobData in enumerate(jobDataList):
			self._log(logging.DEBUG3, "Retrieving job files (%d/%d): %s" %( index, len(jobDataList), jobData[0]) )
			getProcess = self._adapter.LoggedGet(self.getStagingDir(jobData), fileInfoBlob[2])
			if putProcess.wait(timeout = self._adapterMaxWait):
				putProcess.logError(self.parentPool.errorLog, brief=True)
				self._log(logging.INFO1, "Retrieval failed for job %d." %(jobData[0]) )
			else:
				retrievedJobs.append(jobDataList)
			try:
				self.cleanStagingDir(jobData = jobData)
			except RuntimeError as err:
				self._log( logging.DEFAULT_VERBOSITY, err.message )
		# clean up task dir if no job(dir)s remain
		try:
			statProcess = self._adapter.LoggedExecute('find %s -maxdepth 1 -type d | wc -l' % self.getStagingDir(jobData = ('*', jobDataList[0][1],0,0)))
			if statProcess.wait(timeout = self._adapterMaxWait):
				statProcess.logError(self.parentPool.errorLog, brief=True)
				raise RuntimeError('Failed to check remote dir for cleanup : %s @ %s' % (self.cleanStagingDir((taskID = jobDataList[0][1]) ), self.getDomain()))
			elif (int(checkProcess.getOutput()) <= 1):
				self.cleanStagingDir((taskID = jobDataList[0][1]) )
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
		if self.parentPool.getProxy().getAuthFile():
			jdlData.extend([
			'use_x509userproxy       = True',
			'x509userproxy           = "%s"' % proxyFile[2],
			)]
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
				[ src for descr, src, trg in self._getSandboxFilesOut(task)[2:] ) ]
				),
			)]
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
				lambda (desrc, path, base): (descr, path, os.path.join(self.getStagingDir(taskID = task.taskID), base) )
				self.pool._getSandboxFilesIn(task)
				)
			)
		proxyFile = self.pool.getProxy().getAuthFile()
		if proxyFile:
			proxyFiles = 
			('User Proxy', proxyFile, os.path.join(self.getStagingDir(taskID = task.taskID), os.path.basename(proxyFile)))
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
		for index, fileInfoBlob in enumerate(taskFiles + [proxyFile]):
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
							self.cleanStagingDir(jobData = (jobNum, task.taskID))
						except RuntimeError as err:
							self._log( logging.INFO1, err.message )
						raise RuntimeError
			except RuntimeError:
				continue
			else:
				stagedJobs.append(jobNum)
		return stagedJobs

	def _getStagingToken(self, jobData = None, taskID = None):
		"""Construct the key for a staging directory"""
		try:
			return 'taskID.%s.job_%s' % ( str(jobData[1]), str(jobData[0]) )
		except TypeError:
			if taskID:
				return 'taskID.%s' % taskID
		return ''
	_getStagingDirToken = lru_cache(_getStagingDirToken, 31)
	def getStagingDir(self, jobData = None, taskID = None):
		token = self._getStagingToken(jobData = None, taskID = None)
		try:
			return self._stageDirCache[token]
		except KeyError:
			stageDirBase = os.path.join('GC.work', token, '')
		stageDirPath = self._adapter.getDomainAbsPath(stageDirBase)
		# -m 744 -> rwxr--r--
		mkdirProcess = self._adapter.LoggedExecute("mkdir -m 744 -p", stageDirPath )
		if mkdirProcess.wait(timeout = self._adapterMaxWait):
			mkdirProcess.logError(self.parentPool.errorLog, brief=True)
			raise RuntimeError
		self._stageDirCache[token] = stageDirPath
		return stageDirPath

	def cleanStagingDir(self, jobData = None, taskID = None):
		token = self._getStagingToken(jobData = None, taskID = None)
		stageDirPath = self.getStagingDir(jobData = None, taskID = None)
		rmdirProcess = self._adapter.LoggedExecute("rm -rf", stageDirPath )
		if rmdirProcess.wait(timeout = self._adapterMaxWait):
			rmdirProcess.logError(self.parentPool.errorLog, brief=True)
			raise RuntimeError('Failed to clean remote dir : %s @ %s' % (stageDirPath, self.getDomain()))
		del(self._stageDirCache[token])
