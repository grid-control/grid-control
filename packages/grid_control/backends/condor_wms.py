# -*- coding: utf-8 -*-

import sys, os, time
import re
import commands, glob
#enable for debugging only! $python condor_wms.py
#sys.path.insert(1, os.path.join(sys.path[0], '../../../packages'))
from gcPackage import *

import tempfile
from grid_control import utils, QM
from wms import WMS, BasicWMS

# Important notes on this module
# Condor handles job IDs as (Cluster).(Process) which is loaded as float by GC by default, e.g. transforming 197.60 to 197.6
#	thus the condor ID is stored as "C(Cluster).(Process)" and stripped to the raw ID when interfacing with condor

#TODO: Consider support for different Condor architectures (static pool, GlideinWMS, Cloud, ...)
#TODO: Add option for loading custom Dicts for ClassAd/Frontend naming convections

class CondorWMS(BasicWMS):
	# dictionary mapping vanilla condor job status to GC job status
	# condor: U = unexpanded (never been run), H = on hold, R = running, I = idle (waiting for a machine to execute on), C = completed, and X = removed. 
	# 0 Unexpanded 	U -- 1	Idle 	I -- 2	Running 	R -- 3	Removed 	X -- 4	Completed 	C -- 5	Held 	H -- 6	Submission_err 	E
	# GC: 'INIT', 'SUBMITTED', 'DISABLED', 'READY', 'WAITING', 'QUEUED', 'ABORTED', 'RUNNING', 'CANCELLED', 'DONE', 'FAILED', 'SUCCESS'
	_statusMap = {		# dictionary mapping vanilla condor job status to GC job status
		'0' : Job.SUBMITTED	,# unexpanded (never been run)
		'5' : Job.WAITING	,# on hold
		'2' : Job.RUNNING	,# running
		'1' : Job.QUEUED	,# idle (waiting for a machine to execute on)
		'4' : Job.DONE	,# completed
		'3' : Job.CANCELLED	,# removed
		'6' : Job.FAILED	,# submit error
		}
	_humanMap = {		# dictionary mapping vanilla condor job status to human readable status
		'0' : 'Unexpanded'	,
		'1' : 'Idle',
		'2' : 'Running',
		'3' : 'Removed',
		'4' : 'Completed',
		'5' : 'Held',
		'6' : 'Submission_err'
		}

	# Implementation specific dictionaries:
	#TODO: Add option for loading custom Dicts for ClassAd/Frontend naming conventions
	# dictionary of Sites and lists of identifiers for each
	_siteMap = {
		"T1_DE_KIT" : ["GridKA"],
		"T1_US_FNAL" : [""],
		"T2_DE_RWTH" : ["Achen"],
		"T2_DE_DESY" : ["Desy"],
		}
	for key, value in _siteMap.items():
		_siteMap[key].append(key)
	# dictionary of ClassAd feature names
	#	Map custom Condor features to standardized GC features
	# how to translate an internal feature to a Condor ClassAd feature
	_condorPublishMap = {
		"blacklistSite" : "+REFUSED_Sites",
		"whitelistSite" : "+DESIRED_Sites",
		"walltimeMin" : "+DESIRED_Walltime",
		"storageElement" : "+DESIRED_SEs",
		}

	# output format request and matching dictionary keys
	# condor_q and condor_history will be querried using these
	statusReturnBlank="""'IfThenElse(isUndefined(ClusterId),"","")'""" # Dummy for producing empty format specifier
	statusReturnFormat= "-format '%d.' ClusterId -format '%d ' ProcId" + \
						" -format '%s ' GridControl_GCIDtoWMSID" + \
						" -format '%d ' JobStatus" + \
						""" -format '%s ' 'formatTime(QDate,"%m/%d-%H:%M")'""" + \
						""" -format '%s ' 'formatTime(CompletionDate,"%m/%d-%H:%M")'""" + \
						""" -format '%s ' 'IfThenElse(isUndefined(RemoteHost)==False,RemoteHost,IfThenElse(isUndefined(LastRemoteHost)==False,LastRemoteHost,"N/A"))'""" 
	# Glidein specific - should check pool type here
	statusReturnFormat+=""" -format '%s' 'IfThenElse(isUndefined(MATCH_EXP_JOB_GLIDEIN_Entry_Name)==False,MATCH_EXP_JOB_GLIDEIN_Entry_Name,"N/A")'"""
	statusReturnFormat+=" -format '%%s\n' %s" % statusReturnBlank
	statusReturnKeys = ["wmsid", "GCID@WMSID", "status", "submit_time", "completion_time", "RemoteHost"]
	# Glidein specific - should check pool type here
	statusReturnKeys +=["Queue"]
	
	print statusReturnKeys


# __init__: start Condor based job management
#>>config: Config class extended dictionary
#>>module: job input module (e.g. UserMod, CMSSW)
	def __init__(self, config, wmsName):
		utils.vprint('Using batch system: Condor/GlideInWMS', -1)
		BasicWMS.__init__(self, config, wmsName, 'grid')
		# make job data accessible for creating JDLs
		self.config = config
		# get executables for submitting, querrying and canceling a job for this framework
		self.submitExec = utils.resolveInstallPath('condor_submit')
		self.statusExec = utils.resolveInstallPath('condor_q')
		self.historyExec = utils.resolveInstallPath('condor_history')	# completed/failed jobs are stored outside the queue
		self.cancelExec = utils.resolveInstallPath('condor_rm')
		self.transferExec = utils.resolveInstallPath('condor_transfer_data')	# submission might spool to another schedd and need to fetch output
		self.versionExec = utils.resolveInstallPath('condor_version')

		self.remotePool = config.get( "Condor", "remotePool", None, mutable=True)
		self.requestArg=QM(self.remotePool, "-name %s" % self.remotePool, "")#self.remotePool and "-name %s" % self.remotePool or ""
		self.submitArg=QM(self.remotePool, "-remote %s" % self.remotePool, "")#self.remotePool and "-remote %s" % self.remotePool or ""
		# Sandboxe base path where individual job data is stored, staged and returned to
		self.sandPath = config.getPath('local', 'sandbox path', os.path.join(config.workDir, 'sandbox'), check=False)
		# history querry is faster with split files - check if and how this is used
		self.historyFile = None
		if not self.remotePool and commands.getoutput("condor_config_val ENABLE_HISTORY_ROTATION").lower() == "true":
			self.historyFile = commands.getoutput("condor_config_val HISTORY")
			if not os.path.isfile(self.historyFile):
				self.historyFile = None

		# special debug out/messages/annotations - may have noticeable effect on storage and performance!
		if config.get( "Condor", "debugLog", False, mutable=True):
			self.debug=open(config.get( "Condor", "debugLog", False, mutable=True),'a')
		else:
			self.debug=False


	def explainError(self, proc, code):
		if 'Keyboard interrupt raised by user' in proc.getError():
			return True
		return False

# overwrite for check/submit/fetch intervals
	def getTimings(self):
		return (10, 5)

# GetVersion: get a comparable representation of condor version from condor_version
#>> args: command line argument to condor_version
	def getVersion(arg=""):
		for line in popen2.popen3(str(self.versionExec) + arg)[0].readlines():
			if line.startswith('$CondorVersion: '):
				version=tuple([ int(ver) for ver in line.split()[1].split(".") ])
		return version

# getSandbox: return path to sandbox for this specific job
	def getSandboxPath(self, jobNum):
		sandpath = os.path.join(self.sandPath, str(jobNum) )
		if not os.path.exists(sandpath):
			try:
				os.makedirs(sandpath)
			except Exception:
				raise RethrowError('Error accessing or creating sandbox directory:\n	%s' % sandpath)
		return sandpath

# getJobsOutput: retrieve task output files from sandbox directory
#>>wmsJobIdList: list of (wmsID, JobNum) tuples
	def _getJobsOutput(self, wmsJobIdList):
		if not len(wmsJobIdList):
			raise StopIteration

		wmsJobIdList=[(elem[0][1:],elem[1]) for elem in wmsJobIdList]

		activity = utils.ActivityLog('retrieving job outputs')
		for wmsId, jobNum in wmsJobIdList:
			sandpath = self.getSandboxPath(jobNum)
			if sandpath == None:
				yield (jobNum, None)
				continue

			# when working with a remote schedd, actively trigger file return
			if self.remotePool:
				statusProcess = utils.LoggedProcess(self.transferExec, '%(Remote)s %(jobID)s' % {"jobID" : wmsId, "Remote": self.requestArg })
				retCode = statusProcess.wait()
				if retCode != 0:
					if self.explainError(statusProcess, retCode):
						pass
					else:
						statusProcess.logError(self.errorLog)

			# Cleanup sandbox
			#outFiles = utils.listMapReduce(lambda pat: glob.glob(os.path.join(sandpath, pat)), self.sandboxOut)
			#utils.removeFiles(filter(lambda x: x not in outFiles, map(lambda fn: os.path.join(sandpath, fn), os.listdir(sandpath))))

			yield (jobNum, sandpath)
		del activity

# cancelJobs: remove jobs from queue and yield (wmsID, jobNum) of cancelled jobs
#>>wmsJobIdList: list of (wmsID, JobNum) tuples
	def cancelJobs(self, wmsJobIdList):
		if len(wmsJobIdList) == 0:
			raise StopIteration

		wmsJobIdList=[(elem[0][1:],elem[1]) for elem in wmsJobIdList]
		wmsToJobMap = dict(wmsJobIdList)
		wmsIdList = set(zip(*wmsJobIdList)[0])
		wmsIdArgument = " ".join(wmsIdList)
		#wmsIdArgument = " ".join(str(wmsID) for wmsID in zip(*wmsJobIdList)[0])

		activity = utils.ActivityLog('cancelling jobs')
		cancelProcess = utils.LoggedProcess(self.cancelExec, '%(Remote)s %(jobIDs)s' % {"jobIDs" : wmsIdArgument, "Remote": self.requestArg })

		# check if canceling actually worked
		for cancelReturnLine in cancelProcess.iter():
			if ( cancelReturnLine!= '\n' ) and ( 'marked for removal' in cancelReturnLine ):
				try:
					wmsID=cancelReturnLine.split()[1]
					#if ( wmsID in wmsIdList):
					wmsIdList.remove(wmsID)
					yield ( wmsID, wmsToJobMap[wmsID])
				except ValueError:	# mismatch in GC<->Condor mapping
					raise RethrowError('Error with canceled condor job:\n%s\nExpected Condor IDs:\n%s\nRemaining condor_rm Output:%s' % (wmsID, wmsIdList, cancelProcess.getMessage() ))

		# cleanup after final yield
		retCode = cancelProcess.wait()
		del activity
		if retCode != 0:
			if self.explainError(cancelProcess, retCode):
				pass
			else:
				cancelProcess.logError(self.errorLog)

# checkJobs: Check status of jobs and yield (jobNum, wmsID, status, other data)
#>>wmsJobIdList: list of (wmsID, JobNum) tuples
	def checkJobs(self, wmsJobIdList):
		if len(wmsJobIdList) == 0:
			raise StopIteration

		wmsJobIdList=[(elem[0][1:],elem[1]) for elem in wmsJobIdList]
		wmsToJobMap = dict(wmsJobIdList)
		wmsIdList = set(zip(*wmsJobIdList)[0])
		wmsIdArgument = " ".join(wmsIdList)

		# NOTE: condor_q calls should extract the maximum amount of information with minimum amount of instructions
		# condor_q will CHECK every job in the queue and RETURN info on every *constraint matching* job in the queue
		# try to include as little non-owned jobs and as many owned jobs as possible
		#statusReturnConstraint = """-constraint '(isUndefined(GridControl_GCIDtoWMSID)==False)'"""

		activity = utils.ActivityLog('checking job status')
		statusProcess = utils.LoggedProcess(self.statusExec, '%(Remote)s %(format)s %(jobIDs)s' % {"jobIDs" : wmsIdArgument, "Remote": self.requestArg, "format" : self.statusReturnFormat })

		utils.vprint('Checking Jobs %s' % wmsIdArgument, 2)
		utils.vprint('condor_q', 2)
		# process all lines of the status executable output
		for statusReturnLine in statusProcess.iter():
			try:
				# test if wmsID job was requested, then extact data and remove from check list
				if statusReturnLine.split()[0] in wmsIdList:
					( jobID, wmsID, status, jobinfo ) = self._statusReturnLineRead(statusReturnLine)
					if wmsToJobMap[wmsID]==jobID:	# make sure these are the droids we are looking for
						wmsIdList.remove(wmsID)
						yield ( jobID, wmsID, status, jobinfo )
			except Exception:
				raise RethrowError('Error reading job status info:\n%s' % statusReturnLine)

		# cleanup after final yield
		retCode = statusProcess.wait()
		if retCode != 0:
			if self.explainError(statusProcess, retCode):
				pass
			else:
				statusProcess.logError(self.errorLog, brief=[256])
			reportQueue=False

		# querrying the history can be SLOW! only do when necessary and possible
		if len(wmsIdList) > 0 and not self.remotePool:
			utils.vprint('condor_history', 2)
			# querrying the history can be VERY slow! Only do so bit by bit if possible
			if self.historyFile:
				historyList = [ "-f "+ file for file in filter(os.path.isfile, glob.glob(self.historyFile+"*")) ]
				historyList.sort()
			else:
				historyList=[""]
			# querry the history file by file until no more jobs need updating
			for historyFile in historyList:
				if len(wmsIdList) > 0:
					# BUG: current condor version does not allow querrying for specific tasks
					statusProcess = utils.LoggedProcess(self.historyExec, '%(fileQuery)s %(format)s %(jobIDs)s' % {"fileQuery": historyFile, "jobIDs" : " ", "format" : self.statusReturnFormat })
					for statusReturnLine in statusProcess.iter():
						# test if line starts with a number and was requested
						try:
							# test if wmsID job was requested, then extact data and remove from check list
							if statusReturnLine.split()[0] in wmsIdList:
								( jobID, wmsID, status, jobinfo ) = self._statusReturnLineRead(statusReturnLine)
								if wmsToJobMap[wmsID]==jobID:	# make sure these are the droids we are looking for
									wmsIdList.remove(wmsID)
									yield ( jobID, wmsID, status, jobinfo )
						except Exception:
							raise RethrowError('Error reading job history info:\n%s' % statusReturnLine)

					# cleanup after final yield
					retCode = statusProcess.wait()
					if retCode != 0:
						if self.explainError(statusProcess, retCode):
							pass
						else:
							statusProcess.logError(self.errorLog, brief=[256])

		del activity

	# helper: process output line from call to condor_q or condor_history
	#>>line: output from condor_q or condor_history
	def _statusReturnLineRead(self,line):
		try:
			statusReturnValues = line.split()
			jobinfo = dict(zip(self.statusReturnKeys, statusReturnValues))
			jobID,wmsID=jobinfo['GCID@WMSID'].split('@')
			if (wmsID != jobinfo['wmsid']):	# consistency check
				raise Exception
			jobinfo['jobid']=int(jobID)
			del jobinfo['GCID@WMSID']
			if "@" in jobinfo["RemoteHost"] and jobinfo["Queue"] != "N/A":
				jobinfo['dest'] = jobinfo["RemoteHost"].split("@")[1] + ': /' + jobinfo["Queue"]
			elif jobinfo["RemoteHost"] == "N/A":
				jobinfo['dest'] = jobinfo["RemoteHost"]
			elif self.debug:
				self.debug.write("Failed to extract 'dest': '%s'\n" % str(line))
			del jobinfo["RemoteHost"],jobinfo["Queue"]
			status = self._statusMap[jobinfo['status']]
			jobinfo['status'] = self._humanMap[jobinfo['status']]
			return ( jobinfo['jobid'], jobinfo['wmsid'], status, jobinfo )
		except Exception:
			raise RethrowError('Error reading job history info:\n%s' % line)


# submitJobs: Submit a number of jobs and yield (jobNum, WMS ID, other data) sequentially
#	GC handles most job data by sending batch setting up the environment and executing/monitoring the actual job
#>>jobNum: internal ID of the Job
#	JobNum is linked to the actual *task* here
	def submitJobs(self, jobNumList, module):		
		# construct a temporary JDL for this batch of jobs
		jdlDescriptor, jdlFilePath = tempfile.mkstemp(suffix='.jdl')
		try:
			data = self.makeJDLdata(jobNumList, module)
			utils.safeWrite(os.fdopen(jdlDescriptor, 'w'), data)
		except Exception:
			utils.removeFiles([jdlFilePath])
			raise RethrowError('Could not write jdl data to %s.' % jdlFilePath)

		# create the _jobconfig.sh file containing the actual data
		for jobNum in jobNumList:
			try:
				cfgPath = os.path.join(self.getSandboxPath(jobNum), 'job_%d.var' % jobNum)
				self._writeJobConfig(cfgPath, jobNum, module)
			except Exception:
				raise RethrowError('Could not write _jobconfig data for %s.' % jobNum)

		try:
			# submit all jobs simultaneously and temporarily store verbose (ClassAdd) output
			log = tempfile.mktemp('.log')
			activity = utils.ActivityLog('submitting jobs to scheduler')
			proc = utils.LoggedProcess(self.submitExec, ' -verbose %(Remote)s %(JDL)s' % { "JDL": jdlFilePath, "Remote": self.submitArg })

			# extract the Condor ID (WMS ID) of the jobs from output ClassAds
			# +GridControl_GCIDtoWMSID = "%s@$(Cluster).$(Process)" % str(jobNum))
			wmsJobIdList = []
			if self.debug:
				self.debug.write("Matching GC-ID to Job-IDs: %s\n" % str(jobNumList))
			for line in proc.iter():
				if "GridControl_GCIDtoWMSID" in line:
					if self.debug:
						self.debug.write(line)
					GCWMSID=line.split('=')[1].strip(' "\n').split('@')
					GCID,WMSID=int(GCWMSID[0]),'C'+GCWMSID[1].strip()
					# Condor creates a default job then overwrites settings on any subsequent job
					# i.e. skip every second - but better be sure
					if ( not wmsJobIdList ) or ( GCID not in zip(*wmsJobIdList)[0] ):
						wmsJobIdList.append((WMSID,GCID))

			retCode = proc.wait()
			del activity

			if (retCode != 0) or ( len(wmsJobIdList) < len(jobNumList) ):
				if self.explainError(proc, retCode):
					pass
				else:
					print "Submitted %4d jobs of %4d expected" % (len(wmsJobIdList),len(jobNumList))
					proc.logError(self.errorLog, jdl = jdlFilePath)
		finally:
			utils.removeFiles([jdlFilePath])

		if self.debug:
			self.debug.write(str(wmsJobIdList) + str(wmsJobIdList) + str.join('', data))
		# yield the (jobNum, WMS ID, other data) of each job successively
		for index in range(len(wmsJobIdList)):
			yield (wmsJobIdList[index][1], wmsJobIdList[index][0], {} )

# makeJDL: create a JDL file's *content* specifying job data for several Jobs
#	GridControl handles job data (executable, environment etc) via batch files which are pre-placed in the sandbox refered to by the JDL
#>>jobNumList: List of jobNums for which to define tasks in this JDL
	def makeJDLdata(self, jobNumList, module):
		gcExec, transferFiles = "",[]
		for toTransfer in map(lambda (description, source, target): source, self._getSandboxFilesIn(module)):
			if 'gc-run.sh' in toTransfer:
				gcExec=toTransfer
			else:
				transferFiles.append(toTransfer)

		gcExec = str(utils.pathShare('gc-run.sh'))
		# header for all jobs
		jdlData = [
			'Universe   = vanilla',
			'Executable = ' + gcExec,
			'notify_user = ' + str( self.config.get( "Condor", "NotifyEmail", None, mutable=True)),
			'Log = ' + str(os.path.join(self.sandPath, "GC_Condor.%s.log")) % time.strftime("%Y%m%d"),
			# transfer files from Schedd to Worker Node
			'should_transfer_files = ' + str( self.config.get( "Condor", "TransferFiles", "YES", mutable=True)),
			'when_to_transfer_output = ' + str( self.config.get( "Condor", "TransferOutputOn", "ON_EXIT", mutable=True)),
			 # remove jobs frozen for more than two days
			'periodic_remove = ( (JobStatus == 5 && (CurrentTime - EnteredCurrentStatus) > (172800)))'
			]
		# remote submissal requires job data to stay active until retrieved
		if self.remotePool:
			jdlData.append("leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")

		# classad data
		for line in str( self.config.get( "Condor", "ClassAdData","",mutable=True) ).splitlines():
			jdlData.append( '+' + line )

		# custom user data
		for line in str( self.config.get( "Condor", "JDLData","",mutable=True) ).splitlines():
			jdlData.append( line )

		# job specific data
		for jobNum in jobNumList:
			sandpath = self.getSandboxPath(jobNum)
			# store matching Grid-Control and Condor ID
			jdlData.append('+GridControl_GCIDtoWMSID = "%s@$(Cluster).$(Process)"' % str(jobNum))
			# condor doesn"t execute the job directly. actual job data, files and arguments are accessed by the GC scripts (but need to be copied to the worker)
			jdlData.append('transfer_input_files = ' + ",".join(transferFiles + [str(os.path.join(sandpath, 'job_%d.var' % jobNum))]) )
			# only copy important files +++ stdout and stderr get remapped but transferred automatically, so don't request them as they would not be found
			jdlData.append('transfer_output_files = ' + ",".join( [ source for (description, source, target) in self._getSandboxFilesOut(module) if ( ( source != 'gc.stdout' ) and ( source != 'gc.stderr' ) ) ] ) )
			jdlData.append('initialdir = ' + str(sandpath))
			jdlData.append('Output = ' + str(os.path.join(sandpath, "gc.stdout")))
			jdlData.append('Error = '  + str(os.path.join(sandpath, "gc.stderr")))
			jdlData.append('arguments = %s '  % jobNum )
			jdlData += self._formatRequirements(jobNum, module)
			jdlData.append('Queue\n')

		# combine JDL and add line breaks
		jdlData = [ line + '\n' for line in jdlData]
		return jdlData


	# helper for converting GC requirements to Condor requirements
	def _formatRequirements(self, jobNum, module):
		jdlReq=[]
		for reqType, reqValue in module.getRequirements(jobNum):

			if reqType == WMS.SITES:
				(blacklist, whitelist) = utils.splitBlackWhiteList(reqValue[1])
				# sites matching regular expression requirements
				refuseRegx=[ site for site in self._siteMap.keys() if True in [ re.search(expression.lower(),siteDescript.lower()) is not None for siteDescript in _siteMap[site] for expression in blacklist ] ]
				desireRegx=[ site for site in self._siteMap.keys() if True in [ re.search(expression.lower(),siteDescript.lower()) is not None for siteDescript in _siteMap[site] for expression in whitelist ] ]
				# sites specifically matched
				refuseSite=[ site for site in self._siteMap.keys() if site.lower() in map(lambda req: req.lower(), blacklist) ]
				desireSite=[ site for site in self._siteMap.keys() if site.lower() in map(lambda req: req.lower(), whitelist) ]
				# sites to actually match; refusing takes precedence over desiring, specific takes precedence over expression
				refuseSites=set(refuseSite).union(set(refuseRegx))
				desireSites=set(desireSite).union(set(desireRegx)-set(refuseRegx))-set(refuseSite)

				if "blacklistSite" in self._condorPublishMap:
					jdlReq.append( str(self._condorPublishMap["blacklistSite"]) + ' = ' + '"' + ','.join(refuseSites)  + '"' )
				if "whitelistSite" in self._condorPublishMap:
					jdlReq.append( str(self._condorPublishMap["whitelistSite"]) + ' = ' + '"' + ','.join(desireSites)  + '"' )

			elif reqType == WMS.WALLTIME:
				if ("walltimeMin" in self._condorPublishMap) and reqValue > 0:
					jdlReq.append( str(self._condorPublishMap["walltimeMin"]) + ' = ' + '"' + str(int(reqValue)) + '"' )

			elif reqType == WMS.STORAGE:
				if ("storageElement" in self._condorPublishMap):
					jdlReq.append( str(self._condorPublishMap["storageElement"]) + ' = ' + '"' + ','.join(reqValue) + '"' )
			
				#append unused requirements to JDL for debugging
			elif self.debug:
				jdlReq.append('# Unused Requirement:')
				jdlReq.append('# Type: %s' % reqType )
				jdlReq.append('# Type: %s' % reqValue )

			#TODO::: GLIDEIN_REQUIRE_GLEXEC_USE, WMS.SOFTWARE, WMS.MEMORY, WMS.CPUS
		return jdlReq