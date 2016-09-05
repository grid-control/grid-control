# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

# Generic base class for workload management systems

import os, glob, shutil, logging
from grid_control import utils
from grid_control.backends.access import AccessToken
from grid_control.backends.aspect_status import CheckInfo
from grid_control.backends.storage import StorageManager
from grid_control.gc_plugin import NamedPlugin
from grid_control.output_processor import JobResult
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.file_objects import SafeFile, VirtualFile
from hpfwk import AbstractError, NestedException, clear_current_exception
from python_compat import ichain, identity, imap, izip, lchain, lmap, set, sorted

class BackendError(NestedException):
	pass

BackendJobState = makeEnum([
	'ABORTED',   # job was aborted by the WMS
	'CANCELLED', # job was cancelled
	'DONE',      # job is finished
	'QUEUED',    # job is at WMS and is assigned a place to run
	'RUNNING',   # job is running
	'UNKNOWN',   # job status is unknown
	'WAITING',   # job is at WMS but was not yet assigned some place to run
])

class WMS(NamedPlugin):
	configSections = NamedPlugin.configSections + ['wms', 'backend']
	tagName = 'wms'

	def __init__(self, config, name):
		name = (name or self.__class__.__name__).upper().replace('.', '_')
		NamedPlugin.__init__(self, config, name)
		self._wait_idle = config.getInt('wait idle', 60, onChange = None)
		self._wait_work = config.getInt('wait work', 10, onChange = None)
		self._job_parser = config.getPlugin('job parser', 'JobInfoProcessor',
			cls = 'JobInfoProcessor', onChange = None)

	def getTimings(self): # Return (waitIdle, wait)
		return utils.Result(waitOnIdle = self._wait_idle, waitBetweenSteps = self._wait_work)

	def canSubmit(self, neededTime, canCurrentlySubmit):
		raise AbstractError

	def getAccessToken(self, gcID):
		raise AbstractError # Return access token instance responsible for this gcID

	def deployTask(self, task, monitor, transferSE, transferSB):
		raise AbstractError

	def submitJobs(self, jobNumList, task): # jobNumList = [1, 2, ...]
		raise AbstractError # Return (jobNum, gcID, data) for successfully submitted jobs

	def checkJobs(self, gcIDs): # Check status and return (gcID, job_state, job_info) for active jobs
		raise AbstractError

	def cancelJobs(self, gcIDs): # Cancel jobs and return list of successfully cancelled gcIDs
		raise AbstractError

	def retrieveJobs(self, gcID_jobNum_List):
		raise AbstractError # Return (jobNum, retCode, data, outputdir) for retrived jobs

	def _createId(self, wmsID):
		return 'WMSID.%s.%s' % (self._name, wmsID)

	def _splitId(self, gcID):
		return tuple(gcID.split('.', 2)[1:])

	def _getRawIDs(self, gcID_jobNum_List):
		for (gcID, _) in gcID_jobNum_List:
			yield self._splitId(gcID)[1]

	def _get_map_wmsID_gcID(self, gcIDs):
		result = {}
		for gcID in gcIDs:
			wmsID = self._splitId(gcID)[1]
			if wmsID in result:
				raise BackendError('Multiple gcIDs map to the same wmsID!')
			result[wmsID] = gcID
		return result
makeEnum(['WALLTIME', 'CPUTIME', 'MEMORY', 'CPUS', 'BACKEND', 'SITES', 'QUEUES', 'SOFTWARE', 'STORAGE'], WMS)


class BasicWMS(WMS):
	def __init__(self, config, name, checkExecutor, cancelExecutor):
		WMS.__init__(self, config, name)
		for executor in [checkExecutor, cancelExecutor]:
			executor.setup(self._log)
		(self._check_executor, self._cancel_executor) = (checkExecutor, cancelExecutor)

		if self._name != self.__class__.__name__.upper():
			self._log.info('Using batch system: %s (%s)', self.__class__.__name__, self._name)
		else:
			self._log.info('Using batch system: %s', self._name)

		self.errorLog = config.getWorkPath('error.tar')
		self._runlib = config.getWorkPath('gc-run.lib')
		if not os.path.exists(self._runlib):
			fp = SafeFile(self._runlib, 'w')
			content = SafeFile(utils.pathShare('gc-run.lib')).read()
			fp.write(content.replace('__GC_VERSION__', __import__('grid_control').__version__))
			fp.close()
		self._outputPath = config.getWorkPath('output')
		self._filecachePath = config.getWorkPath('files')
		utils.ensureDirExists(self._outputPath, 'output directory')
		self._failPath = config.getWorkPath('fail')

		# Initialise access token and storage managers

		# UI -> SE -> WN
		self.smSEIn = config.getPlugin('se input manager', 'SEStorageManager', cls = StorageManager,
			tags = [self], pargs = ('se', 'se input', 'SE_INPUT'))
		self.smSBIn = config.getPlugin('sb input manager', 'LocalSBStorageManager', cls = StorageManager,
			tags = [self], pargs = ('sandbox', 'sandbox', 'SB_INPUT'))
		# UI <- SE <- WN
		self.smSEOut = config.getPlugin('se output manager', 'SEStorageManager', cls = StorageManager,
			tags = [self], pargs = ('se', 'se output', 'SE_OUTPUT'))
		self.smSBOut = None

		self._token = config.getCompositePlugin(['proxy', 'access token'], 'TrivialAccessToken',
			'MultiAccessToken', cls = AccessToken, inherit = True, tags = [self])


	def canSubmit(self, neededTime, canCurrentlySubmit):
		return self._token.canSubmit(neededTime, canCurrentlySubmit)


	def getAccessToken(self, gcID):
		return self._token


	def deployTask(self, task, monitor, transferSE, transferSB):
		self.outputFiles = lmap(lambda d_s_t: d_s_t[2], self._getSandboxFilesOut(task)) # HACK
		task.validateVariables()

		self.smSEIn.addFiles(lmap(lambda d_s_t: d_s_t[2], task.getSEInFiles())) # add task SE files to SM
		# Transfer common SE files
		if transferSE:
			self.smSEIn.doTransfer(task.getSEInFiles())

		def convert(fnList):
			for fn in fnList:
				if isinstance(fn, str):
					yield (fn, os.path.basename(fn), False)
				else:
					yield (None, os.path.basename(fn.name), fn)

		# Package sandbox tar file
		self._log.log(logging.INFO1, 'Packing sandbox')
		sandbox = self._getSandboxName(task)
		utils.ensureDirExists(os.path.dirname(sandbox), 'sandbox directory')
		if not os.path.exists(sandbox) or transferSB:
			utils.genTarball(sandbox, convert(self._getSandboxFiles(task, monitor, [self.smSEIn, self.smSEOut])))


	def submitJobs(self, jobNumList, task):
		for jobNum in jobNumList:
			if utils.abort():
				break
			yield self._submitJob(jobNum, task)


	def _run_executor(self, desc, executor, fmt, gcIDs, *args):
		# Perform some action with the executor, translate wmsID -> gcID and format the result
		activity = Activity(desc)
		wmsID_gcID_Map = self._get_map_wmsID_gcID(gcIDs)
		wmsIDs = sorted(wmsID_gcID_Map.keys())

		for result in executor.execute(wmsIDs, *args):
			wmsID = result[0] # result[0] is the wmsID by convention
			gcID = wmsID_gcID_Map.pop(wmsID, None)
			if gcID is not None:
				yield fmt((gcID,) + result[1:])
			else:
				self._log.debug('unable to find gcID for wmsID %r', wmsID)
		activity.finish()


	def checkJobs(self, gcIDs): # Check status and return (gcID, job_state, job_info) for active jobs
		def fmt(value): # translate CheckInfo enum values in job_info dictionary
			job_info = value[2] # get mutable job_info dictionary from the immutable tuple
			for key in CheckInfo.enumValues:
				if key in job_info:
					job_info[CheckInfo.enum2str(key)] = job_info.pop(key)
			return value
		return self._run_executor('checking job status', self._check_executor, fmt, gcIDs)


	def cancelJobs(self, gcIDs):
		return self._run_executor('cancelling jobs', self._cancel_executor, identity, gcIDs, self._name)


	def retrieveJobs(self, gcID_jobNum_List): # Process output sandboxes returned by getJobsOutput
		# Function to force moving a directory
		def forceMove(source, target):
			try:
				if os.path.exists(target):
					shutil.rmtree(target)
			except IOError:
				self._log.exception('%r cannot be removed', target)
				return False
			try:
				shutil.move(source, target)
			except IOError:
				self._log.exception('Error moving job output directory from %r to %r', source, target)
				return False
			return True

		retrievedJobs = []

		for inJobNum, pathName in self._getJobsOutput(gcID_jobNum_List):
			# inJobNum != None, pathName == None => Job could not be retrieved
			if pathName is None:
				if inJobNum not in retrievedJobs:
					yield (inJobNum, -1, {}, None)
				continue

			# inJobNum == None, pathName != None => Found leftovers of job retrieval
			if inJobNum is None:
				continue

			# inJobNum != None, pathName != None => Job retrieval from WMS was ok
			jobFile = os.path.join(pathName, 'job.info')
			try:
				job_info = self._job_parser.process(pathName)
			except Exception:
				self._log.exception('Unable to parse job.info')
				job_info = None
			if job_info:
				jobNum = job_info[JobResult.JOBNUM]
				if jobNum != inJobNum:
					raise BackendError('Invalid job id in job file %s' % jobFile)
				if forceMove(pathName, os.path.join(self._outputPath, 'job_%d' % jobNum)):
					retrievedJobs.append(inJobNum)
					yield (jobNum, job_info[JobResult.EXITCODE], job_info[JobResult.RAW], pathName)
				else:
					yield (jobNum, -1, {}, None)
				continue

			# Clean empty pathNames
			for subDir in imap(lambda x: x[0], os.walk(pathName, topdown=False)):
				try:
					os.rmdir(subDir)
				except Exception:
					clear_current_exception()

			if os.path.exists(pathName):
				# Preserve failed job
				utils.ensureDirExists(self._failPath, 'failed output directory')
				forceMove(pathName, os.path.join(self._failPath, os.path.basename(pathName)))

			yield (inJobNum, -1, {}, None)


	def _getSandboxName(self, task):
		return os.path.join(self._filecachePath, task.taskID, self._name, 'gc-sandbox.tar.gz')


	def _getSandboxFilesIn(self, task):
		return [
			('GC Runtime', utils.pathShare('gc-run.sh'), 'gc-run.sh'),
			('GC Runtime library', self._runlib, 'gc-run.lib'),
			('GC Sandbox', self._getSandboxName(task), 'gc-sandbox.tar.gz'),
		]


	def _getSandboxFilesOut(self, task):
		return [
			('GC Wrapper - stdout', 'gc.stdout', 'gc.stdout'),
			('GC Wrapper - stderr', 'gc.stderr', 'gc.stderr'),
			('GC Job summary', 'job.info', 'job.info'),
		] + lmap(lambda fn: ('Task output', fn, fn), task.getSBOutFiles())


	def _getSandboxFiles(self, task, monitor, smList):
		# Prepare all input files
		depList = set(ichain(imap(lambda x: x.getDependencies(), [task] + smList)))
		depPaths = lmap(lambda pkg: utils.pathShare('', pkg = pkg), os.listdir(utils.pathPKG()))
		depFiles = lmap(lambda dep: utils.resolvePath('env.%s.sh' % dep, depPaths), depList)
		taskEnv = utils.mergeDicts(imap(lambda x: x.getTaskConfig(), [monitor, task] + smList))
		taskEnv.update({'GC_DEPFILES': str.join(' ', depList), 'GC_USERNAME': self._token.getUsername(),
			'GC_WMS_NAME': self._name})
		taskConfig = sorted(utils.DictFormat(escapeString = True).format(taskEnv, format = 'export %s%s%s\n'))
		varMappingDict = dict(izip(monitor.getTaskConfig().keys(), monitor.getTaskConfig().keys()))
		varMappingDict.update(task.getVarMapping())
		varMapping = sorted(utils.DictFormat(delimeter = ' ').format(varMappingDict, format = '%s%s%s\n'))
		# Resolve wildcards in task input files
		def getTaskFiles():
			for f in task.getSBInFiles():
				matched = glob.glob(f.pathAbs)
				if matched != []:
					for match in matched:
						yield match
				else:
					yield f.pathAbs
		return lchain([monitor.getFiles(), depFiles, getTaskFiles(),
			[VirtualFile('_config.sh', taskConfig), VirtualFile('_varmap.dat', varMapping)]])


	def _writeJobConfig(self, cfgPath, jobNum, task, extras):
		try:
			jobEnv = utils.mergeDicts([task.getJobConfig(jobNum), extras])
			jobEnv['GC_ARGS'] = task.getJobArguments(jobNum).strip()
			content = utils.DictFormat(escapeString = True).format(jobEnv, format = 'export %s%s%s\n')
			utils.safeWrite(open(cfgPath, 'w'), content)
		except Exception:
			raise BackendError('Could not write job config data to %s.' % cfgPath)


	def _submitJob(self, jobNum, task):
		raise AbstractError # Return (jobNum, gcID, data) for successfully submitted jobs


	def _getJobsOutput(self, gcID_jobNum_List):
		raise AbstractError # Return (jobNum, sandbox) for finished jobs


class Grid(WMS): # redirector - used to avoid loading the whole grid module just for the default
	configSections = WMS.configSections + ['grid']

	def __new__(cls, config, name):
		gridWMS = 'GliteWMS'
		grid_config = config.changeView(viewClass = 'TaggedConfigView', setClasses = [WMS.getClass(gridWMS)])
		return WMS.createInstance(gridWMS, grid_config, name)
