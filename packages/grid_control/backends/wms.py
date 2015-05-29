#-#  Copyright 2007-2015 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

# Generic base class for workload management systems

import os, glob, shutil, itertools
from grid_control import utils
from grid_control.abstract import ClassFactory, NamedObject
from grid_control.backends.access import AccessToken
from grid_control.backends.storage import StorageManager
from grid_control.exceptions import AbstractError, RethrowError, RuntimeError
from grid_control.utils.file_objects import VirtualFile
from python_compat import set, sorted

class WMS(NamedObject):
	configSections = NamedObject.configSections + ['wms', 'backend']
	tagName = 'wms'

	def __init__(self, config, wmsName):
		wmsName = utils.QM(wmsName, wmsName, self.__class__.__name__).upper().replace('.', '_')
		NamedObject.__init__(self, config, wmsName)
		(self.config, self.wmsName) = (config, wmsName)
		self._wait_idle = config.getInt('wait idle', 60, onChange = None)
		self._wait_work = config.getInt('wait work', 10, onChange = None)

	def getTimings(self): # Return (waitIdle, wait)
		return utils.Result(waitOnIdle = self._wait_idle, waitBetweenSteps = self._wait_work)

	def canSubmit(self, neededTime, canCurrentlySubmit):
		raise AbstractError

	def getAccessToken(self, wmsId):
		raise AbstractError # Return access token instance responsible for this wmsId

	def deployTask(self, task, monitor):
		raise AbstractError

	def submitJobs(self, jobNumList, task): # jobNumList = [1, 2, ...]
		raise AbstractError # Return (jobNum, wmsId, data) for successfully submitted jobs

	def checkJobs(self, ids): # ids = [(WMS-61226, 1), (WMS-61227, 2), ...]
		raise AbstractError # Return (jobNum, wmsId, state, info) for active jobs

	def retrieveJobs(self, ids):
		raise AbstractError # Return (jobNum, retCode, data, outputdir) for retrived jobs

	def cancelJobs(self, ids):
		raise AbstractError # Return (jobNum, wmsId) for cancelled jobs

	def _createId(self, wmsIdRaw):
		return 'WMSID.%s.%s' % (self.wmsName, wmsIdRaw)

	def _splitId(self, wmsId):
		if wmsId.startswith('WMSID'): # local wms
			return tuple(wmsId.split('.', 2)[1:])
		elif wmsId.startswith('http'): # legacy support
			return ('grid', wmsId)

	def _getRawIDs(self, ids):
		return map(lambda (wmsId, jobNum): self._splitId(wmsId)[1], ids)

	def parseJobInfo(fn):
		if not os.path.exists(fn):
			return utils.eprint('Warning: "%s" does not exist.' % fn)
		try:
			info_content = open(fn, 'r').read()
		except Exception, ex:
			return utils.eprint('Warning: Unable to read "%s"!\n%s' % (fn, str(ex)))
		if not info_content:
			return utils.eprint('Warning: "%s" is empty!' % fn)
		try:
			data = utils.DictFormat().parse(info_content, keyParser = {None: str})
			return (data['JOBID'], data['EXITCODE'], data)
		except:
			return utils.eprint('Warning: Unable to parse "%s"!' % fn)
	parseJobInfo = staticmethod(parseJobInfo)
utils.makeEnum(['WALLTIME', 'CPUTIME', 'MEMORY', 'CPUS', 'BACKEND', 'SITES', 'QUEUES', 'SOFTWARE', 'STORAGE'], WMS)


class InactiveWMS(WMS):
	def __init__(self, config, wmsName):
		WMS.__init__(self, config, wmsName)
		self._token = ClassFactory(config, ('access', 'TrivialAccessToken'), ('access manager', 'MultiAccessToken'),
			cls = AccessToken, inherit = True, tags = [self]).getInstance()

	def canSubmit(self, neededTime, canCurrentlySubmit):
		return True

	def getAccessToken(self, wmsId):
		return self._token

	def deployTask(self, task, monitor):
		return

	def submitJobs(self, jobNumList, task): # jobNumList = [1, 2, ...]
		utils.vprint('Inactive WMS (%s): Discarded submission of %d jobs' % (self.wmsName, len(jobNumList)), -1)

	def checkJobs(self, ids): # ids = [(WMS-61226, 1), (WMS-61227, 2), ...]
		utils.vprint('Inactive WMS (%s): Discarded check of %d jobs' % (self.wmsName, len(ids)), -1)

	def retrieveJobs(self, ids):
		utils.vprint('Inactive WMS (%s): Discarded retrieval of %d jobs' % (self.wmsName, len(ids)), -1)

	def cancelJobs(self, ids):
		utils.vprint('Inactive WMS (%s): Discarded abort of %d jobs' % (self.wmsName, len(ids)), -1)
WMS.moduleMap['inactive'] = 'InactiveWMS'


class BasicWMS(WMS):
	def __init__(self, config, wmsName):
		WMS.__init__(self, config, wmsName)
		if self.wmsName != self.__class__.__name__.upper():
			utils.vprint('Using batch system: %s (%s)' % (self.__class__.__name__, self.wmsName), -1)
		else:
			utils.vprint('Using batch system: %s' % self.wmsName, -1)

		self.errorLog = config.getWorkPath('error.tar')
		self._outputPath = config.getWorkPath('output')
		utils.ensureDirExists(self._outputPath, 'output directory')
		self._failPath = config.getWorkPath('fail')

		# Initialise access token, broker and storage manager
		self._token = ClassFactory(config, ('access token', 'TrivialAccessToken'), ('access token manager', 'MultiAccessToken'),
			cls = AccessToken, inherit = True, tags = [self]).getInstance()

		# UI -> SE -> WN
		self.smSEIn = config.getClass('se input manager', 'SEStorageManager', cls = StorageManager, tags = [self]).getInstance('se', 'se input', 'SE_INPUT')
		self.smSBIn = config.getClass('sb input manager', 'LocalSBStorageManager', cls = StorageManager, tags = [self]).getInstance('sandbox', 'sandbox', 'SB_INPUT')
		# UI <- SE <- WN
		self.smSEOut = config.getClass('se output manager', 'SEStorageManager', cls = StorageManager, tags = [self]).getInstance('se', 'se output', 'SE_OUTPUT')
		self.smSBOut = None


	def canSubmit(self, neededTime, canCurrentlySubmit):
		return self._token.canSubmit(neededTime, canCurrentlySubmit)


	def getAccessToken(self, wmsId):
		return self._token


	def deployTask(self, task, monitor):
		self.outputFiles = map(lambda (d, s, t): t, self._getSandboxFilesOut(task)) # HACK
		task.validateVariables()

		self.smSEIn.addFiles(map(lambda (d, s, t): t, task.getSEInFiles())) # add task SE files to SM
		# Transfer common SE files
		if self.config.getState('init', detail = 'storage'):
			self.smSEIn.doTransfer(task.getSEInFiles())

		def convert(fnList):
			for fn in fnList:
				if isinstance(fn, str):
					yield (fn, os.path.basename(fn), False)
				else:
					yield (None, os.path.basename(fn.name), fn)

		# Package sandbox tar file
		utils.vprint('Packing sandbox:')
		sandbox = self._getSandboxName(task)
		utils.ensureDirExists(os.path.dirname(sandbox), 'sandbox directory')
		if not os.path.exists(sandbox) or self.config.getState('init', detail = 'sandbox'):
			utils.genTarball(sandbox, convert(self._getSandboxFiles(task, monitor, [self.smSEIn, self.smSEOut])))


	def submitJobs(self, jobNumList, task):
		for jobNum in jobNumList:
			if utils.abort():
				raise StopIteration
			yield self._submitJob(jobNum, task)


	def retrieveJobs(self, ids): # Process output sandboxes returned by getJobsOutput
		# Function to force moving a directory
		def forceMove(source, target):
			try:
				if os.path.exists(target):
					shutil.rmtree(target)
			except IOError, e:
				utils.eprint('Warning: "%s" cannot be removed: %s' % (target, str(e)))
				return False
			try:
				shutil.move(source, target)
			except IOError, e:
				utils.eprint('Warning: Error moving job output directory from "%s" to "%s": %s' % (source, target, str(e)))
				return False
			return True

		retrievedJobs = []

		for inJobNum, dir in self._getJobsOutput(ids):
			# inJobNum != None, dir == None => Job could not be retrieved
			if dir == None:
				if inJobNum not in retrievedJobs:
					yield (inJobNum, -1, {}, None)
				continue

			# inJobNum == None, dir != None => Found leftovers of job retrieval
			if inJobNum == None:
				continue

			# inJobNum != None, dir != None => Job retrieval from WMS was ok
			jobInfo = WMS.parseJobInfo(os.path.join(dir, 'job.info'))
			if jobInfo:
				(jobNum, jobExitCode, jobData) = jobInfo
				if jobNum != inJobNum:
					raise RuntimeError('Invalid job id in job file %s' % info)
				if forceMove(dir, os.path.join(self._outputPath, 'job_%d' % jobNum)):
					retrievedJobs.append(inJobNum)
					yield (jobNum, jobExitCode, jobData, dir)
				else:
					yield (jobNum, -1, {}, None)
				continue

			# Clean empty dirs
			for subDir in map(lambda x: x[0], os.walk(dir, topdown=False)):
				try:
					os.rmdir(subDir)
				except:
					pass

			if os.path.exists(dir):
				# Preserve failed job
				utils.ensureDirExists(self._failPath, 'failed output directory')
				forceMove(dir, os.path.join(self._failPath, os.path.basename(dir)))

			yield (inJobNum, -1, {}, None)


	def _getSandboxName(self, task):
		return self.config.getWorkPath('files', task.taskID, self.wmsName, 'gc-sandbox.tar.gz')


	def _getSandboxFilesIn(self, task):
		return [
			('GC Runtime', utils.pathShare('gc-run.sh'), 'gc-run.sh'),
			('GC Runtime library', utils.pathShare('gc-run.lib'), 'gc-run.lib'),
			('GC Sandbox', self._getSandboxName(task), 'gc-sandbox.tar.gz'),
		]


	def _getSandboxFilesOut(self, task):
		return [
			('GC Wrapper - stdout', 'gc.stdout', 'gc.stdout'),
			('GC Wrapper - stderr', 'gc.stderr', 'gc.stderr'),
			('GC Job summary', 'job.info', 'job.info'),
		] + map(lambda fn: ('Task output', fn, fn), task.getSBOutFiles())


	def _getSandboxFiles(self, task, monitor, smList):
		# Prepare all input files
		depList = set(itertools.chain(*map(lambda x: x.getDependencies(), [task] + smList)))
		depPaths = map(lambda pkg: utils.pathShare('', pkg = pkg), os.listdir(utils.pathGC('packages')))
		depFiles = map(lambda dep: utils.resolvePath('env.%s.sh' % dep, depPaths), depList)
		taskEnv = list(itertools.chain(map(lambda x: x.getTaskConfig(), [monitor, task] + smList)))
		taskEnv.append({'GC_DEPFILES': str.join(' ', depList), 'GC_USERNAME': self._token.getUsername(),
			'GC_WMS_NAME': self.wmsName})
		taskConfig = sorted(utils.DictFormat(escapeString = True).format(utils.mergeDicts(taskEnv), format = 'export %s%s%s\n'))
		varMappingDict = dict(zip(monitor.getTaskConfig().keys(), monitor.getTaskConfig().keys()))
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
		return list(itertools.chain(monitor.getFiles(), depFiles, getTaskFiles(),
			[VirtualFile('_config.sh', taskConfig), VirtualFile('_varmap.dat', varMapping)]))


	def _writeJobConfig(self, cfgPath, jobNum, task, extras = {}):
		try:
			jobEnv = utils.mergeDicts([task.getJobConfig(jobNum), extras])
			jobEnv['GC_ARGS'] = task.getJobArguments(jobNum).strip()
			content = utils.DictFormat(escapeString = True).format(jobEnv, format = 'export %s%s%s\n')
			utils.safeWrite(open(cfgPath, 'w'), content)
		except:
			raise RethrowError('Could not write job config data to %s.' % cfgPath)


	def _submitJob(self, jobNum, task):
		raise AbstractError # Return (jobNum, wmsId, data) for successfully submitted jobs


	def _getJobsOutput(self, ids):
		raise AbstractError # Return (jobNum, sandbox) for finished jobs
