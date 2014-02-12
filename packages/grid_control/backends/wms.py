# Generic base class for workload management systems

from python_compat import set, sorted
import sys, os, time, stat, shutil, tarfile, glob, itertools
from grid_control import QM, NamedObject, AbstractError, ConfigError, RuntimeError, RethrowError, UserError, utils, Proxy, StorageManager
from broker import Broker

class WMS(NamedObject):
	getConfigSections = NamedObject.createFunction_getConfigSections(['backend'])

	reqTypes = ('WALLTIME', 'CPUTIME', 'MEMORY', 'CPUS', 'BACKEND', 'SITES', 'QUEUES', 'SOFTWARE', 'STORAGE')
	for idx, reqType in enumerate(reqTypes):
		locals()[reqType] = idx

	def __init__(self, config, wmsName, wmsClass):
		wmsName = QM(wmsName, wmsName, self.__class__.__name__).upper().replace('.', '_')
		NamedObject.__init__(self, config, wmsName)
		(self.config, self.wmsName, self.wmsClass) = (config, wmsName, wmsClass)

	def getTimings(self): # Return (waitIdle, wait)
		raise AbstractError

	def canSubmit(self, neededTime, canCurrentlySubmit):
		raise AbstractError

	def getProxy(self, wmsId):
		raise AbstractError # Return proxy instance responsible for this wmsId

	def deployTask(self, module, monitor):
		raise AbstractError

	def submitJobs(self, jobNumList, module): # jobNumList = [1, 2, ...]
		raise AbstractError # Return (jobNum, wmsId, data) for successfully submitted jobs

	def checkJobs(self, ids): # ids = [(WMS-61226, 1), (WMS-61227, 2), ...]
		raise AbstractError # Return (jobNum, wmsId, state, info) for active jobs

	def retrieveJobs(self, ids):
		raise AbstractError # Return (jobNum, retCode, data) for retrived jobs

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

	def _getSections(self, prefix):
		mkSection = lambda x: [x, '%s %s' % (prefix, x)]
		return mkSection(self.wmsName) + mkSection(self.__class__.__name__) + mkSection(self.wmsClass) + [prefix]

WMS.registerObject(tagName = 'wms')


class InactiveWMS(WMS):
	def __init__(self, config, wmsName, wmsClass):
		WMS.__init__(self, config, wmsName, wmsClass)
		self.proxy = Proxy.open('TrivialProxy', config.getScoped(['proxy']))

	def getTimings(self): # Return (waitIdle, wait)
		return (0, 0)

	def canSubmit(self, neededTime, canCurrentlySubmit):
		return True

	def getProxy(self, wmsId):
		return self.proxy

	def deployTask(self, module, monitor):
		return

	def submitJobs(self, jobNumList, module): # jobNumList = [1, 2, ...]
		utils.vprint('Inactive WMS (%s): Discarded submission of %d jobs' % (self.wmsName, len(jobNumList)), -1)

	def checkJobs(self, ids): # ids = [(WMS-61226, 1), (WMS-61227, 2), ...]
		utils.vprint('Inactive WMS (%s): Discarded check of %d jobs' % (self.wmsName, len(ids)), -1)

	def retrieveJobs(self, ids):
		utils.vprint('Inactive WMS (%s): Discarded retrieval of %d jobs' % (self.wmsName, len(ids)), -1)

	def cancelJobs(self, ids):
		utils.vprint('Inactive WMS (%s): Discarded abort of %d jobs' % (self.wmsName, len(ids)), -1)


class BasicWMS(WMS):
	def __init__(self, config, wmsName, wmsClass):
		WMS.__init__(self, config, wmsName, wmsClass)
		if self.wmsName != self.__class__.__name__.upper():
			utils.vprint('Using batch system: %s (%s)' % (self.__class__.__name__, self.wmsName), -1)
		else:
			utils.vprint('Using batch system: %s' % self.wmsName, -1)

		self.errorLog = config.getWorkPath('error.tar')
		self._outputPath = config.getWorkPath('output')
		utils.ensureDirExists(self._outputPath, 'output directory')
		self._failPath = config.getWorkPath('fail')

		# Initialise proxy, broker and storage manager
		self.proxy = config.getClass('proxy', 'TrivialProxy', cls = Proxy).getInstance(config)

		configSM = config.addSections(self._getSections('storage'))
		# UI -> SE -> WN
		self.smSEIn = StorageManager.open('SEStorageManager', configSM, 'se', 'se input', 'SE_INPUT')
		self.smSBIn = StorageManager.open('LocalSBStorageManager', configSM, 'sandbox', 'sandbox', 'SB_INPUT')
		# UI <- SE <- WN
		self.smSEOut = StorageManager.open('SEStorageManager', configSM, 'se', 'se output', 'SE_OUTPUT')
		self.smSBOut = None


	def getTimings(self):
		return (60, 10)


	def canSubmit(self, neededTime, canCurrentlySubmit):
		return self.proxy.canSubmit(neededTime, canCurrentlySubmit)


	def getProxy(self, wmsId):
		return self.proxy


	def deployTask(self, module, monitor):
		self.outputFiles = map(lambda (d, s, t): t, self._getSandboxFilesOut(module)) # HACK
		module.validateVariables()

		self.smSEIn.addFiles(map(lambda (d, s, t): t, module.getSEInFiles())) # add module SE files to SM
		# Transfer common SE files
		if self.config.opts.init:
			self.smSEIn.doTransfer(module.getSEInFiles())

		def convert(fnList):
			for fn in fnList:
				if isinstance(fn, str):
					yield (fn, os.path.basename(fn), False)
				else:
					yield (None, os.path.basename(fn.name), fn)

		# Package sandbox tar file
		utils.vprint('Packing sandbox:')
		sandbox = self._getSandboxName(module)
		utils.ensureDirExists(os.path.dirname(sandbox), 'sandbox directory')
		if not os.path.exists(sandbox) or self.config.opts.init:
			utils.genTarball(sandbox, convert(self._getSandboxFiles(module, monitor, [self.smSEIn, self.smSEOut])))


	def submitJobs(self, jobNumList, module):
		for jobNum in jobNumList:
			if utils.abort():
				raise StopIteration
			yield self._submitJob(jobNum, module)


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
					yield (inJobNum, -1, {})
				continue

			# inJobNum == None, dir != None => Found leftovers of job retrieval
			if inJobNum == None:
				continue

			# inJobNum != None, dir != None => Job retrieval from WMS was ok
			info = os.path.join(dir, 'job.info')
			info_content = None
			if not os.path.exists(info):
				utils.eprint('Warning: "%s" does not exist.' % info)
			else:
				try:
					info_content = open(info, 'r').read()
				except Exception, ex:
					utils.eprint('Warning: Unable to read "%s"!\n%s' % (info, str(ex)))
			if info_content:
				try:
					# Function to parse job info file
					data = utils.DictFormat().parse(info_content, keyParser = {None: str})
					jobNum = data['JOBID']
					if jobNum != inJobNum:
						raise RuntimeError('Invalid job id in job file %s' % info)
					if forceMove(dir, os.path.join(self._outputPath, 'job_%d' % jobNum)):
						retrievedJobs.append(inJobNum)
						yield (jobNum, data['EXITCODE'], data)
					else:
						yield (jobNum, -1, {})
					continue
				except:
					# Something went wrong
					utils.eprint('Warning: "%s" seems broken.' % info)

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

			yield (inJobNum, -1, {})


	def _getSandboxName(self, module):
		return self.config.getWorkPath('files', module.taskID, self.wmsName, 'gc-sandbox.tar.gz')


	def _getSandboxFilesIn(self, module):
		return [
			('GC Runtime', utils.pathShare('gc-run.sh'), 'gc-run.sh'),
			('GC Runtime library', utils.pathShare('gc-run.lib'), 'gc-run.lib'),
			('GC Sandbox', self._getSandboxName(module), 'gc-sandbox.tar.gz'),
		]


	def _getSandboxFilesOut(self, module):
		return [
			('GC Wrapper - stdout', 'gc.stdout', 'gc.stdout'),
			('GC Wrapper - stderr', 'gc.stderr', 'gc.stderr'),
			('GC Job summary', 'job.info', 'job.info'),
		] + map(lambda fn: ('Module output', fn, fn), module.getSBOutFiles())


	def _getSandboxFiles(self, module, monitor, smList):
		# Prepare all input files
		depList = set(itertools.chain(*map(lambda x: x.getDependencies(), [module] + smList)))
		depPaths = map(lambda pkg: utils.pathShare('', pkg = pkg), os.listdir(utils.pathGC('packages')))
		depFiles = map(lambda dep: utils.resolvePath('env.%s.sh' % dep, depPaths), depList)
		taskEnv = list(itertools.chain(map(lambda x: x.getTaskConfig(), [monitor, module] + smList)))
		taskEnv.append({'GC_DEPFILES': str.join(' ', depList), 'GC_USERNAME': self.proxy.getUsername(),
			'GC_WMS_NAME': self.wmsName})
		taskConfig = sorted(utils.DictFormat(escapeString = True).format(utils.mergeDicts(taskEnv), format = 'export %s%s%s\n'))
		varMappingDict = dict(zip(monitor.getTaskConfig().keys(), monitor.getTaskConfig().keys()))
		varMappingDict.update(module.getVarMapping())
		varMapping = sorted(utils.DictFormat(delimeter = ' ').format(varMappingDict, format = '%s%s%s\n'))
		# Resolve wildcards in module input files
		def getModuleFiles():
			for f in module.getSBInFiles():
				matched = glob.glob(f)
				if matched != []:
					for match in matched:
						yield match
				else:
					yield f
		return list(itertools.chain(monitor.getFiles(), depFiles, getModuleFiles(),
			[utils.VirtualFile('_config.sh', taskConfig), utils.VirtualFile('_varmap.dat', varMapping)]))


	def _writeJobConfig(self, cfgPath, jobNum, module, extras = {}):
		try:
			jobEnv = utils.mergeDicts([module.getJobConfig(jobNum), extras])
			jobEnv['GC_ARGS'] = module.getJobArguments(jobNum).strip()
			content = utils.DictFormat(escapeString = True).format(jobEnv, format = 'export %s%s%s\n')
			utils.safeWrite(open(cfgPath, 'w'), content)
		except:
			raise RethrowError('Could not write job config data to %s.' % cfgPath)


	def _submitJob(self, jobNum, module):
		raise AbstractError # Return (jobNum, wmsId, data) for successfully submitted jobs


	def _getJobsOutput(self, ids):
		raise AbstractError # Return (jobNum, sandbox) for finished jobs

