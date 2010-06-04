# Generic base class for workload management systems

from python_compat import *
import sys, os, time, shutil, tarfile, glob
from grid_control import AbstractObject, AbstractError, ConfigError, RuntimeError, UserError, utils, Proxy

class WMS(AbstractObject):
	INLINE_TAR_LIMIT = 256 * 1024
	reqTypes = ('SOFTWARE', 'WALLTIME', 'STORAGE', 'SITES', 'CPUTIME', 'MEMORY', 'OTHER')
	for id, reqType in enumerate(reqTypes):
		locals()[reqType] = id


	def __init__(self, config, module, monitor, backend, defaultproxy = 'TrivialProxy'):
		self.config = config
		self.module = module

		# Initialise proxy
		self.proxy = Proxy.open(config.get(backend, 'proxy', defaultproxy, volatile=True), config)

		self.checkSE = config.getBool(backend, 'check storage', True, volatile=True)

		self._outputPath = os.path.join(config.workDir, 'output')
		if not os.path.exists(self._outputPath):
			if config.opts.init:
				try:
					os.mkdir(self._outputPath)
				except IOError, e:
					raise ConfigError("Problem creating work directory '%s': %s" % (self._outputPath, e))
			else:
				raise ConfigError("Not a properly initialized work directory '%s'." % config.workDir)

		tarFile = os.path.join(config.workDir, 'sandbox.tar.gz')

		self.sandboxIn = [ utils.pathGC('share', 'run.sh'), utils.pathGC('share', 'run.lib'), tarFile ]
		self.sandboxOut = module.getOutFiles() + [ 'gc.stdout', 'gc.stderr', 'job.info' ]

		inFiles = list(monitor.getFiles())
		# Resolve wildcards in input files
		for f in module.getInFiles():
			if isinstance(f, str):
				matched = glob.glob(f)
				if matched != []:
					inFiles.extend(matched)
				else:
					inFiles.append(f)

		taskEnv = module.getTaskConfig()
		taskEnv.update(monitor.getEnv(self))
		taskConfig = utils.DictFormat(escapeString = True).format(taskEnv, format = 'export %s%s%s\n')
		inFiles.append(utils.VirtualFile('_config.sh', sorted(taskConfig)))

		module.validateVariables()
		varMapping = map(lambda (x, y): "%s %s\n" % (x, y), module.getVarMapping().items())
		inFiles.append(utils.VirtualFile('_varmap.dat', str.join('', sorted(varMapping))))
		inFiles.extend(map(lambda x: utils.pathGC('share', 'env.%s.sh' % x), module.getDependencies()))

		utils.vprint("Packing sandbox:")
		def shortName(name):
			name = name.replace(config.workDir.rstrip("/"), "<WORKDIR>")
			return name.replace(utils.pathGC().rstrip("/"), "<GCDIR>")

		if config.opts.init:
			utils.vprint("\t%s" % shortName(tarFile))
			tar = tarfile.TarFile.open(tarFile, 'w:gz')

		for f in sorted(inFiles):
			if isinstance(f, str):
				# Path to filename given
				if not os.path.exists(f):
					raise UserError("File %s does not exist!" % f)

				# Put file in sandbox instead of tar file
				if os.path.getsize(f) > self.INLINE_TAR_LIMIT and f.endswith('.gz') or f.endswith('.bz2'):
					self.sandboxIn.append(f)
					continue

			if config.opts.init:
				# Package sandbox tar file
				if isinstance(f, str):
					utils.vprint("\t\t%s" % shortName(f))
					info = tarfile.TarInfo(os.path.basename(f))
					info.size = os.path.getsize(f)
					handle = open(f, 'rb')
				else:
					utils.vprint("\t\t%s" % shortName(f.name))
					info, handle = f.getTarInfo()

				if info.name.endswith('.sh'):
					info.mode = 0755
				elif info.name.endswith('.py'):
					info.mode = 0755
				else:
					info.mode = 0644
				info.mtime = time.time()

				tar.addfile(info, handle)
				handle.close()

		if config.opts.init:
			tar.close()
		for f in self.sandboxIn:
			if f != tarFile or not config.opts.init:
				utils.vprint("\t%s" % shortName(f))


	def canSubmit(self, length, flag):
		return self.proxy.canSubmit(length, flag)


	def getTimings(self):
		return (60, 10)


	def getRequirements(self, jobNum):
		return self.module.getRequirements(jobNum)


	def bulkSubmissionBegin(self, nJobs):
		return True


	def bulkSubmissionEnd(self):
		pass


	def writeJobConfig(self, jobNum, cfgPath, extras = {}):
		jobEnv = self.module.getJobConfig(jobNum)
		jobEnv['GC_ARGS'] = self.module.getJobArguments(jobNum).strip()
		jobEnv.update(extras)

		try:
			content = utils.DictFormat(escapeString = True).format(jobEnv, format = 'export %s%s%s\n')
			utils.safeWriteFile(cfgPath, content)
		except:
			sys.stderr.write("Could not write job config data to %s.\n" % cfgPath)
			raise


	def submitJob(self, jobNum):
		raise AbstractError


	def submitJobs(self, jobNumList):
		for jobNum in jobNumList:
			if self.config.opts.abort:
				raise StopIteration
			validStorage = True
			for (k, v) in self.module.getRequirements(jobNum):
				if k == WMS.STORAGE and v == []:
					validStorage = False
			if validStorage or not self.checkSE:
				jobNum, wmsId, data = self.submitJob(jobNum)
				yield (jobNum, wmsId, data)
			else:
				utils.vprint("Skipped submission of job %s - empty data location list!" % jobNum, printTime=True, once=True)


	def retrieveJobs(self, ids):
		# Function to parse job info file
		def readJobFile(info):
			data = utils.DictFormat().parse(open(info, 'r'), lowerCaseKey = False)
			return (data['JOBID'], data['EXITCODE'], data)

		# Function to force moving a directory
		def forceMove(source, target):
			try:
				if os.path.exists(target):
					shutil.rmtree(target)
			except IOError, e:
				sys.stderr.write("Warning: '%s' cannot be removed: %s\n" % (target, str(e)))
				return False
			try:
				shutil.move(source, target)
			except IOError, e:
				sys.stderr.write("Warning: Error moving job output directory from '%s' to '%s': %s\n" % (source, target, str(e)))
				return False
			return True

		retrievedJobs = []
		failPath = os.path.join(self.config.workDir, 'fail')

		for inJobNum, dir in self.getJobsOutput(ids):
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
			try:
				jobNum, retCode, data = readJobFile(info)
				if jobNum != inJobNum:
					raise RuntimeError("Invalid job id in job file %s" % info)
				if forceMove(dir, os.path.join(self._outputPath, 'job_%d' % jobNum)):
					retrievedJobs.append(inJobNum)
					yield (jobNum, retCode, data)
				else:
					yield (jobNum, -1, {})
				continue
			except:
				pass

			# Something went wrong
			sys.stderr.write("Warning: '%s' seems broken.\n" % info)
			# Clean empty dirs
			for subDir in map(lambda x: x[0], os.walk(dir, topdown=False)):
				try:
					os.rmdir(subDir)
				except:
					pass

			if os.path.exists(dir):
				# Preserve failed job
				if not os.path.exists(failPath):
					os.mkdir(failPath)
				forceMove(dir, os.path.join(failPath, os.path.basename(dir)))

			yield (inJobNum, -1, {})

WMS.dynamicLoaderPath()
