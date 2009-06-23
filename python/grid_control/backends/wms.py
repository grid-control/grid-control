# Generic base class for workload management systems

import sys, os, time, shutil, tarfile
from grid_control import AbstractObject, ConfigError, RuntimeError, utils, proxy

class WMS(AbstractObject):
	INLINE_TAR_LIMIT = 256 * 1024
	reqTypes = ('MEMBER', 'WALLTIME', 'STORAGE', 'SITES', 'CPUTIME', 'MEMORY', 'OTHER')
	for id, reqType in enumerate(reqTypes):
		locals()[reqType] = id


	def __init__(self, workDir, config, opts, module, backend):
		self.config = config
		self.module = module
		self.workDir = workDir
		self.proxy = config.get(backend, 'proxy', 'TrivialProxy')

		self._outputPath = os.path.join(self.workDir, 'output')

		if not os.path.exists(self._outputPath):
			if opts.init:
				try:
					os.mkdir(self._outputPath)
				except IOError, e:
					raise ConfigError("Problem creating work directory '%s': %s" % (self._outputPath, e))
			else:
				raise ConfigError("Not a properly initialized work directory '%s'." % self.workDir)

		tarFile = os.path.join(self.workDir, 'sandbox.tar.gz')

		self.sandboxIn = [ utils.atRoot('share', 'grid.sh'), utils.atRoot('share', 'run.lib'), tarFile ]
		self.sandboxOut = self.module.getOutFiles() + [ 'stdout.txt', 'stderr.txt', 'jobinfo.txt' ]

		taskConfig = utils.DictFormat(escapeString = True).format(self.module.getTaskConfig())
		inFiles = self.module.getInFiles() + [ utils.VirtualFile('_config.sh', utils.SortedList(taskConfig)) ]

		utils.vprint("Packing sandbox:")
		if opts.init:
			utils.vprint("\t%s" % tarFile)
			tar = tarfile.TarFile.open(tarFile, 'w:gz')

		for file in inFiles:
			if type(file) == str:
				# Path to filename given
				if os.path.isabs(file):
					path = file
				else:
					path = os.path.join(self.workDir, file)

				# Put file in sandbox instead of tar file
				if os.path.getsize(path) > self.INLINE_TAR_LIMIT and file.endswith('.gz') or file.endswith('.bz2'):
					self.sandboxIn.append(path)
					continue

			if opts.init:
				# Package sandbox tar file
				if type(file) == str:
					utils.vprint("\t\t%s" % path)
					info = tarfile.TarInfo(os.path.basename(file))
					info.size = os.path.getsize(path)
					handle = open(path, 'rb')
				else:
					utils.vprint("\t\t%s" % file.name)
					info, handle = file.getTarInfo()

				if info.name.endswith('.sh'):
					info.mode = 0755
				elif info.name.endswith('.py'):
					info.mode = 0755
				else:
					info.mode = 0644
				info.mtime = time.time()

				tar.addfile(info, handle)
				handle.close()

		if opts.init:
			tar.close()
		for file in self.sandboxIn:
			if file != tarFile or not opts.init:
				utils.vprint("\t%s" % file)


	def getRequirements(self, job):
		return self.module.getRequirements(job)


	def bulkSubmissionBegin(self, jobs):
		pass


	def bulkSubmissionEnd(self):
		pass


	def submitJobs(self, ids):
		for jobNum in ids:
			jobNum, wmsId, data = self.submitJob(jobNum)
			if wmsId == None:
				continue # FIXME
			yield (jobNum, wmsId, data)


	def retrieveJobs(self, ids):
		def readJobFile(fp):
			data = utils.DictFormat().parse(fp, lowerCaseKey = False)
			return (data['JOBID'], data['EXITCODE'], data)

		for dir in self.getJobsOutput(ids):
			info = os.path.join(dir, 'jobinfo.txt')
			try:
				id, retCode, data = readJobFile(open(info, 'r'))
			except:
				sys.stderr.write("Warning: '%s' seems broken.\n" % info)
#				# Try to extract jobinfo from stdout file
#				if not os.path.exists(dir):
#					continue
#				try:
#					info = os.path.join(dir, 'stdout.txt')
#					id, retCode, data = readJobFile(open(info, 'r').readlines()[-3:])
#					sys.stderr.write("Recovered job %d with exit code %d...\n" % (id, retCode))
#				except:
					# Move corrupted output to fail directory
				failpath = os.path.join(self.workDir, 'fail')
				if not os.path.exists(failpath):
					os.mkdir(failpath)
				shutil.move(dir, os.path.join(failpath, os.path.basename(dir)))
				continue

			dst = os.path.join(self._outputPath, 'job_%d' % id)

			try:
				if os.path.exists(dst):
					shutil.rmtree(dst)
			except IOError, e:
				sys.stderr.write("Warning: '%s' cannot be removed: %s" % (dst, str(e)))
				continue

			try:
				shutil.move(dir, dst)
			except IOError, e:
				sys.stderr.write("Warning: Error moving job output directory from '%s' to '%s': %s" \
					% (dir, dst, str(e)))
				continue

			yield (id, retCode, data)


	def getProxy(self):
		return proxy.Proxy.open(self.proxy)
