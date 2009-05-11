# Generic base class for workload management systems

import sys, os, time, shutil, tarfile
from grid_control import AbstractObject, ConfigError, RuntimeError, utils, proxy

class WMS(AbstractObject):
	INLINE_TAR_LIMIT = 256 * 1024
	reqTypes = ('MEMBER', 'WALLTIME', 'STORAGE', 'SITES', 'CPUTIME', 'MEMORY', 'OTHER')
	_reqTypeDict = {}
	for id, reqType in enumerate(reqTypes):
		_reqTypeDict[reqType] = id
		locals()[reqType] = id


	def __init__(self, config, module, backend, init):
		self.config = config
		self.module = module
		self.workDir = config.getPath('global', 'workdir')
		self._proxy = config.get(backend, 'proxy', 'TrivialProxy')
		self._sites = config.get(backend, 'sites', '').split()

		self._outputPath = os.path.join(self.workDir, 'output')

		if not os.path.exists(self._outputPath):
			if init:
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
		if init:
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

			if init:
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

		if init:
			tar.close()
		for file in self.sandboxIn:
			if file != tarFile or not init:
				utils.vprint("\t%s" % file)


	def getRequirements(self, job):
		reqs = self.module.getRequirements(job)
		# add site requirements
		if len(self._sites):
			reqs.append((self.SITES, self._sites))
		if len(reqs) == 0:
			return None
		return reqs


	def retrieveJobs(self, ids):
		dirs = self.getJobsOutput(ids)

		result = []

		for dir in dirs:
			info = os.path.join(dir, 'jobinfo.txt')
			if not os.path.exists(info):
				continue

			try:
				data = utils.DictFormat().parse(open(info, 'r'), lowerCaseKey = False)
				id = data['JOBID']
				retCode = data['EXITCODE']
			except:
				print >> sys.stderr, "Warning: '%s' seems broken." % info
				continue

			dst = os.path.join(self._outputPath, 'job_%d' % id)

			try:
				if os.path.exists(dst):
					shutil.rmtree(dst)
			except IOError, e:
				print >> sys.stderr, "Warning: '%s' cannot be removed: %s" % (dst, str(e))
				continue

			try:
				try:
					shutil.move(dir, dst)
				except AttributeError:
					os.renames(dir, dst)
			except IOError, e:
				print >> sys.stderr, \
					"Warning: Error moving job output directory from '%s' to '%s': %s" \
					% (dir, dst, str(e))
				continue

			result.append((id, retCode, data))
		return result


	def bulkSubmissionBegin(self):
		pass


	def bulkSubmissionEnd(self):
		pass


	def getProxy(self):
		return proxy.Proxy.open(self._proxy)
