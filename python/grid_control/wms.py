# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

import os, time, shutil, tarfile
from grid_control import AbstractObject, ConfigError, RuntimeError, utils, enumerate

class WMS(AbstractObject):
	INLINE_TAR_LIMIT = 256 * 1024
	reqTypes = ('MEMBER', 'WALLTIME', 'STORAGE', 'SITES')
	_reqTypeDict = {}
	for id, reqType in enumerate(reqTypes):
		_reqTypeDict[reqType] = id
		locals()[reqType] = id


	def __init__(self, config, module, init):
		self.config = config
		self.module = module
		self.workDir = config.getPath('global', 'workdir')

		self._sites = config.get('grid', 'sites', '').split()

		self._outputPath = os.path.join(self.workDir, 'output')
		try:
			if not os.path.exists(self._outputPath):
				if init:
					os.mkdir(self._outputPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % self.workDir)
		except IOError, e:
			raise ConfigError("Problem creating work directory '%s': %s" % (self._outputPath, e))

		tarFile = os.path.join(self.workDir, 'sandbox.tar.gz')
		self.sandboxIn = [ utils.atRoot('share', 'run.sh'), tarFile ]

		self.sandboxOut = [ 'stdout.txt', 'stderr.txt', 'jobinfo.txt' ]
		self.sandboxOut.extend(self.module.getOutFiles())

		if init:
			tar = tarfile.TarFile.open(tarFile, 'w:gz')

		inFiles = [ self.module.makeConfig() ]
		inFiles.extend(self.module.getInFiles())

		for file in inFiles:
			if type(file) == str:
				name = os.path.basename(file)
				if not os.path.isabs(file):
					path = os.path.join(self.workDir, file)
				else:
					path = file
				file = open(path, 'rb')
				file.seek(0, 2)
				size = file.tell()
				file.seek(0, 0)
				if size > self.INLINE_TAR_LIMIT and \
				   name.endswith('.gz') or name.endswith('.bz2'):
					file.close()
					self.sandboxIn.append(path)
					continue
			else:
				name = file.name
				size = file.size

			if init:
				info = tarfile.TarInfo(name)
				if name.endswith('.sh'):
					info.mode = 0755
				else:
					info.mode = 0644
				info.size = size
				info.mtime = time.time()

				tar.addfile(info, file)

			file.close()

		if init:
			tar.close()


	def _formatRequirement(self, type, *args):
		if type == self.MEMBER:
			return self.memberReq(*args)
		elif type == self.WALLTIME:
			return self.wallTimeReq(*args)
		elif type == self.STORAGE:
			return self.storageReq(*args)
		elif type == self.SITES:
			return self.sitesReq(*args)
		else:
			raise RuntimeError('unknown requirement type %d' % type)


	def formatRequirements(self, reqs_):
		# add requirements not handled by caller
		def mangleSite(site):
			neg = site[0] == '-'
			if neg:
				site = site[1:]
			return (self.SITES, site, neg)

		reqs = map(mangleSite, self._sites)
		reqs.extend(reqs_)

		if len(reqs) == 0:
			return None
		return str.join(' && ', map(lambda x: self._formatRequirement(*x), reqs))


	def memberReq(self, *args):
		raise RuntimeError('memberReq is abstract')


	def wallTimeReq(self, *args):
		raise RuntimeError('wallTimeReq is abstract')


	def retrieveJobs(self, ids):
		dirs = self.getJobsOutput(ids)

		result = []

		for dir in dirs:
			info = os.path.join(dir, 'jobinfo.txt')
			if not os.path.exists(info):
				continue

			try:
				data = utils.parseShellDict(open(info, 'r'))
				id = data['JOBID']
				retCode = data['EXITCODE']
			except:
				print >> sys.stderr, "Warning: '%s' seems broken." % info
				continue

			dst = os.path.join(self._outputPath, 'job_%d' % id)

			if os.path.exists(dst):
				try:
					shutil.rmtree(dst)
				except IOError, e:
					print >> sys.stderr, \
					      "Warning: '%s' cannot be removed: %s" \
					      % (dst, str(e))
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

			result.append((id, retCode))

		return result
