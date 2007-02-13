# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

import os, time, tarfile
from grid_control import AbstractObject, RuntimeError, utils, enumerate

class WMS(AbstractObject):
	INLINE_TAR_LIMIT = 256 * 1024
	reqTypes = ('MEMBER', 'WALLTIME', 'STORAGE')
	_reqTypeDict = {}
	for id, reqType in enumerate(reqTypes):
		_reqTypeDict[reqType] = id
		locals()[reqType] = id


	def __init__(self, config, module, init):
		self.config = config
		self.module = module
		self.workDir = config.getPath('global', 'workdir')

		tarFile = os.path.join(self.workDir, 'sandbox.tar.gz')
		self.sandboxIn = [ utils.atRoot('share', 'run.sh'), tarFile ]

		self.sandboxOut = [ 'stdout.txt', 'stderr.txt' ]

		if init:
			tar = tarfile.TarFile.open(tarFile, 'w:gz')

		for file in self.module.getInFiles():
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
		else:
			raise RuntimeError('unknown requirement type %d' % type)


	def formatRequirements(self, reqs):
		if len(reqs) == 0:
			return None
		return str.join(' && ', map(lambda x: self._formatRequirement(*x), reqs))


	def memberReq(self, *args):
		raise RuntimeError('memberReq is abstract')


	def wallTimeReq(self, *args):
		raise RuntimeError('wallTimeReq is abstract')
