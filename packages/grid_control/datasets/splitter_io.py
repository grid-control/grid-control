import os, tarfile, time, copy, cStringIO, threading, gzip
from grid_control import QM, LoadableObject, AbstractError, RuntimeError, utils, ConfigError, Config, noDefault
from provider_base import DataProvider
from splitter_base import DataSplitter

class BaseJobFileTarAdaptor(object):
	def __init__(self, path):
		log = utils.ActivityLog('Reading job mapping file')
		self.mutex = threading.Semaphore()
		self._fmt = utils.DictFormat()
		self._tar = tarfile.open(path, 'r:')
		(self._cacheKey, self._cacheTar) = (None, None)

		metadata = self._fmt.parse(self._tar.extractfile('Metadata').readlines(), keyParser = {None: str})
		self.maxJobs = metadata.pop('MaxJobs')
		self.classname = metadata.pop('ClassName')
		self.metadata = {None: dict(filter(lambda (k, v): not k.startswith('['), metadata.items()))}
		for (k, v) in filter(lambda (k, v): k.startswith('['), metadata.items()):
			self.metadata.setdefault('None %s' % k.split(']')[0].lstrip('['), {})[k.split(']')[1].strip()] = v
		del log


class DataSplitterIO_V1:
	# Save as tar file to allow random access to mapping data with little memory overhead
	def saveState(self, path, meta, source, sourceLen, message):
		tar = tarfile.open(path, 'w:')
		fmt = utils.DictFormat()

		# Function to close all tarfiles
		def closeSubTar(jobNum, subTarFile, subTarFileObj):
			if subTarFile:
				subTarFile.close()
				subTarFileObj.seek(0)
				subTarFileInfo = tarfile.TarInfo('%03dXX.tgz' % (jobNum / 100))
				subTarFileInfo.size = len(subTarFileObj.getvalue())
				tar.addfile(subTarFileInfo, subTarFileObj)
		# Write the splitting info grouped into subtarfiles
		log = None
		(jobNum, subTarFile, subTarFileObj) = (-1, None, None)
		for jobNum, entry in enumerate(source):
			if jobNum % 100 == 0:
				closeSubTar(jobNum - 1, subTarFile, subTarFileObj)
				subTarFileObj = cStringIO.StringIO()
				subTarFile = tarfile.open(mode = 'w:gz', fileobj = subTarFileObj)
				del log
				log = utils.ActivityLog('%s [%d / %d]' % (message, jobNum, sourceLen))
			# Determine shortest way to store file list
			tmp = entry.pop(DataSplitter.FileList)
			commonprefix = os.path.commonprefix(tmp)
			commonprefix = str.join('/', commonprefix.split('/')[:-1])
			if len(commonprefix) > 6:
				entry[DataSplitter.CommonPrefix] = commonprefix
				savelist = map(lambda x: x.replace(commonprefix + '/', ''), tmp)
			else:
				savelist = tmp
			# Write files with infos / filelist
			def flat((x, y, z)):
				if x in [DataSplitter.Metadata, DataSplitter.MetadataHeader]:
					return (x, y, repr(z))
				elif isinstance(z, list):
					return (x, y, str.join(',', z))
				return (x, y, z)
			for name, data in [('list', str.join('\n', savelist)), ('info', fmt.format(entry, fkt = flat))]:
				info, file = utils.VirtualFile(os.path.join('%05d' % jobNum, name), data).getTarInfo()
				subTarFile.addfile(info, file)
				file.close()
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in entry:
				entry.pop(DataSplitter.CommonPrefix)
			entry[DataSplitter.FileList] = tmp
		closeSubTar(jobNum, subTarFile, subTarFileObj)
		del log
		# Write metadata to allow reconstruction of data splitter
		meta['MaxJobs'] = jobNum + 1
		info, file = utils.VirtualFile('Metadata', fmt.format(meta)).getTarInfo()
		tar.addfile(info, file)
		file.close()
		tar.close()


	def loadState(self, path):
		class JobFileTarAdaptor_V1(BaseJobFileTarAdaptor):
			def __getitem__(self, key):
				self.mutex.acquire()
				if not self._cacheKey == key / 100:
					self._cacheKey = key / 100
					subTarFileObj = self._tar.extractfile('%03dXX.tgz' % (key / 100))
					subTarFileObj = cStringIO.StringIO(gzip.GzipFile(fileobj = subTarFileObj).read()) # 3-4x speedup for sequential access
					self._cacheTar = tarfile.open(mode = 'r', fileobj = subTarFileObj)
				parserMap = { None: str, DataSplitter.NEntries: int, DataSplitter.Skipped: int, 
					DataSplitter.DatasetID: int, DataSplitter.Invalid: utils.parseBool,
					DataSplitter.SEList: utils.parseList, DataSplitter.MetadataHeader: eval,
					DataSplitter.Metadata: lambda x: eval(x.strip("'")) }
				data = self._fmt.parse(self._cacheTar.extractfile('%05d/info' % key).readlines(),
					keyParser = {None: int}, valueParser = parserMap)
				fileList = self._cacheTar.extractfile('%05d/list' % key).readlines()
				if DataSplitter.CommonPrefix in data:
					fileList = map(lambda x: '%s/%s' % (data[DataSplitter.CommonPrefix], x), fileList)
				data[DataSplitter.FileList] = map(str.strip, fileList)
				self.mutex.release()
				return data

		try:
			return JobFileTarAdaptor_V1(path)
		except:
			raise ConfigError("No valid dataset splitting found in '%s'." % path)

class DataSplitterIO_V2:
	def __init__(self):
		self.keySize = 100

	# Save as tar file to allow random access to mapping data with little memory overhead
	def saveState(self, path, meta, source, sourceLen, message):
		tar = tarfile.open(path, 'w:')
		fmt = utils.DictFormat()

		# Function to close all tarfiles
		def closeSubTar(jobNum, subTarFile, subTarFileObj):
			if subTarFile:
				subTarFile.close()
				subTarFileObj.seek(0)
				subTarFileInfo = tarfile.TarInfo('%03dXX.tgz' % (jobNum / self.keySize))
				subTarFileInfo.size = len(subTarFileObj.getvalue())
				tar.addfile(subTarFileInfo, subTarFileObj)
		# Write the splitting info grouped into subtarfiles
		log = None
		(jobNum, lastValid, subTarFile, subTarFileObj) = (-1, -1, None, None)
		for jobNum, entry in enumerate(source):
			if not entry.get(DataSplitter.Invalid, False):
				lastValid = jobNum
			if jobNum % self.keySize == 0:
				closeSubTar(jobNum - 1, subTarFile, subTarFileObj)
				subTarFileObj = cStringIO.StringIO()
				subTarFile = tarfile.open(mode = 'w:gz', fileobj = subTarFileObj)
				del log
				log = utils.ActivityLog('%s [%d / %d]' % (message, jobNum, sourceLen))
			# Determine shortest way to store file list
			tmp = entry.pop(DataSplitter.FileList)
			commonprefix = os.path.commonprefix(tmp)
			commonprefix = str.join('/', commonprefix.split('/')[:-1])
			if len(commonprefix) > 6:
				entry[DataSplitter.CommonPrefix] = commonprefix
				savelist = map(lambda x: x.replace(commonprefix + '/', ''), tmp)
			else:
				savelist = tmp
			# Write files with infos / filelist
			def flat((x, y, z)):
				if x in [DataSplitter.Metadata, DataSplitter.MetadataHeader]:
					return (x, y, repr(z))
				elif isinstance(z, list):
					return (x, y, str.join(',', z))
				return (x, y, z)
			data = str.join('', fmt.format(entry, fkt = flat) + map(lambda fn: '=%s\n' % fn, savelist))
			info, file = utils.VirtualFile('%05d' % jobNum, data).getTarInfo()
			subTarFile.addfile(info, file)
			file.close()
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in entry:
				entry.pop(DataSplitter.CommonPrefix)
			entry[DataSplitter.FileList] = tmp
		closeSubTar(jobNum, subTarFile, subTarFileObj)
		del log
		# Write metadata to allow reconstruction of data splitter
		meta['MaxJobs'] = lastValid + 1
		for (fn, data) in [('Metadata', fmt.format(meta)), ('Version', '2')]:
			info, file = utils.VirtualFile(fn, data).getTarInfo()
			tar.addfile(info, file)
			file.close()
		tar.close()


	def loadState(self, path):
		class JobFileTarAdaptor_V2(BaseJobFileTarAdaptor):
			def __init__(self, path, keySize):
				BaseJobFileTarAdaptor.__init__(self, path)
				self.keySize = keySize

			def __getitem__(self, key):
				if key >= self.maxJobs:
					raise IndexError
				self.mutex.acquire()
				if not self._cacheKey == key / self.keySize:
					self._cacheKey = key / self.keySize
					subTarFileObj = self._tar.extractfile('%03dXX.tgz' % (key / self.keySize))
					subTarFileObj = cStringIO.StringIO(gzip.GzipFile(fileobj = subTarFileObj).read()) # 3-4x speedup for sequential access
					self._cacheTar = tarfile.open(mode = 'r', fileobj = subTarFileObj)
				parserMap = { None: str, DataSplitter.NEntries: int, DataSplitter.Skipped: int, 
					DataSplitter.DatasetID: int, DataSplitter.Invalid: utils.parseBool,
					DataSplitter.SEList: utils.parseList, DataSplitter.MetadataHeader: eval,
					DataSplitter.Metadata: lambda x: eval(x.strip("'")) }
				fullData = self._cacheTar.extractfile('%05d' % key).readlines()
				data = self._fmt.parse(filter(lambda x: not x.startswith('='), fullData),
					keyParser = {None: int}, valueParser = parserMap)
				fileList = map(lambda x: x[1:], filter(lambda x: x.startswith('='), fullData))
				if DataSplitter.CommonPrefix in data:
					fileList = map(lambda x: '%s/%s' % (data[DataSplitter.CommonPrefix], x), fileList)
				data[DataSplitter.FileList] = map(str.strip, fileList)
				self.mutex.release()
				return data

		try:
			return JobFileTarAdaptor_V2(path, self.keySize)
		except:
			raise ConfigError("No valid dataset splitting found in '%s'." % path)


class DataSplitterIO:
	def saveState(self, path, meta, source, sourceLen, message = 'Writing job mapping file', version = 2):
		if version == 1:
			writer = DataSplitterIO_V1()
		else:
			writer = DataSplitterIO_V2()
		writer.saveState(path, meta, source, sourceLen, message)

	def loadState(self, path):
		try:
			version = int(tarfile.open(path, 'r:').extractfile('Version').read())
		except:
			version = 1
		if version == 1:
			state = DataSplitterIO_V1().loadState(path)
		else:
			state = DataSplitterIO_V2().loadState(path)
		return state
