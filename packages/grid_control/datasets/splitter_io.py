# | Copyright 2013-2016 Karlsruhe Institute of Technology
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

import os, gzip
from grid_control import utils
from grid_control.datasets.splitter_base import DataSplitter, DataSplitterIO, PartitionError
from grid_control.utils.file_objects import VirtualFile
from grid_control.utils.parsing import parseBool, parseJSON, parseList
from grid_control.utils.thread_tools import GCLock
from python_compat import BytesBuffer, bytes2str, ifilter, imap, json, lfilter, lmap, tarfile

class BaseJobFileTarAdaptor(object):
	def __init__(self, path):
		activity = utils.ActivityLog('Reading dataset partition file')
		self._lock = GCLock()
		self._fmt = utils.DictFormat()
		self._tar = tarfile.open(path, 'r:')
		(self._cacheKey, self._cacheTar) = (None, None)

		metadata = self._fmt.parse(self._tar.extractfile('Metadata').readlines(), keyParser = {None: str})
		self.maxJobs = metadata.pop('MaxJobs')
		self.classname = metadata.pop('ClassName')
		self.metadata = {'dataset': dict(ifilter(lambda k_v: not k_v[0].startswith('['), metadata.items()))}
		for (k, v) in ifilter(lambda k_v: k_v[0].startswith('['), metadata.items()):
			self.metadata.setdefault('dataset %s' % k.split(']')[0].lstrip('['), {})[k.split(']')[1].strip()] = v
		activity.finish()

		self._parserMap = { None: str, DataSplitter.NEntries: int, DataSplitter.Skipped: int,
			DataSplitter.DatasetID: int, DataSplitter.Invalid: parseBool,
			DataSplitter.Locations: lambda x: parseList(x, ','),
			DataSplitter.MetadataHeader: parseJSON,
			DataSplitter.Metadata: lambda x: parseJSON(x.strip("'")) }

	def __getitem__(self, key):
		if key >= self.maxJobs:
			raise IndexError('Invalid dataset partition %s' % repr(key))
		try:
			self._lock.acquire()
			return self._getPartition(key)
		finally:
			self._lock.release()


class DataSplitterIOAuto(DataSplitterIO):
	def saveSplitting(self, path, meta, source, sourceLen, message = 'Writing job mapping file'):
		writer = DataSplitterIO_V2()
		writer.saveSplitting(path, meta, source, sourceLen, message)

	def loadSplitting(self, path):
		try:
			version = int(tarfile.open(path, 'r:').extractfile('Version').read())
		except Exception:
			version = 1
		if version == 1:
			state = DataSplitterIO_V1().loadSplitting(path)
		else:
			state = DataSplitterIO_V2().loadSplitting(path)
		return state


class DataSplitterIOBase(DataSplitterIO):
	def __init__(self):
		self._fmt = utils.DictFormat() # use a single instance to save time

	def _addToTar(self, tar, name, data):
		info, fileObj = VirtualFile(name, data).getTarInfo()
		tar.addfile(info, fileObj)
		fileObj.close()

	def _addToSubTar(self, subTarTuple, name, data):
		self._addToTar(subTarTuple[0], name, data)

	def _createSubTar(self, subTarFileName):
		subTarFileObj = BytesBuffer()
		subTarFile = tarfile.open(mode = 'w:gz', fileobj = subTarFileObj)
		return (subTarFile, subTarFileObj, subTarFileName)

	# Function to close all contained
	def _closeSubTar(self, tar, subTarTuple):
		if subTarTuple:
			(subTarFile, subTarFileObj, subTarFileName) = subTarTuple
			subTarFile.close()
			try: # Python 3.2 does not close the wrapping gzip file object if an external file object is given
				subTarFile.fileobj.close()
			except Exception:
				pass
			subTarFileObj.seek(0)
			subTarFileInfo = tarfile.TarInfo(subTarFileName)
			subTarFileInfo.size = len(subTarFileObj.getvalue())
			tar.addfile(subTarFileInfo, subTarFileObj)

	# Function to determine the filenames to write (and conditionally set the common prefix in entry)
	def _getReducedFileList(self, entry, filenames):
		commonprefix = os.path.commonprefix(filenames)
		commonprefix = str.join('/', commonprefix.split('/')[:-1])
		if len(commonprefix) > 6:
			entry[DataSplitter.CommonPrefix] = commonprefix
			return lmap(lambda x: x.replace(commonprefix + '/', ''), filenames)
		return filenames

	# Function to format a single file entry with metadata
	def _formatFileEntry(self, k_s_v):
		(x, y, z) = k_s_v
		if x in [DataSplitter.Metadata, DataSplitter.MetadataHeader]:
			return (x, y, json.dumps(z))
		elif isinstance(z, list):
			return (x, y, str.join(',', z))
		return (x, y, z)

	# Save as tar file to allow random access to mapping data with little memory overhead
	def saveSplitting(self, path, meta, source, sourceLen, message = 'Writing job mapping file'):
		tar = tarfile.open(path, 'w:')
		self._saveStateToTar(tar, meta, source, sourceLen, message)
		tar.close()


class DataSplitterIO_V1(DataSplitterIOBase):
	# Save as tar file to allow random access to mapping data with little memory overhead
	def _saveStateToTar(self, tar, meta, source, sourceLen, message):
		# Write the splitting info grouped into subtarfiles
		activity = utils.ActivityLog(message)
		(jobNum, subTar) = (-1, None)
		for jobNum, entry in enumerate(source):
			if jobNum % 100 == 0:
				self._closeSubTar(tar, subTar)
				subTar = self._createSubTar('%03dXX.tgz' % int(jobNum / 100))
				activity.finish()
				activity = utils.ActivityLog('%s [%d / %d]' % (message, jobNum, sourceLen))
			# Determine shortest way to store file list
			tmp = entry.pop(DataSplitter.FileList)
			savelist = self._getReducedFileList(entry, tmp) # can modify entry
			# Write files with infos / filelist
			for name, data in [('list', str.join('\n', savelist)), ('info', self._fmt.format(entry, fkt = self._formatFileEntry))]:
				self._addToSubTar(subTar, os.path.join('%05d' % jobNum, name), data)
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in entry:
				entry.pop(DataSplitter.CommonPrefix)
			entry[DataSplitter.FileList] = tmp
		self._closeSubTar(tar, subTar)
		# Write metadata to allow reconstruction of data splitter
		meta['MaxJobs'] = jobNum + 1
		self._addToTar(tar, 'Metadata', self._fmt.format(meta))
		activity.finish()

	def loadSplitting(self, path):
		class JobFileTarAdaptor_V1(BaseJobFileTarAdaptor):
			def _getPartition(self, key):
				if not self._cacheKey == key / 100:
					self._cacheKey = key / 100
					subTarFileObj = self._tar.extractfile('%03dXX.tgz' % (key / 100))
					subTarFileObj = BytesBuffer(gzip.GzipFile(fileobj = subTarFileObj).read()) # 3-4x speedup for sequential access
					self._cacheTar = tarfile.open(mode = 'r', fileobj = subTarFileObj)
				data = self._fmt.parse(self._cacheTar.extractfile('%05d/info' % key).readlines(),
					keyParser = {None: int}, valueParser = self._parserMap)
				fileList = self._cacheTar.extractfile('%05d/list' % key).readlines()
				if DataSplitter.CommonPrefix in data:
					fileList = imap(lambda x: '%s/%s' % (data[DataSplitter.CommonPrefix], x), fileList)
				data[DataSplitter.FileList] = lmap(str.strip, fileList)
				return data

		try:
			return JobFileTarAdaptor_V1(path)
		except Exception:
			raise PartitionError("No valid dataset splitting found in '%s'." % path)


class DataSplitterIO_V2(DataSplitterIOBase):
	def __init__(self):
		DataSplitterIOBase.__init__(self)
		self._keySize = 100

	def _saveStateToTar(self, tar, meta, source, sourceLen, message):
		# Write the splitting info grouped into subtarfiles
		activity = utils.ActivityLog(message)
		(jobNum, lastValid, subTar) = (-1, -1, None)
		for jobNum, entry in enumerate(source):
			if not entry.get(DataSplitter.Invalid, False):
				lastValid = jobNum
			if jobNum % self._keySize == 0:
				self._closeSubTar(tar, subTar)
				subTar = self._createSubTar('%03dXX.tgz' % int(jobNum / self._keySize))
				activity.finish()
				activity = utils.ActivityLog('%s [%d / %d]' % (message, jobNum, sourceLen))
			# Determine shortest way to store file list
			tmp = entry.pop(DataSplitter.FileList)
			savelist = self._getReducedFileList(entry, tmp) # can modify entry
			# Write files with infos / filelist
			data = str.join('', self._fmt.format(entry, fkt = self._formatFileEntry) + lmap(lambda fn: '=%s\n' % fn, savelist))
			self._addToSubTar(subTar, '%05d' % jobNum, data)
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in entry:
				entry.pop(DataSplitter.CommonPrefix)
			entry[DataSplitter.FileList] = tmp
		self._closeSubTar(tar, subTar)
		activity.finish()
		# Write metadata to allow reconstruction of data splitter
		meta['MaxJobs'] = lastValid + 1
		for (fn, data) in [('Metadata', self._fmt.format(meta)), ('Version', '2')]:
			self._addToTar(tar, fn, data)

	def loadSplitting(self, path):
		class JobFileTarAdaptor_V2(BaseJobFileTarAdaptor):
			def __init__(self, path, keySize):
				BaseJobFileTarAdaptor.__init__(self, path)
				self._keySize = keySize

			def _getPartition(self, key):
				if not self._cacheKey == key / self._keySize:
					self._cacheKey = key / self._keySize
					subTarFileObj = self._tar.extractfile('%03dXX.tgz' % (key / self._keySize))
					subTarFileObj = BytesBuffer(gzip.GzipFile(fileobj = subTarFileObj).read()) # 3-4x speedup for sequential access
					self._cacheTar = tarfile.open(mode = 'r', fileobj = subTarFileObj)
				fullData = lmap(bytes2str, self._cacheTar.extractfile('%05d' % key).readlines())
				data = self._fmt.parse(lfilter(lambda x: not x.startswith('='), fullData),
					keyParser = {None: int}, valueParser = self._parserMap)
				fileList = imap(lambda x: x[1:], ifilter(lambda x: x.startswith('='), fullData))
				if DataSplitter.CommonPrefix in data:
					fileList = imap(lambda x: '%s/%s' % (data[DataSplitter.CommonPrefix], x), fileList)
				data[DataSplitter.FileList] = lmap(str.strip, fileList)
				return data

		try:
			return JobFileTarAdaptor_V2(path, self._keySize)
		except Exception:
			raise PartitionError("No valid dataset splitting found in '%s'." % path)
