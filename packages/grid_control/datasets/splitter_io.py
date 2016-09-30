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
from grid_control.datasets.splitter_base import DataSplitter, DataSplitterIO, PartitionError
from grid_control.utils import split_list, DictFormat
from grid_control.utils.activity import Activity
from grid_control.utils.file_objects import VirtualFile
from grid_control.utils.parsing import parse_bool, parse_json, parse_list
from grid_control.utils.thread_tools import GCLock
from hpfwk import clear_current_exception, AbstractError
from python_compat import BytesBuffer, bytes2str, ifilter, imap, json, lfilter, lmap, tarfile


class PartitionReader(object):
	def __init__(self, path):
		activity = Activity('Reading dataset partition file')
		self._lock = GCLock()
		self._fmt = DictFormat()
		self._tar = tarfile.open(path, 'r:')
		(self._cacheKey, self._cacheTar) = (None, None)

		metadata = self._fmt.parse(self._tar.extractfile('Metadata').readlines(), key_parser={None: str})
		self._partition_len = metadata.pop('MaxJobs')
		self.splitter_name = metadata.pop('ClassName')
		(metadata_item_list_general, metadata_item_list_specific) = split_list(metadata.items(),
			fun=lambda k_v: not k_v[0].startswith('['))
		self.metadata = {'dataset': dict(metadata_item_list_general)}
		for (key, value) in metadata_item_list_specific:
			section, option = key.split(']', 1)
			self.metadata.setdefault('dataset %s' % section.lstrip('['), {})[option.strip()] = value
		activity.finish()

		self._map_enum2parser = {
			None: str,
			DataSplitter.NEntries: int, DataSplitter.Skipped: int,
			DataSplitter.Invalid: parse_bool,
			DataSplitter.Locations: lambda x: parse_list(x, ','),
			DataSplitter.MetadataHeader: parse_json,
			DataSplitter.Metadata: lambda x: parse_json(x.strip("'"))
		}

	def get_partition_len(self):
		return self._partition_len

	def __getitem__(self, partition_num):
		if partition_num >= self._partition_len:
			raise IndexError('Invalid dataset partition %s' % repr(partition_num))
		try:
			self._lock.acquire()
			return self._get_partition(partition_num)
		finally:
			self._lock.release()

	def _get_partition(self, partition_num):
		raise AbstractError


class DataSplitterIOAuto(DataSplitterIO):
	def import_partition_source(self, path):
		try:
			version = int(tarfile.open(path, 'r:').extractfile('Version').read())
		except Exception:
			version = 1
		if version == 1:
			return DataSplitterIOV1().import_partition_source(path)
		return DataSplitterIOV2().import_partition_source(path)

	def save_partition_source(self, path, meta, source, sourceLen, message='Writing job mapping file'):
		writer = DataSplitterIOV2()
		writer.save_partition_source(path, meta, source, sourceLen, message)


class DataSplitterIOBase(DataSplitterIO):
	def __init__(self):
		self._fmt = DictFormat()  # use a single instance to save time

	def import_partition_source(self, path):
		# Save as outer_tar file to allow random access to mapping data with little memory overhead
		try:
			return self._load_partition_source(path)
		except Exception:
			raise PartitionError("No valid dataset splitting found in '%s'." % path)

	def _load_partition_source(self, path):
		raise AbstractError

	def _save_partition_source(self):
		raise AbstractError

	def save_partition_source(self, path, meta, source, sourceLen, message='Writing job mapping file'):
		outer_tar = tarfile.open(path, 'w:')
		self._save_partition_source(outer_tar, meta, source, sourceLen, message)
		outer_tar.close()

	def _addTonested_tar(self, nested_tarTuple, name, data):
		self._addToTar(nested_tarTuple[0], name, data)

	def _addToTar(self, outer_tar, name, data):
		info, fp = VirtualFile(name, data).get_tar_info()
		outer_tar.addfile(info, fp)
		fp.close()

	def _closenested_tar(self, outer_tar, nested_tarTuple):
		# Function to close all contained outer_tar objects
		if nested_tarTuple:
			(nested_tar, nested_tar_fp, nested_tarName) = nested_tarTuple
			nested_tar.close()
			try:  # Python 3.2 does not close the wrapping gzip file object if an external file object is given
				nested_tar.fileobj.close()
			except Exception:
				clear_current_exception()
			nested_tar_fp.seek(0)
			nested_tarInfo = tarfile.TarInfo(nested_tarName)
			nested_tarInfo.size = len(nested_tar_fp.getvalue())
			outer_tar.addfile(nested_tarInfo, nested_tar_fp)

	def _create_nested_tar(self, nested_tarName):
		nested_tar_fp = BytesBuffer()
		nested_tar = tarfile.open(mode='w:gz', fileobj=nested_tar_fp)
		return (nested_tar, nested_tar_fp, nested_tarName)

	def _formatFileEntry(self, k_s_v):
		# Function to format a single file entry with metadata
		(x, y, z) = k_s_v
		if x in [DataSplitter.Metadata, DataSplitter.MetadataHeader]:
			return (x, y, json.dumps(z))
		elif isinstance(z, list):
			return (x, y, str.join(',', z))
		return (x, y, z)

	def _getReducedFileList(self, partition, filenames):
		# Function to determine the filenames to write (and conditionally set the common prefix in partition)
		commonprefix = os.path.commonprefix(filenames)
		commonprefix = str.join('/', commonprefix.split('/')[:-1])
		if len(commonprefix) > 6:
			partition[DataSplitter.CommonPrefix] = commonprefix
			return lmap(lambda x: x.replace(commonprefix + '/', ''), filenames)
		return filenames


class DataSplitterIOV1(DataSplitterIOBase):
	# Save as outer_tar file to allow random access to mapping data with little memory overhead
	def _load_partition_source(self, path):
		class JobFileTarAdaptorV1(PartitionReader):
			def _get_partition(self, key):
				if not self._cacheKey == key / 100:
					self._cacheKey = key / 100
					nested_tar_fp = self._tar.extractfile('%03dXX.tgz' % (key / 100))
					nested_tar_fp = BytesBuffer(gzip.GzipFile(fileobj=nested_tar_fp).read())  # 3-4x speedup for sequential access
					self._cacheTar = tarfile.open(mode='r', fileobj=nested_tar_fp)
				data = self._fmt.parse(self._cacheTar.extractfile('%05d/info' % key).readlines(),
					key_parser={None: int}, value_parser=self._map_enum2parser)
				fileList = lmap(bytes2str, self._cacheTar.extractfile('%05d/list' % key).readlines())
				if DataSplitter.CommonPrefix in data:
					fileList = imap(lambda x: '%s/%s' % (data[DataSplitter.CommonPrefix], x), fileList)
				data[DataSplitter.FileList] = lmap(str.strip, fileList)
				return data
		return JobFileTarAdaptorV1(path)

	def _save_partition_source(self, outer_tar, meta, source, sourceLen, message):
		# Write the splitting info grouped into nested_tars
		activity = Activity(message)
		(jobnum, nested_tar) = (-1, None)
		for jobnum, partition in enumerate(source):
			if jobnum % 100 == 0:
				self._closenested_tar(outer_tar, nested_tar)
				nested_tar = self._create_nested_tar('%03dXX.tgz' % int(jobnum / 100))
				activity.update('%s [%d / %d]' % (message, jobnum, sourceLen))
			# Determine shortest way to store file list
			tmp = partition.pop(DataSplitter.FileList)
			savelist = self._getReducedFileList(partition, tmp)  # can modify partition
			# Write files with infos / filelist
			for name, data in [('list', str.join('\n', savelist)), ('info', self._fmt.format(partition, fkt=self._formatFileEntry))]:
				self._addTonested_tar(nested_tar, os.path.join('%05d' % jobnum, name), data)
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in partition:
				partition.pop(DataSplitter.CommonPrefix)
			partition[DataSplitter.FileList] = tmp
		self._closenested_tar(outer_tar, nested_tar)
		# Write metadata to allow reconstruction of data splitter
		meta['MaxJobs'] = jobnum + 1
		self._addToTar(outer_tar, 'Metadata', self._fmt.format(meta))
		activity.finish()


class DataSplitterIOV2(DataSplitterIOBase):
	def __init__(self):
		DataSplitterIOBase.__init__(self)
		self._keySize = 100

	def _load_partition_source(self, path):
		class JobFileTarAdaptorV2(PartitionReader):
			def __init__(self, path, keySize):
				PartitionReader.__init__(self, path)
				self._keySize = keySize

			def _get_partition(self, key):
				if not self._cacheKey == key / self._keySize:
					self._cacheKey = key / self._keySize
					nested_tar_fp = self._tar.extractfile('%03dXX.tgz' % (key / self._keySize))
					nested_tar_fp = BytesBuffer(gzip.GzipFile(fileobj=nested_tar_fp).read())  # 3-4x speedup for sequential access
					self._cacheTar = tarfile.open(mode='r', fileobj=nested_tar_fp)
				fullData = lmap(bytes2str, self._cacheTar.extractfile('%05d' % key).readlines())
				data = self._fmt.parse(lfilter(lambda x: not x.startswith('='), fullData),
					key_parser={None: int}, value_parser=self._map_enum2parser)
				fileList = imap(lambda x: x[1:], ifilter(lambda x: x.startswith('='), fullData))
				if DataSplitter.CommonPrefix in data:
					fileList = imap(lambda x: '%s/%s' % (data[DataSplitter.CommonPrefix], x), fileList)
				data[DataSplitter.FileList] = lmap(str.strip, fileList)
				return data
		return JobFileTarAdaptorV2(path, self._keySize)

	def _save_partition_source(self, outer_tar, meta, source, sourceLen, message):
		# Write the splitting info grouped into nested_tars
		activity = Activity(message)
		(jobnum, lastValid, nested_tar) = (-1, -1, None)
		for jobnum, partition in enumerate(source):
			if not partition.get(DataSplitter.Invalid, False):
				lastValid = jobnum
			if jobnum % self._keySize == 0:
				self._closenested_tar(outer_tar, nested_tar)
				nested_tar = self._create_nested_tar('%03dXX.tgz' % int(jobnum / self._keySize))
				activity.update('%s [%d / %d]' % (message, jobnum, sourceLen))
			# Determine shortest way to store file list
			fn_list = partition.pop(DataSplitter.FileList)
			fn_list_reduced = self._getReducedFileList(partition, fn_list)  # can modify partition
			# Write files with infos / filelist
			data = str.join('', self._fmt.format(partition, fkt=self._formatFileEntry) + lmap(lambda fn: '=%s\n' % fn, fn_list_reduced))
			self._addTonested_tar(nested_tar, '%05d' % jobnum, data)
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in partition:
				partition.pop(DataSplitter.CommonPrefix)
			partition[DataSplitter.FileList] = fn_list
		self._closenested_tar(outer_tar, nested_tar)
		activity.finish()
		# Write metadata to allow reconstruction of data splitter
		meta['MaxJobs'] = lastValid + 1
		for (fn, data) in [('Metadata', self._fmt.format(meta)), ('Version', '2')]:
			self._addToTar(outer_tar, fn, data)
