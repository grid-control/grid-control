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
from grid_control.utils import DictFormat, split_list
from grid_control.utils.activity import Activity
from grid_control.utils.file_objects import VirtualFile
from grid_control.utils.parsing import parse_bool, parse_json, parse_list
from grid_control.utils.thread_tools import GCLock
from hpfwk import AbstractError, clear_current_exception
from python_compat import BytesBuffer, bytes2str, ifilter, imap, json, lfilter, lmap, tarfile


class PartitionReader(object):
	def __init__(self, path):
		activity = Activity('Reading dataset partition file')
		self._lock = GCLock()
		self._fmt = DictFormat()
		self._tar = tarfile.open(path, 'r:')

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

	def __getitem__(self, partition_num):
		if partition_num >= self._partition_len:
			raise IndexError('Invalid dataset partition %s' % repr(partition_num))
		try:
			self._lock.acquire()
			return self._get_partition(partition_num)
		finally:
			self._lock.release()

	def get_partition_len(self):
		return self._partition_len

	def _combine_partition_parts(self, partition, fn_list):
		if DataSplitter.CommonPrefix in partition:
			fn_list = imap(lambda x: '%s/%s' % (partition[DataSplitter.CommonPrefix], x), fn_list)
		partition[DataSplitter.FileList] = lmap(str.strip, fn_list)
		return partition

	def _get_partition(self, partition_num):
		raise AbstractError


class CachingPartitionReader(PartitionReader):
	def __init__(self, path):
		PartitionReader.__init__(self, path)
		(self._cache_nested_fn, self._cache_nested_tar) = (None, None)

	def _get_nested_tar(self, nested_fn):
		if self._cache_nested_fn != nested_fn:  # caching gives 3-4x speedup for sequential access
			self._cache_nested_tar = self._open_nested_tar(nested_fn)
			self._cache_nested_fn = nested_fn
		return self._cache_nested_tar

	def _open_nested_tar(self, nested_fn):
		nested_tar_fp = self._tar.extractfile(nested_fn)
		nested_tar_fp = BytesBuffer(gzip.GzipFile(fileobj=nested_tar_fp).read())
		return tarfile.open(mode='r', fileobj=nested_tar_fp)


class TarPartitionReaderV1(CachingPartitionReader):
	# Save as outer_tar file to allow random access to mapping data with little memory overhead
	def _get_partition(self, key):
		nested_tar = self._get_nested_tar('%03dXX.tgz' % (key / 100))
		partition = self._fmt.parse(nested_tar.extractfile('%05d/info' % key).readlines(),
			key_parser={None: DataSplitter.intstr2enum}, value_parser=self._map_enum2parser)
		fn_list = lmap(bytes2str, nested_tar.extractfile('%05d/list' % key).readlines())
		return self._combine_partition_parts(partition, fn_list)


class TarPartitionReaderV2(CachingPartitionReader):
	def __init__(self, path, keySize):
		CachingPartitionReader.__init__(self, path)
		self._partition_chunk_size = keySize

	def _get_partition(self, partition_num):
		nested_tar = self._get_nested_tar('%03dXX.tgz' % (partition_num / self._partition_chunk_size))
		partition_str_list = lmap(bytes2str, nested_tar.extractfile('%05d' % partition_num).readlines())
		partition = self._fmt.parse(lfilter(lambda x: not x.startswith('='), partition_str_list),
			key_parser={None: DataSplitter.intstr2enum}, value_parser=self._map_enum2parser)
		fn_list = imap(lambda x: x[1:], ifilter(lambda x: x.startswith('='), partition_str_list))
		return self._combine_partition_parts(partition, fn_list)


class DataSplitterIOAuto(DataSplitterIO):
	def import_partition_source(self, path):
		try:
			version = int(tarfile.open(path, 'r:').extractfile('Version').read())
		except Exception:
			version = 1
		reader = DataSplitterIO.create_instance('version_%s' % version)
		return reader.import_partition_source(path)

	def save_partitions_and_info(self, progress, path, partition_iter, splitter_info_dict):
		writer = DataSplitterIOV2()
		return writer.save_partitions_and_info(progress, path, partition_iter, splitter_info_dict)


class DataSplitterIOBase(DataSplitterIO):
	def __init__(self):
		self._fmt = DictFormat()  # use a single instance to save time

	def import_partition_source(self, path):
		# Save as outer_tar file to allow random access to mapping data with little memory overhead
		try:
			return self._load_partition_source(path)
		except Exception:
			raise PartitionError("No valid dataset splitting found in '%s'." % path)

	def save_partitions_and_info(self, progress, path, partition_iter, splitter_info_dict):
		outer_tar = tarfile.open(path, 'w:')
		self._save_partitions_and_info(progress, outer_tar, partition_iter, splitter_info_dict)
		outer_tar.close()

	def _add_to_tar(self, tar, fn, data):
		info, fp = VirtualFile(fn, data).get_tar_info()
		tar.addfile(info, fp)
		fp.close()

	def _close_nested_tar(self, outer_tar, nested_tar):
		# Function to close all contained outer_tar objects
		if nested_tar:
			nested_tar.close()
			try:  # Python 3.2 does not close the wrapping gzip file object
				nested_tar.fileobj.close()  # if an external file object is given
			except Exception:
				clear_current_exception()
			nested_tar.nested_tar_fp.seek(0)
			nested_tar_info = tarfile.TarInfo(nested_tar.nested_fn)
			nested_tar_info.size = len(nested_tar.nested_tar_fp.getvalue())
			outer_tar.addfile(nested_tar_info, nested_tar.nested_tar_fp)

	def _create_nested_tar(self, fn):
		nested_tar_fp = BytesBuffer()
		nested_tar = tarfile.open(mode='w:gz', fileobj=nested_tar_fp)
		nested_tar.nested_tar_fp = nested_tar_fp
		nested_tar.nested_fn = fn
		return nested_tar

	def _format_file_entry(self, k_s_v):
		# Function to format a single file entry with metadata
		(key, separator, value) = k_s_v
		if key in [DataSplitter.Metadata, DataSplitter.MetadataHeader]:
			return (key, separator, json.dumps(value))
		elif isinstance(value, list):
			return (key, separator, str.join(',', value))
		return (key, separator, value)

	def _get_reduced_fn_list(self, partition, fn_list):
		# Determine the filenames to write (and conditionally set the common prefix in partition)
		commonprefix = os.path.commonprefix(fn_list)
		commonprefix = str.join('/', commonprefix.split('/')[:-1])
		if len(commonprefix) > 6:
			partition[DataSplitter.CommonPrefix] = commonprefix
			return lmap(lambda x: x.replace(commonprefix + '/', ''), fn_list)
		return fn_list

	def _load_partition_source(self, path):
		raise AbstractError

	def _save_partitions_and_info(self, progress, outer_tar, partition_iter, splitter_info_dict):
		raise AbstractError


class DataSplitterIOV1(DataSplitterIOBase):
	alias_list = ['version_1']

	def _load_partition_source(self, path):
		return TarPartitionReaderV1(path)

	def _save_partitions_and_info(self, progress, outer_tar, partition_iter, splitter_info_dict):
		# Write the splitting info grouped into nested_tars
		(partition_num, nested_tar) = (-1, None)
		for (partition_num, partition) in enumerate(partition_iter):
			if partition_num % 100 == 0:
				self._close_nested_tar(outer_tar, nested_tar)
				nested_tar = self._create_nested_tar('%03dXX.tgz' % int(partition_num / 100))
				progress.update_progress(partition_num)
			# Determine shortest way to store file list
			fn_list = partition.pop(DataSplitter.FileList)
			fn_list_reduced = self._get_reduced_fn_list(partition, fn_list)  # can modify partition
			# Write files with infos / filelist
			fn_data_list = [
				('list', str.join('\n', fn_list_reduced)),
				('info', self._fmt.format(partition, fkt=self._format_file_entry))
			]
			for fn, data in fn_data_list:
				self._add_to_tar(nested_tar, os.path.join('%05d' % partition_num, fn), data)
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in partition:
				partition.pop(DataSplitter.CommonPrefix)
			partition[DataSplitter.FileList] = fn_list
		self._close_nested_tar(outer_tar, nested_tar)
		# Write metadata to allow reconstruction of data splitter
		splitter_info_dict['MaxJobs'] = partition_num + 1
		self._add_to_tar(outer_tar, 'Metadata', self._fmt.format(splitter_info_dict))


class DataSplitterIOV2(DataSplitterIOBase):
	alias_list = ['version_2']

	def __init__(self):
		DataSplitterIOBase.__init__(self)
		self._partition_chunk_size = 100

	def _load_partition_source(self, path):
		return TarPartitionReaderV2(path, self._partition_chunk_size)

	def _save_partitions_and_info(self, progress, outer_tar, partition_iter, splitter_info_dict):
		# Write the splitting info grouped into nested_tars
		(partition_num, last_valid_pnum, nested_tar) = (-1, -1, None)
		for (partition_num, partition) in enumerate(partition_iter):
			if not partition.get(DataSplitter.Invalid, False):
				last_valid_pnum = partition_num
			if partition_num % self._partition_chunk_size == 0:
				self._close_nested_tar(outer_tar, nested_tar)
				nested_tar = self._create_nested_tar(
					'%03dXX.tgz' % int(partition_num / self._partition_chunk_size))
				progress.update_progress(partition_num)
			# Determine shortest way to store file list
			fn_list = partition.pop(DataSplitter.FileList)
			fn_list_reduced = self._get_reduced_fn_list(partition, fn_list)  # can modify partition
			fn_list_write = lmap(lambda fn: '=%s\n' % fn, fn_list_reduced)
			# Write files with infos / filelist
			self._add_to_tar(nested_tar, '%05d' % partition_num,
				str.join('', self._fmt.format(partition, fkt=self._format_file_entry) + fn_list_write))
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in partition:
				partition.pop(DataSplitter.CommonPrefix)
			partition[DataSplitter.FileList] = fn_list
		self._close_nested_tar(outer_tar, nested_tar)
		# Write metadata to allow reconstruction of data splitter
		splitter_info_dict['MaxJobs'] = last_valid_pnum + 1
		for (fn, data) in [('Metadata', self._fmt.format(splitter_info_dict)), ('Version', '2')]:
			self._add_to_tar(outer_tar, fn, data)
