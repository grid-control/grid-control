# | Copyright 2013-2017 Karlsruhe Institute of Technology
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
from grid_control.datasets.splitter_base import DataSplitter, PartitionReader, PartitionWriter
from grid_control.utils import DictFormat
from grid_control.utils.activity import Activity
from grid_control.utils.file_tools import VirtualFile
from grid_control.utils.parsing import parse_bool, parse_json, parse_list
from hpfwk import AbstractError, NestedException, clear_current_exception, ignore_exception
from python_compat import BytesBuffer, bytes2str, ifilter, imap, json, lmap, tarfile


class PartitionReaderError(NestedException):
	pass


class FilePartitionReader(PartitionReader):
	def __init__(self, path, partition_len=None):
		PartitionReader.__init__(self, partition_len)


class TrivialPartitionReader(PartitionReader):
	def __init__(self, partition_iter):
		self._partition_data = list(partition_iter)
		PartitionReader.__init__(self, len(self._partition_data))

	def get_partition_unchecked(self, partition_num):
		return self._partition_data[partition_num]


class TarPartitionWriter(PartitionWriter):
	def __init__(self):
		PartitionWriter.__init__(self)
		self._fmt = DictFormat()  # use a single instance to save time

	def save_partitions(self, path, partition_iter, progress=None):
		outer_tar = tarfile.open(path, 'w:')
		self._save_partitions(outer_tar, partition_iter, progress)
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

	def _get_reduced_url_list(self, partition, url_list):
		# Determine the filenames to write (and conditionally set the common prefix in partition)
		commonprefix = os.path.commonprefix(url_list)
		commonprefix = str.join('/', commonprefix.split('/')[:-1])
		if len(commonprefix) > 6:
			partition[DataSplitter.CommonPrefix] = commonprefix
			return lmap(lambda x: x.replace(commonprefix + '/', ''), url_list)
		return url_list

	def _save_partitions(self, outer_tar, partition_iter, progress):
		raise AbstractError


class AutoPartitionReader(FilePartitionReader):
	alias_list = ['auto']

	def __new__(cls, path):
		version = ignore_exception(Exception, 1,
			lambda: int(tarfile.open(path, 'r:').extractfile('Version').read()))
		return FilePartitionReader.create_instance('version_%s' % version, path)


class TarPartitionReader(FilePartitionReader):
	def __init__(self, path):
		activity = Activity('Reading dataset partition file')
		self._fmt = DictFormat()
		try:
			self._tar = tarfile.open(path, 'r:')

			metadata = self._fmt.parse(self._tar.extractfile('Metadata').readlines(), key_parser={None: str})
			FilePartitionReader.__init__(self, path, metadata.pop('MaxJobs'))
			self._metadata = metadata
			activity.finish()
		except Exception:
			raise PartitionReaderError('No valid dataset splitting found in %s' % path)

		self._map_enum2parser = {
			None: str,
			DataSplitter.NEntries: int, DataSplitter.Skipped: int,
			DataSplitter.Invalid: parse_bool,
			DataSplitter.Locations: lambda x: parse_list(x, ','),
			DataSplitter.MetadataHeader: parse_json,
			DataSplitter.Metadata: lambda x: parse_json(x.strip("'"))
		}
		(self._cache_nested_fn, self._cache_nested_tar) = (None, None)

	def _combine_partition_parts(self, partition, url_list):
		if DataSplitter.CommonPrefix in partition:
			common_prefix = partition.pop(DataSplitter.CommonPrefix)
			url_list = imap(lambda x: '%s/%s' % (common_prefix, x), url_list)
		partition[DataSplitter.FileList] = lmap(str.strip, url_list)
		return partition

	def _get_nested_tar(self, nested_fn):
		if self._cache_nested_fn != nested_fn:  # caching gives 3-4x speedup for sequential access
			self._cache_nested_tar = self._open_nested_tar(nested_fn)
			self._cache_nested_fn = nested_fn
		return self._cache_nested_tar

	def _open_nested_tar(self, nested_fn):
		nested_tar_fp = self._tar.extractfile(nested_fn)
		nested_tar_fp = BytesBuffer(gzip.GzipFile(fileobj=nested_tar_fp).read())
		return tarfile.open(mode='r', fileobj=nested_tar_fp)


class TarPartitionWriterV1(TarPartitionWriter):
	alias_list = ['version_1']

	def _save_partitions(self, outer_tar, partition_iter, progress):
		# Write the splitting info grouped into nested_tars
		(partition_num, nested_tar) = (-1, None)
		for (partition_num, partition) in enumerate(partition_iter):
			if partition_num % 100 == 0:
				self._close_nested_tar(outer_tar, nested_tar)
				nested_tar = self._create_nested_tar('%03dXX.tgz' % int(partition_num / 100))
				if progress:
					progress.update_progress(partition_num)
			# Determine shortest way to store file list
			url_list = partition.pop(DataSplitter.FileList)
			url_list_reduced = self._get_reduced_url_list(partition, url_list)  # can modify partition
			# Write files with infos / filelist
			fn_data_list = [
				('list', str.join('\n', url_list_reduced)),
				('info', self._fmt.format(partition, fkt=self._format_file_entry))
			]
			for fn, data in fn_data_list:
				self._add_to_tar(nested_tar, os.path.join('%05d' % partition_num, fn), data)
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in partition:
				partition.pop(DataSplitter.CommonPrefix)
			partition[DataSplitter.FileList] = url_list
		self._close_nested_tar(outer_tar, nested_tar)
		# Write metadata to allow reconstruction of data splitter
		splitter_info_dict = {'MaxJobs': partition_num + 1, 'ClassName': 'BlockBoundarySplitter'}
		self._add_to_tar(outer_tar, 'Metadata', self._fmt.format(splitter_info_dict))


class TarPartitionWriterV2(TarPartitionWriter):
	alias_list = ['version_2', 'auto']

	def __init__(self):
		TarPartitionWriter.__init__(self)
		self._partition_chunk_size = 100

	def _save_partitions(self, outer_tar, partition_iter, progress):
		# Write the splitting info grouped into nested_tars
		(partition_num, last_valid_pnum, nested_tar) = (-1, -1, None)
		for (partition_num, partition) in enumerate(partition_iter):
			if not partition.get(DataSplitter.Invalid, False):
				last_valid_pnum = partition_num
			if partition_num % self._partition_chunk_size == 0:
				self._close_nested_tar(outer_tar, nested_tar)
				nested_tar = self._create_nested_tar(
					'%03dXX.tgz' % int(partition_num / self._partition_chunk_size))
				if progress:
					progress.update_progress(partition_num)
			# Determine shortest way to store file list
			url_list = partition.pop(DataSplitter.FileList)
			url_list_reduced = self._get_reduced_url_list(partition, url_list)  # can modify partition
			url_list_write = lmap(lambda url: '=%s\n' % url, url_list_reduced)
			# Write files with infos / filelist
			self._add_to_tar(nested_tar, '%05d' % partition_num,
				str.join('', self._fmt.format(partition, fkt=self._format_file_entry) + url_list_write))
			# Remove common prefix from info
			if DataSplitter.CommonPrefix in partition:
				partition.pop(DataSplitter.CommonPrefix)
			partition[DataSplitter.FileList] = url_list
		self._close_nested_tar(outer_tar, nested_tar)
		# Write metadata to allow reconstruction of data splitter
		metadata_data = 'MaxJobs=%d\nClassName=BlockBoundarySplitter' % (last_valid_pnum + 1)
		for (fn, data) in [('Metadata', metadata_data), ('Version', '2')]:
			self._add_to_tar(outer_tar, fn, data)


class TarPartitionReaderV1(TarPartitionReader):
	alias_list = ['version_1']

	def get_partition_unchecked(self, partition_num):
		# Save as outer_tar file to allow random access to mapping data with little memory overhead
		nested_tar = self._get_nested_tar('%03dXX.tgz' % (partition_num / 100))
		partition = self._fmt.parse(nested_tar.extractfile('%05d/info' % partition_num).readlines(),
			key_parser={None: DataSplitter.intstr2enum}, value_parser=self._map_enum2parser)
		url_list = lmap(bytes2str, nested_tar.extractfile('%05d/list' % partition_num).readlines())
		return self._combine_partition_parts(partition, url_list)


class TarPartitionReaderV2(TarPartitionReader):
	alias_list = ['version_2', 'auto']

	def __init__(self, path):
		TarPartitionReader.__init__(self, path)
		self._partition_chunk_size = self._metadata.pop('ChunkSize', 100)

	def get_partition_unchecked(self, partition_num):
		nested_tar = self._get_nested_tar('%03dXX.tgz' % (partition_num / self._partition_chunk_size))
		partition_str_list = lmap(bytes2str, nested_tar.extractfile('%05d' % partition_num).readlines())
		partition = self._fmt.parse(ifilter(lambda x: not x.startswith('='), partition_str_list),
			key_parser={None: DataSplitter.intstr2enum}, value_parser=self._map_enum2parser)
		url_list = imap(lambda x: x[1:], ifilter(lambda x: x.startswith('='), partition_str_list))
		return self._combine_partition_parts(partition, url_list)
