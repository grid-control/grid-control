# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import copy
from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.splitter_base import DataSplitter, PartitionError, PartitionResyncHandler
from grid_control.utils import Result, TwoSidedIterator
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import make_enum
from python_compat import itemgetter, lmap, next, sort_inplace


# enum order matters here! (prio: "disable" overrides "complete", etc.)
ResyncMode = make_enum(['disable', 'complete', 'changed', 'ignore'])  # pylint:disable=invalid-name
ResyncMode.no_changed = [ResyncMode.disable, ResyncMode.complete, ResyncMode.ignore]
ResyncOrder = make_enum(['append', 'preserve', 'fillgap', 'reorder'])  # pylint:disable=invalid-name


class BlockResyncState(object):
	def __init__(self, block_list_old, block_list_new):
		activity = Activity('Performing resynchronization of dataset')
		block_resync_tuple = DataProvider.resync_blocks(block_list_old, block_list_new)
		(self.block_list_added, self._block_list_missing, self._block_list_matching) = block_resync_tuple
		for block_missing in self._block_list_missing:  # Files in matching blocks are already sorted
			sort_inplace(block_missing[DataProvider.FileList], key=itemgetter(DataProvider.URL))
		activity.finish()

	def get_block_change_info(self, partition):
		# Get block information (block_old, block_new, fi_list_missing, fi_list_matched)
		# which partition is based on. Search for block in missing and matched blocks
		def _get_block_key(block):
			return (block[DataProvider.Dataset], block[DataProvider.BlockName])
		partition_key = (partition[DataSplitter.Dataset], partition[DataSplitter.BlockName])
		block_missing = _fast_search(self._block_list_missing, _get_block_key, partition_key)
		if block_missing:
			return (block_missing, None, block_missing[DataProvider.FileList], [])
		# compare with old block
		return _fast_search(self._block_list_matching, lambda x: _get_block_key(x[0]), partition_key)


class DefaultPartitionResyncHandler(PartitionResyncHandler):
	def __init__(self, config):
		PartitionResyncHandler.__init__(self, config)
		# behaviour in case of event size changes
		self._mode_removed = config.get_enum('resync mode removed', ResyncMode, ResyncMode.complete,
			subset=ResyncMode.no_changed)
		self._mode_expanded = config.get_enum('resync mode expand', ResyncMode, ResyncMode.changed)
		self._mode_shrunken = config.get_enum('resync mode shrink', ResyncMode, ResyncMode.changed)
		self._mode_added = config.get_enum('resync mode added', ResyncMode, ResyncMode.complete,
			subset=[ResyncMode.complete, ResyncMode.ignore])
		# behaviour in case of metadata changes
		self._metadata_option = {}
		for metadata_name in config.get_list('resync metadata', [], on_change=None):
			self._metadata_option[metadata_name] = config.get_enum('resync mode %s' % metadata_name,
				ResyncMode, ResyncMode.complete, subset=ResyncMode.no_changed)
		# behaviour in case of job changes
		#  - disable changed jobs, preserve job number of changed jobs or reorder
		self._order = config.get_enum('resync jobs', ResyncOrder, ResyncOrder.append)

	def resync(self, splitter, reader, block_list_old, block_list_new):
		block_resync_state = BlockResyncState(block_list_old, block_list_new)
		# User overview and setup starts here
		resync_info_iter = self._iter_resync_infos(splitter, reader, block_resync_state)

		# Use reordering if setup - log interventions (disable, redo) according to proc_mode
		if self._order == ResyncOrder.fillgap:
			(partition_list_updated, partition_list_added) = _sort_resync_info_list(resync_info_iter)
			resync_info_iter = _iter_resync_infos_valid(partition_list_updated, iter(partition_list_added))
		elif self._order == ResyncOrder.reorder:
			(partition_list_updated, partition_list_added) = _sort_resync_info_list(resync_info_iter)
			tsi = TwoSidedIterator(partition_list_updated + partition_list_added)
			resync_info_iter = _iter_resync_infos_valid(tsi.forward(), tsi.backward())
		elif self._order == ResyncOrder.append:
			resync_info_iter = _iter_resync_infos_appendchange(resync_info_iter)

		return _get_resync_result(resync_info_iter)

	def _expand_outside(self, splitter, fi_idx, partition_num, size_list,
			fi_old, fi_new, block_new, partition_list_added):
		fi_list = block_new.pop(DataProvider.FileList)
		block_new[DataProvider.FileList] = [fi_new]
		iter_partitions_added = splitter.split_partitions([block_new],
			entry_first=fi_old[DataProvider.NEntries])
		for partition_added in iter_partitions_added:
			partition_added[DataSplitter.Comment] = 'src: %d [ext_1]' % partition_num
			partition_list_added.append(partition_added)
		block_new[DataProvider.FileList] = fi_list
		size_list[fi_idx] = fi_new[DataProvider.NEntries]

	def _get_metadata_setup_list(self, block_old, block_new):
		# Get list of matching metadata indices
		metadata_setup_list = []
		for (metdata_name, metadata_proc_mode) in self._metadata_option.items():
			(metadata_idx_old, metadata_idx_new) = (None, None)
			if block_old and (metdata_name in block_old.get(DataProvider.Metadata, [])):
				metadata_idx_old = block_old[DataProvider.Metadata].index(metdata_name)
			if block_new and (metdata_name in block_new.get(DataProvider.Metadata, [])):
				metadata_idx_new = block_new[DataProvider.Metadata].index(metdata_name)
			if (metadata_idx_old is not None) or (metadata_idx_new is not None):
				metadata_setup_list.append((metadata_idx_old, metadata_idx_new, metadata_proc_mode))
		return metadata_setup_list

	def _handle_changed_entries(self, splitter, fi_idx, partition_mod, partition_num, size_list,
			fi_old, fi_new, block_new, partition_list_added):
		# Process changed files in partition - returns True if file index should stay
		partition_mod[DataSplitter.Comment] += ' [changed] ' + fi_old[DataProvider.URL]
		partition_mod[DataSplitter.Comment] += (' -%d ' % fi_old[DataProvider.NEntries])
		partition_mod[DataSplitter.Comment] += (' +%d ' % fi_new[DataProvider.NEntries])

		if fi_idx == len(partition_mod[DataSplitter.FileList]) - 1:
			self._handle_changed_entries_last(splitter, fi_idx, partition_num, partition_mod, size_list,
				fi_old, fi_new, block_new, partition_list_added)

		elif fi_idx == 0:
			# First file is affected - returns True if fi_idx should stay
			return self._handle_changed_entries_first(fi_idx, partition_mod,
				size_list, fi_old, fi_new)
		else:
			# File in the middle is affected - solution very simple :)
			# Replace file - expanding files could be swapped to the (fully contained) end
			# to allow expansion via adding only the expanding part
			self._replace_complete_file(fi_idx, partition_mod, size_list, fi_old, fi_new)
			partition_mod[DataSplitter.Comment] += '[middle_add_1] '

	def _handle_changed_entries_first(self, fi_idx, partition_mod, size_list, fi_old, fi_new):
		if fi_new[DataProvider.NEntries] > partition_mod.get(DataSplitter.Skipped, 0):
			# First file changes and still lives in new partition
			following = size_list[0] - (partition_mod.get(DataSplitter.Skipped, 0) +
				partition_mod[DataSplitter.NEntries])
			shrinkage = fi_old[DataProvider.NEntries] - fi_new[DataProvider.NEntries]
			if following > 0:
				# First file not completely covered by current partition
				if following < shrinkage:
					# Covered area of first file shrinks
					partition_mod[DataSplitter.NEntries] += following
					self._replace_complete_file(fi_idx, partition_mod, size_list, fi_old, fi_new)
					partition_mod[DataSplitter.Comment] += '[first_add_1] '
				else:
					# First file changes outside of current partition
					size_list[fi_idx] = fi_new[DataProvider.NEntries]
					partition_mod[DataSplitter.Comment] = '[first_add_2] '
			else:
				# Change of first file ending in current partition - One could try to
				# 'reverse fix' expanding files to allow expansion via adding only the expanding part
				self._replace_complete_file(fi_idx, partition_mod, size_list, fi_old, fi_new)
				partition_mod[DataSplitter.Comment] += '[first_add_3] '
		else:
			# Removal of first file from current partition
			partition_mod[DataSplitter.NEntries] += max(0, size_list[fi_idx] - (
				partition_mod.get(DataSplitter.Skipped, 0) + partition_mod[DataSplitter.NEntries]))
			partition_mod[DataSplitter.NEntries] += partition_mod.get(DataSplitter.Skipped, 0)
			if DataSplitter.Skipped in partition_mod:
				partition_mod[DataSplitter.Skipped] = 0
			self._remove_complete_file(fi_idx, partition_mod, size_list, fi_old)
			return True

	def _handle_changed_entries_last(self, splitter, fi_idx, partition_num, partition_mod,
			size_list, fi_old, fi_new, block_new, partition_list_added):
		cover_last = (partition_mod.get(DataSplitter.Skipped, 0) +
			partition_mod[DataSplitter.NEntries] - sum(size_list[:-1]))
		if cover_last == fi_old[DataProvider.NEntries]:
			do_expand_outside = partition_list_added is not None
			# Change of last file, which ends in current partition
			if do_expand_outside and (fi_old[DataProvider.NEntries] < fi_new[DataProvider.NEntries]):
				self._expand_outside(splitter, fi_idx, partition_num, size_list,
					fi_old, fi_new, block_new, partition_list_added)
				partition_mod[DataSplitter.Comment] += '[last_add_1] '
			else:
				self._replace_complete_file(fi_idx, partition_mod, size_list, fi_old, fi_new)
				partition_mod[DataSplitter.Comment] += '[last_add_2] '
		elif cover_last > fi_new[DataProvider.NEntries]:
			# Change of last file, which changes current coverage
			partition_mod[DataSplitter.NEntries] -= cover_last
			partition_mod[DataSplitter.NEntries] += fi_old[DataProvider.NEntries]
			self._replace_complete_file(fi_idx, partition_mod, size_list, fi_old, fi_new)
			partition_mod[DataSplitter.Comment] += '[last_add_3] '
		else:
			# Change of last file outside of current partition
			size_list[fi_idx] = fi_new[DataProvider.NEntries]
			partition_mod[DataSplitter.Comment] += '[last_add_4] '

	def _handle_changed_file(self, splitter, proc_mode, fi_idx, partition_mod, partition_num,
			size_list, block_new, partition_list_added, fi_list_matched,
			metadata_list_new, metadata_setup_list):
		(fi_old, fi_new) = _fast_search(fi_list_matched,
			lambda x: x[0][DataProvider.URL], partition_mod[DataSplitter.FileList][fi_idx])

		if DataProvider.Metadata in fi_new:
			proc_mode = self._handle_changed_file_metadata(fi_old, fi_new,
				metadata_setup_list, metadata_list_new, proc_mode)
		if fi_old[DataProvider.NEntries] == fi_new[DataProvider.NEntries]:
			return (proc_mode, fi_idx + 1)  # go to next file
		elif fi_old[DataProvider.NEntries] * fi_new[DataProvider.NEntries] < 0:
			raise PartitionError('Unable to change %r from %d to %d entries!' % (fi_new[DataProvider.URL],
				fi_old[DataProvider.NEntries], fi_new[DataProvider.NEntries]))
		elif fi_new[DataProvider.NEntries] < 0:
			return (proc_mode, fi_idx + 1)  # go to next file

		old_props = (partition_mod[DataSplitter.NEntries], partition_mod.get(DataSplitter.Skipped))

		if not self._handle_changed_entries(splitter, fi_idx, partition_mod, partition_num,
				size_list, fi_old, fi_new, block_new, partition_list_added):
			fi_idx += 1  # False => file index should be increased

		proc_mode = self._select_changed_proc_mode(proc_mode, fi_old, fi_new, partition_mod, old_props)
		return (proc_mode, fi_idx)  # go to next file

	def _handle_changed_file_metadata(self, fi_old, fi_new,
			metadata_setup_list, metadata_list_current, proc_mode):
		metadata_list_current.append(fi_new[DataProvider.Metadata])
		for (metadata_idx_old, metadata_idx_new, metadata_proc_mode) in metadata_setup_list:
			if (metadata_idx_old is None) or (metadata_idx_new is None):
				proc_mode = min(proc_mode, metadata_proc_mode)  # Metadata was removed
			else:
				metadata_old = fi_old[DataProvider.Metadata]
				metadata_new = fi_new[DataProvider.Metadata]
				if metadata_old[metadata_idx_old] != metadata_new[metadata_idx_new]:
					proc_mode = min(proc_mode, metadata_proc_mode)  # Metadata was changed
		return proc_mode

	def _handle_removed_file(self, proc_mode, fi_idx, partition_mod, size_list, fi_removed):
		# Remove files from partition
		partition_mod[DataSplitter.Comment] += '[rm] ' + fi_removed[DataProvider.URL]
		partition_mod[DataSplitter.Comment] += '-%d ' % fi_removed[DataProvider.NEntries]

		if fi_removed[DataProvider.NEntries] > 0:
			if fi_idx == len(partition_mod[DataSplitter.FileList]) - 1:
				# Removal of last file from current partition
				partition_mod[DataSplitter.NEntries] = (sum(size_list) -
					partition_mod.get(DataSplitter.Skipped, 0))
				partition_mod[DataSplitter.Comment] += '[rm_last] '
			elif fi_idx == 0:
				# Removal of first file from current partition
				partition_mod[DataSplitter.NEntries] += max(0, size_list[fi_idx] - (
					partition_mod.get(DataSplitter.Skipped, 0) + partition_mod[DataSplitter.NEntries]))
				partition_mod[DataSplitter.NEntries] += partition_mod.get(DataSplitter.Skipped, 0)
				if DataSplitter.Skipped in partition_mod:
					partition_mod[DataSplitter.Skipped] = 0
				partition_mod[DataSplitter.Comment] += '[rm_first] '
			else:
				# File in the middle is affected - solution very simple :)
				partition_mod[DataSplitter.Comment] += '[rm_middle] '
			partition_mod[DataSplitter.NEntries] -= fi_removed[DataProvider.NEntries]

		partition_mod[DataSplitter.FileList].pop(fi_idx)
		size_list.pop(fi_idx)

		proc_mode = min(proc_mode, self._mode_removed)
		for meta in partition_mod.get(DataSplitter.MetadataHeader, []):
			proc_mode = min(proc_mode, self._metadata_option.get(meta, ResyncMode.ignore))
		return proc_mode

	def _iter_resync_infos(self, splitter, reader, block_resync_state):
		# Process partitions and yield (partition_num, partition, proc_mode) tuples
		partition_list_added_all = []
		# Perform resync of existing partitions
		for (partition_num, partition) in enumerate(reader.iter_partitions()):
			(partition_modified, proc_mode, partition_list_added) = self._resync_existing_partitions(
				splitter, block_resync_state, partition_num, partition)
			partition_list_added_all.extend(partition_list_added)
			yield (partition_num, partition, partition_modified, proc_mode)
		# Yield collected extensions of existing partitions
		for partition_added in partition_list_added_all:
			yield (None, None, partition_added, None)
		# Yield completely new partitions
		if self._mode_added == ResyncMode.complete:
			for partition_added in splitter.split_partitions(block_resync_state.block_list_added):
				yield (None, None, partition_added, None)

	def _remove_complete_file(self, fi_idx, partition_mod, size_list, fi_old):
		partition_mod[DataSplitter.NEntries] -= fi_old[DataProvider.NEntries]
		partition_mod[DataSplitter.FileList].pop(fi_idx)
		size_list.pop(fi_idx)

	def _replace_complete_file(self, fi_idx, partition_mod, size_list, fi_old, fi_new):
		partition_mod[DataSplitter.NEntries] += fi_new[DataProvider.NEntries]
		partition_mod[DataSplitter.NEntries] -= fi_old[DataProvider.NEntries]
		size_list[fi_idx] = fi_new[DataProvider.NEntries]

	def _resync_existing_partitions(self, splitter, block_resync_state, partition_num, partition):
		if DataSplitter.Comment not in partition:
			partition[DataSplitter.Comment] = 'src: %d ' % partition_num
		if partition.get(DataSplitter.Invalid, False):
			return (partition, ResyncMode.ignore, [])
		partition_modified = copy.deepcopy(partition)
		file_resync_state = block_resync_state.get_block_change_info(partition_modified)
		(proc_mode, partition_list_added) = self._resync_partition(splitter, partition_modified,
			partition_num, file_resync_state, doexpand_outside=True)
		return (partition_modified, proc_mode, partition_list_added)

	def _resync_files(self, splitter, partition_mod, partition_num, size_list,
			fi_list_missing, fi_list_matched, block_new, metadata_setup_list, partition_list_added):
		# resync a single file in the partition, return next file index to process
		# Select processing mode for job (disable > complete > changed > ignore)
		#   [ie. disable overrides all] using min
		# Result: one of [disable, complete, ignore] (changed -> complete or igore)
		fi_idx = 0
		metadata_list_current = []
		proc_mode = ResyncMode.ignore
		while fi_idx < len(partition_mod[DataSplitter.FileList]):
			fi_removed = _fast_search(fi_list_missing, itemgetter(DataProvider.URL),
				partition_mod[DataSplitter.FileList][fi_idx])
			if fi_removed:
				proc_mode = self._handle_removed_file(proc_mode, fi_idx,
					partition_mod, size_list, fi_removed)
			else:
				(proc_mode, fi_idx) = self._handle_changed_file(splitter, proc_mode, fi_idx,
					partition_mod, partition_num, size_list, block_new, partition_list_added,
					fi_list_matched, metadata_list_current, metadata_setup_list)
		return (proc_mode, metadata_list_current)

	def _resync_partition(self, splitter, partition_mod, partition_num,
			file_resync_state, doexpand_outside):
		(block_old, block_new, fi_list_missing, fi_list_matched) = file_resync_state

		# Resync single partition
		# Determine old size infos and get started
		# With doexpand_outside, gc tries to handle expanding files via the partition function
		if block_new:  # copy new location information
			partition_mod[DataSplitter.Locations] = block_new.get(DataProvider.Locations)

		size_list = _get_partition_size_list(partition_mod, block_old)
		partition_list_added = None
		if doexpand_outside:  # enable spawning new partitions if more entries are added
			partition_list_added = []
		old_entries = partition_mod[DataSplitter.NEntries]
		(proc_mode, metadata_list_current) = self._resync_files(splitter, partition_mod,
			partition_num, size_list, fi_list_missing, fi_list_matched, block_new,
			self._get_metadata_setup_list(block_old, block_new), partition_list_added)

		# Disable invalid / invalidated partitions
		if not partition_mod[DataSplitter.FileList]:
			proc_mode = ResyncMode.disable
		elif old_entries * partition_mod[DataSplitter.NEntries] <= 0:
			proc_mode = ResyncMode.disable

		if proc_mode == ResyncMode.disable:
			partition_mod[DataSplitter.Invalid] = True
			return (ResyncMode.disable, [])  # Discard extensions

		# Update metadata
		if DataSplitter.Metadata in partition_mod:
			partition_mod.pop(DataSplitter.MetadataHeader)
			partition_mod.pop(DataSplitter.Metadata)
		if metadata_list_current:
			partition_mod[DataSplitter.MetadataHeader] = block_new.get(DataProvider.Metadata)
			partition_mod[DataSplitter.Metadata] = metadata_list_current

		return (proc_mode, partition_list_added or [])

	def _select_changed_proc_mode(self, proc_mode, fi_old, fi_new, partition_mod, old_props):
		if fi_old[DataProvider.NEntries] < fi_new[DataProvider.NEntries]:
			file_proc_mode = self._mode_expanded
		else:
			file_proc_mode = self._mode_shrunken
		if file_proc_mode == ResyncMode.changed:
			file_proc_mode = ResyncMode.ignore
			if (partition_mod[DataSplitter.NEntries], partition_mod.get(DataSplitter.Skipped)) != old_props:
				file_proc_mode = ResyncMode.complete
		return min(proc_mode, file_proc_mode)


def _convert_resync_info_iter(resync_info_iter, pnum_list_redo, pnum_list_disable):
	# Iter partitions and fill pnum lists for existing partitions according to processing mode
	for (partition_num, _, partition, proc_mode) in resync_info_iter:
		if partition_num:  # already existing partition
			if proc_mode == ResyncMode.complete:
				pnum_list_redo.append(partition_num)
			elif proc_mode == ResyncMode.disable:
				pnum_list_disable.append(partition_num)
		yield partition


def _fast_search(collection, key_fun, key):
	(idx, idx_high) = (0, len(collection))
	while idx < idx_high:
		idx_mid = int((idx + idx_high) / 2)
		if key_fun(collection[idx_mid]) < key:
			idx = idx_mid + 1
		else:
			idx_high = idx_mid
	if (idx < len(collection)) and (key_fun(collection[idx]) == key):
		return collection[idx]


def _get_partition_size_list(partition, block_old):
	# Get list of work units for each file in the partition
	def _get_entries_for_url(url):
		fi = _fast_search(block_old[DataProvider.FileList], itemgetter(DataProvider.URL), url)
		if not fi:
			raise Exception('url %s not found in block %s\n%s' % (url, block_old, partition))
		return fi[DataProvider.NEntries]
	return lmap(_get_entries_for_url, partition[DataSplitter.FileList])


def _get_resync_result(resync_info_iter):
	resync_result = Result(pnum_list_redo=[], pnum_list_disable=[])
	resync_result.partition_iter = _convert_resync_info_iter(resync_info_iter,
		resync_result.pnum_list_redo, resync_result.pnum_list_disable)
	return resync_result


def _iter_resync_infos_appendchange(resync_info_iter):
	partition_list_changed = []
	for (partition_num, old_partition, partition, proc_mode) in resync_info_iter:
		if proc_mode == ResyncMode.complete:
			# add modified partition to list of changed partitions
			partition_list_changed.append(partition)
			# replace current partition with a fresh copy that is marked as invalid
			partition = copy.copy(old_partition)
			partition[DataSplitter.Invalid] = True
			proc_mode = ResyncMode.disable
		yield (partition_num, None, partition, proc_mode)
	for partition in partition_list_changed:
		yield (None, None, partition, None)


def _iter_resync_infos_valid(resync_info_iter, resync_info_iter_alt):
	# yield valid resync infos from resync_info_iter and resync_info_iter_alt
	# invalid or disabled resync_infos from resync_info_iter are replaced by
	# valid resync_infos from resync_info_iter_alt
	for (partition_num, _, partition, proc_mode) in resync_info_iter:
		if (proc_mode == ResyncMode.disable) or partition.get(DataSplitter.Invalid, False):
			resync_info_added = next(resync_info_iter_alt, None)
			while resync_info_added and resync_info_added[2].get(DataSplitter.Invalid, False):
				resync_info_added = next(resync_info_iter_alt, None)
			if resync_info_added:
				yield (partition_num, None, resync_info_added[2], ResyncMode.complete)
				continue  # Overwrite invalid partitions
		yield (partition_num, None, partition, proc_mode)
	for resync_info_added in resync_info_iter_alt:  # deplete resync_info_iter_alt at the end
		yield (None, None, resync_info_added[2], None)


def _sort_resync_info_list(resync_info_iter):
	# Sort resynced partitions into updated and added lists (and discard old partition info)
	(partition_list_updated, partition_list_added) = ([], [])
	for (partition_num, _, partition, proc_mode) in resync_info_iter:
		if partition_num is None:  # Separate existing and new partitions
			partition_list_added.append((None, None, partition, None))
		else:
			partition_list_updated.append((partition_num, None, partition, proc_mode))
	return (partition_list_updated, partition_list_added)
