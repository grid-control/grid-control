# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

import os, copy
from grid_control import utils
from grid_control.config import create_config
from grid_control.datasets.provider_base import DataProvider
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import make_enum
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import imap, irange, itemgetter, lmap, next, sort_inplace, unspecified


# prio: "disable" overrides "complete", etc.
ResyncMode = make_enum(['disable', 'complete', 'changed', 'ignore'])
ResyncMode.noChanged = [ResyncMode.disable, ResyncMode.complete, ResyncMode.ignore]
ResyncOrder = make_enum(['append', 'preserve', 'fillgap', 'reorder']) # reorder mechanism


def _fast_search(lst, key_fun, key):
	(idx, hi) = (0, len(lst))
	while idx < hi:
		mid = int((idx + hi) / 2)
		if key_fun(lst[mid]) < key:
			idx = mid + 1
		else:
			hi = mid
	if (idx < len(lst)) and (key_fun(lst[idx]) == key):
		return lst[idx]


class PartitionError(NestedException):
	pass


def _sort_resync_info_list(partition_iter):
	# Sort resynced partitions into updated and added lists
	(partition_list_updated, partition_list_added) = ([], [])
	for (partition_num, partition, proc_mode) in partition_iter:
		if partition_num is None: # Separate existing and new partitions
			partition_list_added.append((None, partition, None))
		else:
			partition_list_updated.append((partition_num, partition, proc_mode))
	return (partition_list_updated, partition_list_added)


def _iter_fixed_resync_infos(resync_info_iter, resync_info_iter_alt):
	# yield valid resync infos from resync_info_iter and resync_info_iter_alt
	# invalid or disabled resync_infos from resync_info_iter are replaced by
	# valid resync_infos from resync_info_iter_alt
	for (partition_num, partition, proc_mode) in resync_info_iter:
		if (proc_mode == ResyncMode.disable) or partition.get(DataSplitter.Invalid, False):
			resync_info_added = next(resync_info_iter_alt, None)
			while resync_info_added and resync_info_added[1].get(DataSplitter.Invalid, False):
				resync_info_added = next(resync_info_iter_alt, None)
			if resync_info_added:
				yield (partition_num, resync_info_added[1], ResyncMode.complete) # Overwrite invalid partitions
				continue
		yield (partition_num, partition, proc_mode)
	for resync_info_added in resync_info_iter_alt: # deplete resync_info_iter_alt at the end
		# ResyncMode.ignore -> do nothing special for the new partitions
		yield (None, resync_info_added[1], ResyncMode.ignore)


class DataSplitter(ConfigurablePlugin):
	def __init__(self, config, datasource_name):
		ConfigurablePlugin.__init__(self, config)
		self._datasource_name = datasource_name
		self.set_state(partition_source=None, config_protocol={})
		# Resync settings:
		self._interactive = config.isInteractive(['partition resync', '%s partition resync' % datasource_name], False)
		#   behaviour in case of event size changes
		self._mode_removed = config.getEnum('resync mode removed', ResyncMode, ResyncMode.complete, subset = ResyncMode.noChanged)
		self._mode_expanded = config.getEnum('resync mode expand', ResyncMode, ResyncMode.changed)
		self._mode_shrunken = config.getEnum('resync mode shrink', ResyncMode, ResyncMode.changed)
		self._mode_added = config.getEnum('resync mode added', ResyncMode, ResyncMode.complete, subset = [ResyncMode.complete, ResyncMode.ignore])
		#   behaviour in case of metadata changes
		self._metadata_resync_option = {}
		for metadata_name in config.getList('resync metadata', [], onChange = None):
			self._metadata_resync_option[metadata_name] = config.getEnum('resync mode %s' % metadata_name,
				ResyncMode, ResyncMode.complete, subset = ResyncMode.noChanged)
		#   behaviour in case of job changes - disable changed jobs, preserve job number of changed jobs or reorder
		self._resync_order = config.getEnum('resync jobs', ResyncOrder, ResyncOrder.append)
		self._configure_splitter(config)

		self._dp_ds_prop_list = []
		for prop in ['Dataset', 'BlockName', 'Nickname', 'Locations']:
			self._dp_ds_prop_list.append((getattr(DataProvider, prop), getattr(DataSplitter, prop)))

	def get_needed_enums(cls):
		return [DataSplitter.FileList]
	get_needed_enums = classmethod(get_needed_enums)

	def get_partition(self, partition_num):
		if partition_num >= self.get_partition_len():
			raise PartitionError('Job %d out of range for available dataset' % partition_num)
		return self._partition_source[partition_num]

	def get_partition_len(self):
		return self._partition_source.maxJobs

	def import_partitions(self, path):
		splitter_io = DataSplitterIO.create_instance('DataSplitterIOAuto')
		self._partition_source = splitter_io.import_partition_source(path)

	def iter_partitions(self):
		for partition_num in irange(self.get_partition_len()):
			yield self._partition_source[partition_num]

	def load_partitions_for_script(path, config = None):
		partition_source = DataSplitterIO.create_instance('DataSplitterIOAuto').import_partition_source(path)
		# Transfer config protocol (in case no split function is called)
		config_protocol = {}
		for (section, options) in partition_source.metadata.items():
			section = section.replace('dataset', '').strip()
			for (option, value) in options.items():
				if section:
					option = '[%s] %s' % (section, option)
				config_protocol[option.strip()] = value
				if config is not None:
					config.set(option, str(value))
		# Create and setup splitter
		if config is None:
			config = create_config(configDict = partition_source.metadata)
		splitter = DataSplitter.create_instance(partition_source.classname, config, 'dataset')
		splitter.set_state(partition_source, config_protocol)
		return splitter
	load_partitions_for_script = staticmethod(load_partitions_for_script)

	def partition_blocks(self, path, blocks):
		activity = Activity('Splitting dataset into jobs')
		self.save_partitions(path, self.partition_blocks_raw(blocks))
		self.import_partitions(path)
		activity.finish()

	def partition_blocks_raw(self, blocks, event_first = 0):
		raise AbstractError

	def resync_partitions(self, path, block_list_old, block_list_new):
		activity = Activity('Performing resynchronization of dataset')
		(block_list_added, block_list_missing, block_list_matching) = DataProvider.resyncSources(block_list_old, block_list_new)
		for block_missing in block_list_missing: # Files in matching blocks are already sorted
			sort_inplace(block_missing[DataProvider.FileList], key = itemgetter(DataProvider.URL))
		activity.finish()

		# User overview and setup starts here
		resync_info_iter = self._iter_resync_infos(block_list_added, block_list_missing, block_list_matching)
		result_redo = []
		result_disable = []
		path_tmp = path + '.tmp'
		resync_partition_iter = self._iter_resync_partitions(resync_info_iter, result_redo, result_disable)
		self.save_partitions(path_tmp, resync_partition_iter, partition_len_hint = self.get_partition_len(),
			message = 'Performing resynchronization of dataset map (progress is estimated)')

		if self._interactive:
			# TODO: print info and ask
			if not utils.get_user_bool('Do you want to use the new dataset partition?', False):
				return
		os.rename(path_tmp, path)
		return (result_redo, result_disable)

	def save_partitions(self, path, partition_iter = None, partition_len_hint = None, message = 'Writing job mapping file'):
		# Save as tar file to allow random access to mapping data with little memory overhead
		if partition_iter and not partition_len_hint:
			partition_iter = list(partition_iter)
			partition_len_hint = len(partition_iter)
		elif not partition_iter:
			(partition_iter, partition_len_hint) = (self._partition_source, self.get_partition_len())
		# Write splitter_info_dict to allow reconstruction of data splitter
		splitter_info_dict_dict = {'ClassName': self.__class__.__name__}
		splitter_info_dict_dict.update(self._config_protocol)
		splitter_io = DataSplitterIO.create_instance('DataSplitterIOAuto')
		splitter_io.save_partition_source(path, splitter_info_dict_dict, partition_iter, partition_len_hint, message)

	def set_state(self, partition_source, config_protocol):
		self._partition_source = partition_source
		self._config_protocol = config_protocol

	def _configure_splitter(self, config):
		pass

	def _finish_partition(self, block, partition, fi_list = None):
		# Copy infos from block
		for (dp_prop, ds_prop) in self._dp_ds_prop_list:
			if dp_prop in block:
				partition[ds_prop] = block[dp_prop]
		if DataProvider.Metadata in block:
			partition[DataSplitter.MetadataHeader] = block[DataProvider.Metadata]
		# Helper for very simple splitter
		if fi_list:
			partition[DataSplitter.FileList] = lmap(itemgetter(DataProvider.URL), fi_list)
			partition[DataSplitter.NEntries] = sum(imap(itemgetter(DataProvider.NEntries), fi_list))
			if DataProvider.Metadata in block:
				partition[DataSplitter.Metadata] = lmap(itemgetter(DataProvider.Metadata), fi_list)
		return partition

	def _query_config(self, config_fun, option, default = unspecified):
		config_key = (config_fun, option, default)
		self._setup(config_key, {}) # query once for init
		return config_key

	def _resync_handle_changed_file(self, proc_mode, fi_idx, partition_mod, partition_num, size_list, block_new,
			partition_list_added, fi_old, fi_new, metadata_list_new, metadata_setup_list):
		if DataProvider.Metadata in fi_new:
			proc_mode = self._resync_handle_changed_file_metadata(fi_old, fi_new, metadata_setup_list, metadata_list_new, proc_mode)
		if fi_old[DataProvider.NEntries] == fi_new[DataProvider.NEntries]:
			return (proc_mode, fi_idx + 1) # go to next file
		elif fi_old[DataProvider.NEntries] * fi_new[DataProvider.NEntries] < 0:
			raise PartitionError('Unable to change %r from %d to %d entries!' % (fi_new[DataProvider.LFN],
				fi_old[DataProvider.NEntries], fi_new[DataProvider.NEntries]))
		elif fi_new[DataProvider.NEntries] < 0:
			return (proc_mode, fi_idx + 1) # go to next file
		old_entries = partition_mod[DataSplitter.NEntries]
		old_skip = partition_mod.get(DataSplitter.Skipped)

		if self._resync_handle_changed_file_entries(fi_idx, partition_mod, partition_num, size_list, fi_old, fi_new, block_new, partition_list_added):
			fi_idx += 1 # True => file index should be increased

		mode = utils.QM(fi_old[DataProvider.NEntries] < fi_new[DataProvider.NEntries], self._mode_expanded, self._mode_shrunken)
		if mode == ResyncMode.changed:
			mode = ResyncMode.ignore
			if (old_entries != partition_mod[DataSplitter.NEntries]) or (old_skip != partition_mod.get(DataSplitter.Skipped)):
				mode = ResyncMode.complete
		proc_mode = min(proc_mode, mode)
		return (proc_mode, fi_idx) # go to next file

	def _resync_handle_changed_file_entries(self, fi_idx, partition_mod, partition_num, size_list, fi_old, fi_new, block_new, partition_list_added):
		# Process changed files in partition - returns True if file index should be increased
		partition_mod[DataSplitter.Comment] += ' [changed] ' + fi_old[DataProvider.URL]
		partition_mod[DataSplitter.Comment] += (' -%d ' % fi_old[DataProvider.NEntries])
		partition_mod[DataSplitter.Comment] += (' +%d ' % fi_new[DataProvider.NEntries])

		def expand_outside():
			fi_list = block_new.pop(DataProvider.FileList)
			block_new[DataProvider.FileList] = [fi_new]
			for extSplit in self.partition_blocks_raw([block_new], fi_old[DataProvider.NEntries]):
				extSplit[DataSplitter.Comment] = 'src: %d [ext_1]' % partition_num
				partition_list_added.append(extSplit)
			block_new[DataProvider.FileList] = fi_list
			size_list[fi_idx] = fi_new[DataProvider.NEntries]

		def remove_complete_file():
			partition_mod[DataSplitter.NEntries] -= fi_old[DataProvider.NEntries]
			partition_mod[DataSplitter.FileList].pop(fi_idx)
			size_list.pop(fi_idx)

		def replace_complete_file():
			partition_mod[DataSplitter.NEntries] += fi_new[DataProvider.NEntries]
			partition_mod[DataSplitter.NEntries] -= fi_old[DataProvider.NEntries]
			size_list[fi_idx] = fi_new[DataProvider.NEntries]

		if fi_idx == len(partition_mod[DataSplitter.FileList]) - 1:
			cover_last = partition_mod.get(DataSplitter.Skipped, 0) + partition_mod[DataSplitter.NEntries] - sum(size_list[:-1])
			if cover_last == fi_old[DataProvider.NEntries]:
				# Change of last file, which ends in current partition
				if (partition_list_added is not None) and (fi_old[DataProvider.NEntries] < fi_new[DataProvider.NEntries]):
					expand_outside()
					partition_mod[DataSplitter.Comment] += '[last_add_1] '
				else:
					replace_complete_file()
					partition_mod[DataSplitter.Comment] += '[last_add_2] '
			elif cover_last > fi_new[DataProvider.NEntries]:
				# Change of last file, which changes current coverage
				partition_mod[DataSplitter.NEntries] -= cover_last
				partition_mod[DataSplitter.NEntries] += fi_old[DataProvider.NEntries]
				replace_complete_file()
				partition_mod[DataSplitter.Comment] += '[last_add_3] '
			else:
				# Change of last file outside of current partition
				size_list[fi_idx] = fi_new[DataProvider.NEntries]
				partition_mod[DataSplitter.Comment] += '[last_add_4] '

		elif fi_idx == 0:
			# First file is affected
			if fi_new[DataProvider.NEntries] > partition_mod.get(DataSplitter.Skipped, 0):
				# First file changes and still lives in new partition
				following = size_list[0] - partition_mod.get(DataSplitter.Skipped, 0) - partition_mod[DataSplitter.NEntries]
				shrinkage = fi_old[DataProvider.NEntries] - fi_new[DataProvider.NEntries]
				if following > 0:
					# First file not completely covered by current partition
					if following < shrinkage:
						# Covered area of first file shrinks
						partition_mod[DataSplitter.NEntries] += following
						replace_complete_file()
						partition_mod[DataSplitter.Comment] += '[first_add_1] '
					else:
						# First file changes outside of current partition
						size_list[fi_idx] = fi_new[DataProvider.NEntries]
						partition_mod[DataSplitter.Comment] = '[first_add_2] '
				else:
					# Change of first file ending in current partition - One could try to
					# 'reverse fix' expanding files to allow expansion via adding only the expanding part
					replace_complete_file()
					partition_mod[DataSplitter.Comment] += '[first_add_3] '
			else:
				# Removal of first file from current partition
				partition_mod[DataSplitter.NEntries] += max(0, size_list[fi_idx] - partition_mod.get(DataSplitter.Skipped, 0) - partition_mod[DataSplitter.NEntries])
				partition_mod[DataSplitter.NEntries] += partition_mod.get(DataSplitter.Skipped, 0)
				if DataSplitter.Skipped in partition_mod:
					partition_mod[DataSplitter.Skipped] = 0
				remove_complete_file()
				return False

		else:
			# File in the middle is affected - solution very simple :)
			# Replace file - expanding files could be swapped to the (fully contained) end
			# to allow expansion via adding only the expanding part
			replace_complete_file()
			partition_mod[DataSplitter.Comment] += '[middle_add_1] '
		return True

	def _resync_handle_changed_file_metadata(self, fi_old, fi_new, metadata_setup_list, metadata_list_added, proc_mode):
		metadata_list_added.append(fi_new[DataProvider.Metadata])
		for (metadata_idx_old, metadata_idx_new, metadata_proc_mode) in metadata_setup_list:
			if (metadata_idx_old is None) or (metadata_idx_new is None):
				proc_mode = min(proc_mode, metadata_proc_mode) # Metadata was removed
			elif fi_old[DataProvider.Metadata][metadata_idx_old] != fi_new[DataProvider.Metadata][metadata_idx_new]:
				proc_mode = min(proc_mode, metadata_proc_mode) # Metadata was changed
		return proc_mode

	def _resync_existing_partitions(self, partition_num, partition, block_list_added, block_list_missing, block_list_matching):
		if DataSplitter.Comment not in partition:
			partition[DataSplitter.Comment] = 'src: %d ' % partition_num
		if partition.get(DataSplitter.Invalid, False):
			return (partition, ResyncMode.ignore, [])
		partition_modified = copy.deepcopy(partition)
		(block_old, block_new, fi_list_missing, fi_list_matched) = self._get_block_change_info_for_partition(partition_modified, block_list_missing, block_list_matching)
		(proc_mode, partition_list_added) = self._resync_partition(partition_modified, partition_num, block_old, block_new, fi_list_missing, fi_list_matched, doexpand_outside = True)
		return (partition_modified, proc_mode, partition_list_added)

	def _resync_files(self, partition_mod, partition_num, size_list, fi_list_missing, fi_list_matched, block_new, metadata_setup_list, partition_list_added):
		# resync a single file in the partition, return next file index to process
		# Select processing mode for job (disable > complete > changed > ignore) [ie. disable overrides all] using min
		# Result: one of [disable, complete, ignore] (changed -> complete or igore)
		fi_idx = 0
		metadata_list_added = []
		proc_mode = ResyncMode.ignore
		while fi_idx < len(partition_mod[DataSplitter.FileList]):
			fi_removed = _fast_search(fi_list_missing, itemgetter(DataProvider.URL), partition_mod[DataSplitter.FileList][fi_idx])
			if fi_removed:
				proc_mode = min(proc_mode, self._resync_handle_removed_file(fi_idx, partition_mod, size_list, fi_removed))
			else:
				(fi_old, fi_new) = _fast_search(fi_list_matched, lambda x: x[0][DataProvider.URL], partition_mod[DataSplitter.FileList][fi_idx])
				(proc_mode, fi_idx) = self._resync_handle_changed_file(proc_mode, fi_idx, partition_mod, partition_num, size_list, block_new, partition_list_added, fi_old, fi_new, metadata_list_added, metadata_setup_list)
		return (proc_mode, metadata_list_added)

	def _get_block_change_info_for_partition(self, partition, block_list_missing, block_list_matching):
		# Get block information (block_old, block_new, fi_list_missing, fi_list_matched) which partition is based on
		# Search for block in missing and matched blocks
		def get_block_key(block):
			return (block[DataProvider.Dataset], block[DataProvider.BlockName])
		partition_key = (partition[DataSplitter.Dataset], partition[DataSplitter.BlockName])
		block_missing = _fast_search(block_list_missing, get_block_key, partition_key)
		if block_missing:
			return (block_missing, None, block_missing[DataProvider.FileList], [])
		return _fast_search(block_list_matching, lambda x: get_block_key(x[0]), partition_key) # compare with old block

	def _resync_get_metadata_setup_list(self, block_old, block_new):
		# Get list of matching metadata indices
		metadata_setup_list = []
		for (metdata_name, metadata_proc_mode) in self._metadata_resync_option.items():
			(metadata_idx_old, metadata_idx_new) = (None, None)
			if block_old and (metdata_name in block_old.get(DataProvider.Metadata, [])):
				metadata_idx_old = block_old[DataProvider.Metadata].index(metdata_name)
			if block_new and (metdata_name in block_new.get(DataProvider.Metadata, [])):
				metadata_idx_new = block_new[DataProvider.Metadata].index(metdata_name)
			if (metadata_idx_old is not None) or (metadata_idx_new is not None):
				metadata_setup_list.append((metadata_idx_old, metadata_idx_new, metadata_proc_mode))
		return metadata_setup_list

	def _iter_resync_partitions(self, resync_info_iter, result_redo, result_disable):
		# Use reordering if setup - log interventions (disable, redo) according to proc_mode
		if self._resync_order == ResyncOrder.fillgap:
			(partition_list_updated, partition_list_added) = _sort_resync_info_list(resync_info_iter)
			resync_info_iter = _iter_fixed_resync_infos(partition_list_updated, iter(partition_list_added))
		elif self._resync_order == ResyncOrder.reorder:
			(partition_list_updated, partition_list_added) = _sort_resync_info_list(resync_info_iter)
			# partition_list_added.reverse()
			tsi = utils.TwoSidedIterator(partition_list_updated + partition_list_added)
			resync_info_iter = _iter_fixed_resync_infos(tsi.forward(), tsi.backward())

		try:
			for (partition_num, partition, proc_mode) in resync_info_iter:
				if partition_num:
					if proc_mode == ResyncMode.complete:
						result_redo.append(partition_num)
					elif proc_mode == ResyncMode.disable:
						result_disable.append(partition_num)
				yield partition
		except Exception:
			raise PartitionError('Unable to resync %r' % self._datasource_name)

	def _iter_resync_infos(self, block_list_added, block_list_missing, block_list_matching):
		# Process partitions
		partition_list_added_all = []
		# Perform resync of existing partitions
		for (partition_num, partition) in enumerate(self.iter_partitions()):
			(partition_modified, proc_mode, partition_list_added) = self._resync_existing_partitions(partition_num, partition, block_list_added, block_list_missing, block_list_matching)
			if (self._resync_order == ResyncOrder.append) and (proc_mode == ResyncMode.complete):
				partition_list_added_all.append(partition_modified) # add modified partition to list of new partitions
				partition_modified = copy.copy(partition) # replace current partition with a fresh copy that is marked as invalid
				partition_modified[DataSplitter.Invalid] = True
				proc_mode = ResyncMode.disable
			partition_list_added_all.extend(partition_list_added)
			yield (partition_num, partition_modified, proc_mode)
		# Yield collected extensions of existing partitions
		for partition_added in partition_list_added_all:
			yield (None, partition_added, ResyncMode.ignore)
		# Yield completely new partitions
		if self._mode_added == ResyncMode.complete:
			for partition_added in self.partition_blocks_raw(block_list_added):
				yield (None, partition_added, ResyncMode.ignore)

	def _resync_partition(self, partition_mod, partition_num, block_old, block_new, fi_list_missing, fi_list_matched, doexpand_outside):
		# Resync single partition
		# With doexpand_outside, gc tries to handle expanding files via the partition function
		if block_new: # copy new location information
			partition_mod[DataSplitter.Locations] = block_new.get(DataProvider.Locations)
		# Determine old size infos and get started
		def search_url(url):
			return _fast_search(block_old[DataProvider.FileList], itemgetter(DataProvider.URL), url)
		size_list = lmap(lambda url: search_url(url)[DataProvider.NEntries], partition_mod[DataSplitter.FileList])
		metadata_setup_list = self._resync_get_metadata_setup_list(block_old, block_new)

		partition_list_added = utils.QM(doexpand_outside, [], None)
		old_entries = partition_mod[DataSplitter.NEntries]
		(proc_mode, metadata_list_added) = self._resync_files(partition_mod, partition_num, size_list,
			fi_list_missing, fi_list_matched, block_new, metadata_setup_list, partition_list_added)
		# Disable invalid / invalidated partitions
		if (len(partition_mod[DataSplitter.FileList]) == 0) or (old_entries * partition_mod[DataSplitter.NEntries] <= 0):
			proc_mode = ResyncMode.disable

		if proc_mode == ResyncMode.disable:
			partition_mod[DataSplitter.Invalid] = True
			return (ResyncMode.disable, []) # Discard extensions

		# Update metadata
		if DataSplitter.Metadata in partition_mod:
			partition_mod.pop(DataSplitter.MetadataHeader)
			partition_mod.pop(DataSplitter.Metadata)
		if metadata_list_added:
			partition_mod[DataSplitter.MetadataHeader] = block_new.get(DataProvider.Metadata)
			partition_mod[DataSplitter.Metadata] = metadata_list_added

		return (proc_mode, partition_list_added or [])

	def _resync_handle_removed_file(self, idx, partition_mod, size_list, fi_removed):
		# Remove files from partition
		partition_mod[DataSplitter.Comment] += '[rm] ' + fi_removed[DataProvider.URL]
		partition_mod[DataSplitter.Comment] += '-%d ' % fi_removed[DataProvider.NEntries]

		if fi_removed[DataProvider.NEntries] > 0:
			if idx == len(partition_mod[DataSplitter.FileList]) - 1:
				# Removal of last file from current partition
				partition_mod[DataSplitter.NEntries] = sum(size_list) - partition_mod.get(DataSplitter.Skipped, 0)
				partition_mod[DataSplitter.Comment] += '[rm_last] '
			elif idx == 0:
				# Removal of first file from current partition
				partition_mod[DataSplitter.NEntries] += max(0, size_list[idx] - partition_mod.get(DataSplitter.Skipped, 0) - partition_mod[DataSplitter.NEntries])
				partition_mod[DataSplitter.NEntries] += partition_mod.get(DataSplitter.Skipped, 0)
				if DataSplitter.Skipped in partition_mod:
					partition_mod[DataSplitter.Skipped] = 0
				partition_mod[DataSplitter.Comment] += '[rm_first] '
			else:
				# File in the middle is affected - solution very simple :)
				partition_mod[DataSplitter.Comment] += '[rm_middle] '
			partition_mod[DataSplitter.NEntries] -= fi_removed[DataProvider.NEntries]

		partition_mod[DataSplitter.FileList].pop(idx)
		size_list.pop(idx)

		proc_mode = self._mode_removed
		for meta in partition_mod.get(DataSplitter.MetadataHeader, []):
			proc_mode = min(proc_mode, self._metadata_resync_option.get(meta, ResyncMode.ignore))
		return proc_mode

	def _setup(self, cq_info, block):
		(func, item, default) = cq_info
		# make sure non-specific default value is specified (for metadata and resyncs)
		if item not in self._config_protocol:
			self._config_protocol[item] = func(item, default)
		skey = block.get(DataProvider.Nickname, 'unknown')
		pkey = ('[%s] %s' % (skey, item)).strip()
		if pkey not in self._config_protocol:
			self._config_protocol[pkey] = func(item, default)
		return self._config_protocol[pkey]

make_enum(['Dataset', 'Locations', 'NEntries', 'Skipped', 'FileList', 'Nickname', 'DatasetID', # DatasetID is legacy
	'CommonPrefix', 'Invalid', 'BlockName', 'MetadataHeader', 'Metadata', 'Comment'], DataSplitter, use_hash = False)


class DataSplitterIO(Plugin):
	def import_partition_source(self, path):
		raise AbstractError

	def save_partition_source(self, path, splitter_info_dict_dict, partition_iter, partition_len_hint, message = 'Writing job mapping file'):
		raise AbstractError
