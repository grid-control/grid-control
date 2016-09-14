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
from grid_control.config import create_config, noDefault
from grid_control.datasets.provider_base import DataProvider
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.activity import Activity
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import imap, irange, itemgetter, lmap, next, sort_inplace

def fast_search(lst, key_fun, key):
	(idx, hi) = (0, len(lst))
	while idx < hi:
		mid = int((idx + hi) / 2)
		if key_fun(lst[mid]) < key:
			idx = mid + 1
		else:
			hi = mid
	if (idx < len(lst)) and (key_fun(lst[idx]) == key):
		return lst[idx]

ResyncMode = makeEnum(['disable', 'complete', 'changed', 'ignore']) # prio: "disable" overrides "complete", etc.
ResyncMode.noChanged = [ResyncMode.disable, ResyncMode.complete, ResyncMode.ignore]
ResyncOrder = makeEnum(['append', 'preserve', 'fillgap', 'reorder']) # reorder mechanism

class PartitionError(NestedException):
	pass


class DataSplitterIO(Plugin):
	def saveSplitting(self, path, meta, source, sourceLenHint, message = 'Writing job mapping file'):
		raise AbstractError

	def loadSplitting(self, path):
		raise AbstractError


class DataSplitter(ConfigurablePlugin):
	def __init__(self, config, datasource_name):
		ConfigurablePlugin.__init__(self, config)
		self._datasource_name = datasource_name
		self.setState(src = None, protocol = {})
		# Resync settings:
		self._interactive = config.isInteractive(['partition resync', '%s partition resync' % datasource_name], False)
		#   behaviour in case of event size changes
		self._mode_removed = config.getEnum('resync mode removed', ResyncMode, ResyncMode.complete, subset = ResyncMode.noChanged)
		self._mode_expanded = config.getEnum('resync mode expand', ResyncMode, ResyncMode.changed)
		self._mode_shrunken = config.getEnum('resync mode shrink', ResyncMode, ResyncMode.changed)
		self._mode_new = config.getEnum('resync mode new', ResyncMode, ResyncMode.complete, subset = [ResyncMode.complete, ResyncMode.ignore])
		#   behaviour in case of metadata changes
		self._metadata_resync_option = {}
		for meta in config.getList('resync metadata', [], onChange = None):
			self._metadata_resync_option[meta] = config.getEnum('resync mode %s' % meta, ResyncMode, ResyncMode.complete, subset = ResyncMode.noChanged)
		#   behaviour in case of job changes - disable changed jobs, preserve job number of changed jobs or reorder
		self._resyncOrder = config.getEnum('resync jobs', ResyncOrder, ResyncOrder.append)
		self._configure_splitter(config)


	def setState(self, src, protocol):
		self._splitSource = src
		self._protocol = protocol


	def _configure_splitter(self, config):
		pass


	def _query_config(self, fun, item, default = noDefault):
		key = (fun, item, default)
		self._setup(key, {}) # query once for init
		return key


	def _setup(self, cq_info, block):
		(func, item, default) = cq_info
		# make sure non-specific default value is specified (for metadata and resyncs)
		if item not in self._protocol:
			self._protocol[item] = func(item, default)
		skey = block.get(DataProvider.Nickname, 'unknown')
		pkey = ('[%s] %s' % (skey, item)).strip()
		if pkey not in self._protocol:
			self._protocol[pkey] = func(item, default)
		return self._protocol[pkey]


	def get_needed_enums(cls):
		return [DataSplitter.FileList]
	get_needed_enums = classmethod(get_needed_enums)


	def _finish_partition(self, block, partition, files = None):
		# Copy infos from block
		for prop in ['Dataset', 'BlockName', 'Nickname', 'Locations']:
			if getattr(DataProvider, prop) in block:
				partition[getattr(DataSplitter, prop)] = block[getattr(DataProvider, prop)]
		if DataProvider.Metadata in block:
			partition[DataSplitter.MetadataHeader] = block[DataProvider.Metadata]
		# Helper for very simple splitter
		if files:
			partition[DataSplitter.FileList] = lmap(lambda x: x[DataProvider.URL], files)
			partition[DataSplitter.NEntries] = sum(imap(lambda x: x[DataProvider.NEntries], files))
			if DataProvider.Metadata in block:
				partition[DataSplitter.Metadata] = lmap(lambda x: x[DataProvider.Metadata], files)
		return partition


	def _partition_block(self, blocks, firstEvent = 0):
		raise AbstractError


	def partition_block(self, path, blocks):
		activity = Activity('Splitting dataset into jobs')
		self.save_partitions(path, self._partition_block(blocks))
		self.import_partitions(path)
		activity.finish()


	def get_partition(self, partition_num):
		if partition_num >= self.get_partition_len():
			raise PartitionError('Job %d out of range for available dataset' % partition_num)
		return self._splitSource[partition_num]


	def iter_partitions(self):
		for partition_num in irange(self.get_partition_len()):
			yield self._splitSource[partition_num]


	def get_partition_len(self):
		return self._splitSource.maxJobs


	# Save as tar file to allow random access to mapping data with little memory overhead
	def save_partitions(self, path, source = None, sourceLenHint = None, message = 'Writing job mapping file'):
		if source and not sourceLenHint:
			source = list(source)
			sourceLenHint = len(source)
		elif not source:
			(source, sourceLenHint) = (self._splitSource, self.get_partition_len())
		# Write metadata to allow reconstruction of data splitter
		meta = {'ClassName': self.__class__.__name__}
		meta.update(self._protocol)
		DataSplitterIO.createInstance('DataSplitterIOAuto').saveSplitting(path, meta, source, sourceLenHint, message)


	def import_partitions(self, path):
		self._splitSource = DataSplitterIO.createInstance('DataSplitterIOAuto').loadSplitting(path)


	# Get block information (oldBlock, newBlock, filesMissing, filesMatched) which partition is based on
	def _resync_get_matching_block(self, partition, blocksMissing, blocksMatching):
		# Search for block in missing and matched blocks
		def getBlockKey(dsBlock):
			return (dsBlock[DataProvider.Dataset], dsBlock[DataProvider.BlockName])
		partitionKey = (partition[DataSplitter.Dataset], partition[DataSplitter.BlockName])
		result = fast_search(blocksMissing, getBlockKey, partitionKey)
		if result:
			return (result, None, result[DataProvider.FileList], [])
		return fast_search(blocksMatching, lambda x: getBlockKey(x[0]), partitionKey) # compare with old block


	# Get list of matching metadata indices
	def _resync_get_matching_metadata(self, oldBlock, newBlock):
		result = []
		for meta in self._metadata_resync_option:
			(oldIdx, newIdx) = (None, None)
			if oldBlock and (meta in oldBlock.get(DataProvider.Metadata, [])):
				oldIdx = oldBlock[DataProvider.Metadata].index(meta)
			if newBlock and (meta in newBlock.get(DataProvider.Metadata, [])):
				newIdx = newBlock[DataProvider.Metadata].index(meta)
			if (oldIdx is not None) or (newIdx is not None):
				result.append((oldIdx, newIdx, self._metadata_resync_option[meta]))
		return result


	# Process changed files in partition - returns True if file index should be increased
	def _resync_changed_file_entries(self, idx, modSI, partition_num, sizeInfo, oldFI, newFI, newBlock, extended):
		modSI[DataSplitter.Comment] += ' [changed] ' + oldFI[DataProvider.URL]
		modSI[DataSplitter.Comment] += (' -%d ' % oldFI[DataProvider.NEntries])
		modSI[DataSplitter.Comment] += (' +%d ' % newFI[DataProvider.NEntries])

		def removeCompleteFile():
			modSI[DataSplitter.NEntries] -= oldFI[DataProvider.NEntries]
			modSI[DataSplitter.FileList].pop(idx)
			sizeInfo.pop(idx)

		def replaceCompleteFile():
			modSI[DataSplitter.NEntries] += newFI[DataProvider.NEntries]
			modSI[DataSplitter.NEntries] -= oldFI[DataProvider.NEntries]
			sizeInfo[idx] = newFI[DataProvider.NEntries]

		def expandOutside():
			fileList = newBlock.pop(DataProvider.FileList)
			newBlock[DataProvider.FileList] = [newFI]
			for extSplit in self._partition_block([newBlock], oldFI[DataProvider.NEntries]):
				extSplit[DataSplitter.Comment] = 'src: %d [ext_1]' % partition_num
				extended.append(extSplit)
			newBlock[DataProvider.FileList] = fileList
			sizeInfo[idx] = newFI[DataProvider.NEntries]

		if idx == len(modSI[DataSplitter.FileList]) - 1:
			coverLast = modSI.get(DataSplitter.Skipped, 0) + modSI[DataSplitter.NEntries] - sum(sizeInfo[:-1])
			if coverLast == oldFI[DataProvider.NEntries]:
				# Change of last file, which ends in current partition
				if (extended is not None) and (oldFI[DataProvider.NEntries] < newFI[DataProvider.NEntries]):
					expandOutside()
					modSI[DataSplitter.Comment] += '[last_add_1] '
				else:
					replaceCompleteFile()
					modSI[DataSplitter.Comment] += '[last_add_2] '
			elif coverLast > newFI[DataProvider.NEntries]:
				# Change of last file, which changes current coverage
				modSI[DataSplitter.NEntries] -= coverLast
				modSI[DataSplitter.NEntries] += oldFI[DataProvider.NEntries]
				replaceCompleteFile()
				modSI[DataSplitter.Comment] += '[last_add_3] '
			else:
				# Change of last file outside of current partition
				sizeInfo[idx] = newFI[DataProvider.NEntries]
				modSI[DataSplitter.Comment] += '[last_add_4] '

		elif idx == 0:
			# First file is affected
			if newFI[DataProvider.NEntries] > modSI.get(DataSplitter.Skipped, 0):
				# First file changes and still lives in new partition
				following = sizeInfo[0] - modSI.get(DataSplitter.Skipped, 0) - modSI[DataSplitter.NEntries]
				shrinkage = oldFI[DataProvider.NEntries] - newFI[DataProvider.NEntries]
				if following > 0:
					# First file not completely covered by current partition
					if following < shrinkage:
						# Covered area of first file shrinks
						modSI[DataSplitter.NEntries] += following
						replaceCompleteFile()
						modSI[DataSplitter.Comment] += '[first_add_1] '
					else:
						# First file changes outside of current partition
						sizeInfo[idx] = newFI[DataProvider.NEntries]
						modSI[DataSplitter.Comment] = '[first_add_2] '
				else:
					# Change of first file ending in current partition - One could try to
					# 'reverse fix' expanding files to allow expansion via adding only the expanding part
					replaceCompleteFile()
					modSI[DataSplitter.Comment] += '[first_add_3] '
			else:
				# Removal of first file from current partition
				modSI[DataSplitter.NEntries] += max(0, sizeInfo[idx] - modSI.get(DataSplitter.Skipped, 0) - modSI[DataSplitter.NEntries])
				modSI[DataSplitter.NEntries] += modSI.get(DataSplitter.Skipped, 0)
				if DataSplitter.Skipped in modSI:
					modSI[DataSplitter.Skipped] = 0
				removeCompleteFile()
				return False

		else:
			# File in the middle is affected - solution very simple :)
			# Replace file - expanding files could be swapped to the (fully contained) end
			# to allow expansion via adding only the expanding part
			replaceCompleteFile()
			modSI[DataSplitter.Comment] += '[middle_add_1] '
		return True


	def _resync_changed_file_metadata(self, oldFI, newFI, metaIdxLookup, newMetadata, procMode):
		newMetadata.append(newFI[DataProvider.Metadata])
		for (oldMI, newMI, metaProc) in metaIdxLookup:
			if (oldMI is None) or (newMI is None):
				procMode = min(procMode, metaProc) # Metadata was removed
			elif oldFI[DataProvider.Metadata][oldMI] != newFI[DataProvider.Metadata][newMI]:
				procMode = min(procMode, metaProc) # Metadata was changed
		return procMode


	def _resync_changed_file(self, procMode, idx, modSI, partition_num, sizeInfo, newBlock, extended, oldFI, newFI, newMetadata, metaIdxLookup):
		if DataProvider.Metadata in newFI:
			procMode = self._resync_changed_file_metadata(oldFI, newFI, metaIdxLookup, newMetadata, procMode)
		if oldFI[DataProvider.NEntries] == newFI[DataProvider.NEntries]:
			return (procMode, idx + 1) # go to next file
		elif oldFI[DataProvider.NEntries] * newFI[DataProvider.NEntries] < 0:
			raise PartitionError('Unable to change %r from %d to %d entries!' % (newFI[DataProvider.LFN],
				oldFI[DataProvider.NEntries], newFI[DataProvider.NEntries]))
		elif newFI[DataProvider.NEntries] < 0:
			return (procMode, idx + 1) # go to next file
		oldEvts = modSI[DataSplitter.NEntries]
		oldSkip = modSI.get(DataSplitter.Skipped)

		if self._resync_changed_file_entries(idx, modSI, partition_num, sizeInfo, oldFI, newFI, newBlock, extended):
			idx += 1 # True => file index should be increased

		mode = utils.QM(oldFI[DataProvider.NEntries] < newFI[DataProvider.NEntries], self._mode_expanded, self._mode_shrunken)
		if mode == ResyncMode.changed:
				mode = ResyncMode.ignore
				if (oldEvts != modSI[DataSplitter.NEntries]) or (oldSkip != modSI.get(DataSplitter.Skipped)):
					mode = ResyncMode.complete
		procMode = min(procMode, mode)
		return (procMode, idx) # go to next file


	# Remove files from partition
	def _resync_removed_file(self, idx, modSI, sizeInfo, rmFI):
		modSI[DataSplitter.Comment] += '[rm] ' + rmFI[DataProvider.URL]
		modSI[DataSplitter.Comment] += '-%d ' % rmFI[DataProvider.NEntries]

		if rmFI[DataProvider.NEntries] > 0:
			if idx == len(modSI[DataSplitter.FileList]) - 1:
				# Removal of last file from current partition
				modSI[DataSplitter.NEntries] = sum(sizeInfo) - modSI.get(DataSplitter.Skipped, 0)
				modSI[DataSplitter.Comment] += '[rm_last] '
			elif idx == 0:
				# Removal of first file from current partition
				modSI[DataSplitter.NEntries] += max(0, sizeInfo[idx] - modSI.get(DataSplitter.Skipped, 0) - modSI[DataSplitter.NEntries])
				modSI[DataSplitter.NEntries] += modSI.get(DataSplitter.Skipped, 0)
				if DataSplitter.Skipped in modSI:
					modSI[DataSplitter.Skipped] = 0
				modSI[DataSplitter.Comment] += '[rm_first] '
			else:
				# File in the middle is affected - solution very simple :)
				modSI[DataSplitter.Comment] += '[rm_middle] '
			modSI[DataSplitter.NEntries] -= rmFI[DataProvider.NEntries]

		modSI[DataSplitter.FileList].pop(idx)
		sizeInfo.pop(idx)

		procMode = self._mode_removed
		for meta in modSI.get(DataSplitter.MetadataHeader, []):
			procMode = min(procMode, self._metadata_resync_option.get(meta, ResyncMode.ignore))
		return procMode


	# resync a single file in the partition, return next file index to process
	def _resyncFiles(self, modSI, partition_num, sizeInfo, filesMissing, filesMatched, newBlock, metaIdxLookup, extended):
		# Select processing mode for job (disable > complete > changed > ignore) [ie. disable overrides all] using min
		# Result: one of [disable, complete, ignore] (changed -> complete or igore)
		idx = 0
		newMetadata = []
		procMode = ResyncMode.ignore
		while idx < len(modSI[DataSplitter.FileList]):
			rmFI = fast_search(filesMissing, itemgetter(DataProvider.URL), modSI[DataSplitter.FileList][idx])
			if rmFI:
				procMode = min(procMode, self._resync_removed_file(idx, modSI, sizeInfo, rmFI))
			else:
				(oldFI, newFI) = fast_search(filesMatched, lambda x: x[0][DataProvider.URL], modSI[DataSplitter.FileList][idx])
				(procMode, idx) = self._resync_changed_file(procMode, idx, modSI, partition_num, sizeInfo, newBlock, extended, oldFI, newFI, newMetadata, metaIdxLookup)
		return (procMode, newMetadata)


	# Resync single partition
	# With doExpandOutside, gc tries to handle expanding files via the partition function
	def _resyncPartition(self, modSI, partition_num, oldBlock, newBlock, filesMissing, filesMatched, doExpandOutside):
		if newBlock: # copy new location information
			modSI[DataSplitter.Locations] = newBlock.get(DataProvider.Locations)
		# Determine old size infos and get started
		def search_url(url):
			return fast_search(oldBlock[DataProvider.FileList], itemgetter(DataProvider.URL), url)
		sizeInfo = lmap(lambda url: search_url(url)[DataProvider.NEntries], modSI[DataSplitter.FileList])
		metaIdxLookup = self._resync_get_matching_metadata(oldBlock, newBlock)

		extended = utils.QM(doExpandOutside, [], None)
		old_entries = modSI[DataSplitter.NEntries]
		(procMode, newMetadata) = self._resyncFiles(modSI, partition_num, sizeInfo, filesMissing, filesMatched, newBlock, metaIdxLookup, extended)
		# Disable invalid / invalidated partitions
		if (len(modSI[DataSplitter.FileList]) == 0) or (old_entries * modSI[DataSplitter.NEntries] <= 0):
			procMode = ResyncMode.disable

		if procMode == ResyncMode.disable:
			modSI[DataSplitter.Invalid] = True
			return (ResyncMode.disable, []) # Discard extensions

		# Update metadata
		if DataSplitter.Metadata in modSI:
			modSI.pop(DataSplitter.MetadataHeader)
			modSI.pop(DataSplitter.Metadata)
		if newMetadata:
			modSI[DataSplitter.MetadataHeader] = newBlock.get(DataProvider.Metadata)
			modSI[DataSplitter.Metadata] = newMetadata

		return (procMode, extended or [])


	def _resyncExistingPartitions(self, partition_num, partition, blocksAdded, blocksMissing, blocksMatching):
		if DataSplitter.Comment not in partition:
			partition[DataSplitter.Comment] = 'src: %d ' % partition_num
		if partition.get(DataSplitter.Invalid, False):
			return (partition, ResyncMode.ignore, [])
		modpartition = copy.deepcopy(partition)
		(oldBlock, newBlock, filesMissing, filesMatched) = self._resync_get_matching_block(modpartition, blocksMissing, blocksMatching)
		(procMode, extended) = self._resyncPartition(modpartition, partition_num, oldBlock, newBlock, filesMissing, filesMatched, doExpandOutside = True)
		return (modpartition, procMode, extended)


	# Process partitions
	def _resyncIterator_raw(self, blocksAdded, blocksMissing, blocksMatching):
		extList = []
		# Perform resync of existing partitions
		for (partition_num, partition) in enumerate(self.iter_partitions()):
			(modpartition, procMode, extended) = self._resyncExistingPartitions(partition_num, partition, blocksAdded, blocksMissing, blocksMatching)
			if (self._resyncOrder == ResyncOrder.append) and (procMode == ResyncMode.complete):
				extList.append(modpartition) # add modified partition to list of new partitions
				modpartition = copy.copy(partition) # replace current partition with a fresh copy that is marked as invalid
				modpartition[DataSplitter.Invalid] = True
				procMode = ResyncMode.disable
			extList.extend(extended)
			yield (partition_num, modpartition, procMode)
		# Yield collected extensions of existing partitions
		for extpartition in extList:
			yield (None, extpartition, ResyncMode.ignore)
		# Yield completely new partitions
		if self._mode_new == ResyncMode.complete:
			for newpartition in self._partition_block(blocksAdded):
				yield (None, newpartition, ResyncMode.ignore)


	# Sort resynced partitions into updated and added lists
	def _resyncIterator_sort(self, blocksAdded, blocksMissing, blocksMatching):
		(splitUpdated, splitAdded) = ([], [])
		for (partition_num, partition, procMode) in self._resyncIterator_raw(blocksAdded, blocksMissing, blocksMatching):
			if partition_num is not None: # Separate existing and new partitions
				splitUpdated.append((partition_num, partition, procMode))
			else:
				splitAdded.append((None, partition, None))
		return (splitUpdated, splitAdded)


	# Use reordering if setup - log interventions (disable, redo) according to procMode
	def _resyncIterator(self, resultRedo, resultDisable, blocksAdded, blocksMissing, blocksMatching):
		def getReorderIterator(mainIter, altIter): # alt source is used if main source contains invalid entries
			for (partition_num, partition, procMode) in mainIter:
				if partition.get(DataSplitter.Invalid, False) or (procMode == ResyncMode.disable):
					extInfo = next(altIter, None)
					while extInfo and extInfo[1].get(DataSplitter.Invalid, False):
						extInfo = next(altIter, None)
					if extInfo:
						yield (partition_num, extInfo[1], ResyncMode.complete) # Overwrite invalid partitions
						continue
				yield (partition_num, partition, procMode)
			for extInfo in altIter:
				yield (None, extInfo[1], ResyncMode.ignore)

		if self._resyncOrder == ResyncOrder.fillgap:
			splitUpdated, splitAdded = self._resyncIterator_sort(blocksAdded, blocksMissing, blocksMatching)
			resyncIter = getReorderIterator(splitUpdated, iter(splitAdded))
		elif self._resyncOrder == ResyncOrder.reorder:
			splitUpdated, splitAdded = self._resyncIterator_sort(blocksAdded, blocksMissing, blocksMatching)
			tsi = utils.TwoSidedIterator(splitUpdated + splitAdded)
			resyncIter = getReorderIterator(tsi.forward(), tsi.backward())
		else:
			resyncIter = self._resyncIterator_raw(blocksAdded, blocksMissing, blocksMatching)

		for (partition_num, partition, procMode) in resyncIter:
			if partition_num:
				if procMode == ResyncMode.complete:
					resultRedo.append(partition_num)
				if procMode == ResyncMode.disable:
					resultDisable.append(partition_num)
			yield partition


	def resyncMapping(self, newSplitPath, oldBlocks, newBlocks):
		activity = Activity('Performing resynchronization of dataset')
		(blocksAdded, blocksMissing, blocksMatching) = DataProvider.resyncSources(oldBlocks, newBlocks)
		for rmBlock in blocksMissing: # Files in matching blocks are already sorted
			sort_inplace(rmBlock[DataProvider.FileList], key = lambda x: x[DataProvider.URL])
		activity.finish()

		# User overview and setup starts here
		resultRedo = []
		resultDisable = []
		newSplitPathTMP = newSplitPath + '.tmp'
		resyncIter = self._resyncIterator(resultRedo, resultDisable, blocksAdded, blocksMissing, blocksMatching)
		self.save_partitions(newSplitPathTMP, resyncIter, sourceLenHint = self.get_partition_len(),
			message = 'Performing resynchronization of dataset map (progress is estimated)')

		if self._interactive:
			# TODO: print info and ask
			if not utils.getUserBool('Do you want to use the new dataset partition?', False):
				return
		os.rename(newSplitPathTMP, newSplitPath)

		return (resultRedo, resultDisable)


	def loadPartitionsForScript(path, cfg = None):
		src = DataSplitterIO.createInstance('DataSplitterIOAuto').loadSplitting(path)
		# Transfer config protocol (in case no split function is called)
		protocol = {}
		for (section, options) in src.metadata.items():
			section = section.replace('dataset', '').strip()
			for (option, value) in options.items():
				if section:
					option = '[%s] %s' % (section, option)
				protocol[option.strip()] = value
				if cfg is not None:
					cfg.set(option, str(value))
		# Create and setup splitter
		if cfg is None:
			cfg = create_config(configDict = src.metadata)
		splitter = DataSplitter.createInstance(src.classname, cfg, 'dataset')
		splitter.setState(src, protocol)
		return splitter
	loadPartitionsForScript = staticmethod(loadPartitionsForScript)

makeEnum(['Dataset', 'Locations', 'NEntries', 'Skipped', 'FileList', 'Nickname', 'DatasetID', # DatasetID is legacy
	'CommonPrefix', 'Invalid', 'BlockName', 'MetadataHeader', 'Metadata', 'Comment'], DataSplitter, useHash = False)
