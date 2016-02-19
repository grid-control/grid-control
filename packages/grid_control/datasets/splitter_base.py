#-#  Copyright 2009-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, copy
from grid_control import utils
from grid_control.config import createConfig, noDefault
from grid_control.datasets.provider_base import DataProvider
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import identity, ifilter, imap, irange, ismap, itemgetter, lmap, next, sort_inplace

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
	def saveState(self, path, meta, source, sourceLen, message = 'Writing job mapping file'):
		raise AbstractError

	def loadState(self, path):
		raise AbstractError


class DataSplitter(ConfigurablePlugin):
	def __init__(self, config):
		self.config = config
		self.splitSource = None
		self._protocol = {}
		# Resync settings:
		self.interactive = config.getBool('resync interactive', False, onChange = None)
		#   behaviour in case of event size changes
		self.mode_removed = config.getEnum('resync mode removed', ResyncMode, ResyncMode.complete, subset = ResyncMode.noChanged)
		self.mode_expanded = config.getEnum('resync mode expand', ResyncMode, ResyncMode.changed)
		self.mode_shrunken = config.getEnum('resync mode shrink', ResyncMode, ResyncMode.changed)
		self.mode_new = config.getEnum('resync mode new', ResyncMode, ResyncMode.complete, subset = [ResyncMode.complete, ResyncMode.ignore])
		#   behaviour in case of metadata changes
		self.metaOpts = {}
		for meta in config.getList('resync metadata', [], onChange = None):
			self.metaOpts[meta] = config.getEnum('resync mode %s' % meta, ResyncMode, ResyncMode.complete, subset = ResyncMode.noChanged)
		#   behaviour in case of job changes - disable changed jobs, preserve job number of changed jobs or reorder
		self.resyncOrder = config.getEnum('resync jobs', ResyncOrder, ResyncOrder.append)



	def setup(self, func, block, item, default = noDefault):
		# make sure non-specific default value is specified (for metadata and resyncs)
		if item not in self._protocol:
			self._protocol[item] = func(item, default)
		skey = block.get(DataProvider.Nickname, 'unknown')
		pkey = ('[%s] %s' % (skey, item)).strip()
		if pkey not in self._protocol:
			self._protocol[pkey] = func(item, default)
		return self._protocol[pkey]


	def neededVars(cls):
		return [DataSplitter.FileList]
	neededVars = classmethod(neededVars)


	def finaliseJobSplitting(self, block, splitInfo, files = None):
		# Copy infos from block
		for prop in ['Dataset', 'BlockName', 'DatasetID', 'Nickname', 'Locations']:
			if getattr(DataProvider, prop) in block:
				splitInfo[getattr(DataSplitter, prop)] = block[getattr(DataProvider, prop)]
		if DataProvider.Metadata in block:
			splitInfo[DataSplitter.MetadataHeader] = block[DataProvider.Metadata]
		# Helper for very simple splitter
		if files:
			splitInfo[DataSplitter.FileList] = lmap(lambda x: x[DataProvider.URL], files)
			splitInfo[DataSplitter.NEntries] = sum(imap(lambda x: x[DataProvider.NEntries], files))
			if DataProvider.Metadata in block:
				splitInfo[DataSplitter.Metadata] = lmap(lambda x: x[DataProvider.Metadata], files)
		return splitInfo


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		raise AbstractError


	def splitDataset(self, path, blocks):
		log = utils.ActivityLog('Splitting dataset into jobs')
		self.saveState(path, self.splitDatasetInternal(blocks))
		self.importState(path)


	def getSplitInfo(self, jobNum):
		if jobNum >= self.getMaxJobs():
			raise PartitionError('Job %d out of range for available dataset' % jobNum)
		return self.splitSource[jobNum]


	def getMaxJobs(self):
		return self.splitSource.maxJobs


	def resyncMapping(self, newSplitPath, oldBlocks, newBlocks):
		log = utils.ActivityLog('Performing resynchronization of dataset')
		(blocksAdded, blocksMissing, blocksMatching) = DataProvider.resyncSources(oldBlocks, newBlocks)
		for rmBlock in blocksMissing: # Files in matching blocks are already sorted
			sort_inplace(rmBlock[DataProvider.FileList], key = lambda x: x[DataProvider.URL])
		del log

		# Get block information (oldBlock, newBlock, filesMissing, filesMatched) which splitInfo is based on
		def getMatchingBlock(splitInfo):
			# Search for block in missing and matched blocks
			def getBlockKey(dsBlock):
				return (dsBlock[DataProvider.Dataset], dsBlock[DataProvider.BlockName])
			splitInfoKey = (splitInfo[DataSplitter.Dataset], splitInfo[DataSplitter.BlockName])
			result = fast_search(blocksMissing, getBlockKey, splitInfoKey)
			if result:
				return (result, None, result[DataProvider.FileList], [])
			return fast_search(blocksMatching, lambda x: getBlockKey(x[0]), splitInfoKey) # compare with old block

		#######################################
		# Process modifications of event sizes
		#######################################

		# Apply modification list to old splitting
		# Input: oldSplit, modList = [(rmfile, addfile), ...], doExpandOutside
		# With doExpandOutside, gc tries to handle expanding files via the splitting function
		def resyncSplitting(oldSplit, doExpandOutside, jobNum):
			if oldSplit.get(DataSplitter.Invalid, False):
				return (oldSplit, ResyncMode.ignore, [])

			(oldBlock, newBlock, filesMissing, filesMatched) = getMatchingBlock(oldSplit)

			modSI = copy.deepcopy(oldSplit)
			if newBlock:
				modSI[DataSplitter.Locations] = newBlock.get(DataProvider.Locations)
			# Determine size infos and get started
			def search_url(url):
				return fast_search(oldBlock[DataProvider.FileList], itemgetter(DataProvider.URL), url)
			sizeInfo = lmap(lambda url: search_url(url)[DataProvider.NEntries], modSI[DataSplitter.FileList])
			extended = []
			metaIdxLookup = []
			for meta in self.metaOpts:
				(oldIdx, newIdx) = (None, None)
				if oldBlock and (meta in oldBlock.get(DataProvider.Metadata, [])):
					oldIdx = oldBlock[DataProvider.Metadata].index(meta)
				if newBlock and (meta in newBlock.get(DataProvider.Metadata, [])):
					newIdx = newBlock[DataProvider.Metadata].index(meta)
				if (oldIdx is not None) or (newIdx is not None):
					metaIdxLookup.append((oldIdx, newIdx, self.metaOpts[meta]))

			# Select processing mode for job (disable > complete > changed > ignore) [ie. disable overrides all] using min
			# Result: one of [disable, complete, ignore] (changed -> complete or igore)
			procMode = ResyncMode.ignore

			# Remove files from splitting
			def removeFile(idx, rmFI):
				modSI[DataSplitter.Comment] += '[rm] ' + rmFI[DataProvider.URL]
				modSI[DataSplitter.Comment] += '-%d ' % rmFI[DataProvider.NEntries]

				if idx == len(modSI[DataSplitter.FileList]) - 1:
					# Removal of last file from current splitting
					modSI[DataSplitter.NEntries] = sum(sizeInfo) - modSI.get(DataSplitter.Skipped, 0)
					modSI[DataSplitter.Comment] += '[rm_last] '
				elif idx == 0:
					# Removal of first file from current splitting
					modSI[DataSplitter.NEntries] += max(0, sizeInfo[idx] - modSI.get(DataSplitter.Skipped, 0) - modSI[DataSplitter.NEntries])
					modSI[DataSplitter.NEntries] += modSI.get(DataSplitter.Skipped, 0)
					modSI[DataSplitter.Skipped] = 0
					modSI[DataSplitter.Comment] += '[rm_first] '
				else:
					# File in the middle is affected - solution very simple :)
					modSI[DataSplitter.Comment] += '[rm_middle] '

				modSI[DataSplitter.NEntries] -= rmFI[DataProvider.NEntries]
				modSI[DataSplitter.FileList].pop(idx)
				sizeInfo.pop(idx)


			# Process changed files in splitting - returns True if file index should be increased
			def changeFile(idx, oldFI, newFI):
				modSI[DataSplitter.Comment] += '[changed] ' + oldFI[DataProvider.URL]
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
					for extSplit in self.splitDatasetInternal([newBlock], oldFI[DataProvider.NEntries]):
						extSplit[DataSplitter.Comment] = oldSplit[DataSplitter.Comment] + '[ext_1] '
						extended.append(extSplit)
					newBlock[DataProvider.FileList] = fileList
					sizeInfo[idx] = newFI[DataProvider.NEntries]

				if idx == len(modSI[DataSplitter.FileList]) - 1:
					coverLast = modSI.get(DataSplitter.Skipped, 0) + modSI[DataSplitter.NEntries] - sum(sizeInfo[:-1])
					if coverLast == oldFI[DataProvider.NEntries]:
						# Change of last file, which ends in current splitting
						if doExpandOutside and (oldFI[DataProvider.NEntries] < newFI[DataProvider.NEntries]):
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
						# Change of last file outside of current splitting
						sizeInfo[idx] = newFI[DataProvider.NEntries]
						modSI[DataSplitter.Comment] += '[last_add_4] '

				elif idx == 0:
					# First file is affected
					if (newFI[DataProvider.NEntries] > modSI.get(DataSplitter.Skipped, 0)):
						# First file changes and still lives in new splitting
						following = sizeInfo[0] - modSI.get(DataSplitter.Skipped, 0) - modSI[DataSplitter.NEntries]
						shrinkage = oldFI[DataProvider.NEntries] - newFI[DataProvider.NEntries]
						if following > 0:
							# First file not completely covered by current splitting
							if following < shrinkage:
								# Covered area of first file shrinks
								modSI[DataSplitter.NEntries] += following
								replaceCompleteFile()
								modSI[DataSplitter.Comment] += '[first_add_1] '
							else:
								# First file changes outside of current splitting
								sizeInfo[idx] = newFI[DataProvider.NEntries]
								modSI[DataSplitter.Comment] = '[first_add_2] '
						else:
							# Change of first file ending in current splitting - One could try to
							# 'reverse fix' expanding files to allow expansion via adding only the expanding part
							replaceCompleteFile()
							modSI[DataSplitter.Comment] += '[first_add_3] '
					else:
						# Removal of first file from current splitting
						modSI[DataSplitter.NEntries] += max(0, sizeInfo[idx] - modSI.get(DataSplitter.Skipped, 0) - modSI[DataSplitter.NEntries])
						modSI[DataSplitter.NEntries] += modSI.get(DataSplitter.Skipped, 0)
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

			idx = 0
			newMetadata = []
			while idx < len(modSI[DataSplitter.FileList]):
				url = modSI[DataSplitter.FileList][idx]

				rmFI = fast_search(filesMissing, itemgetter(DataProvider.URL), url)
				if rmFI:
					removeFile(idx, rmFI)
					procMode = min(procMode, self.mode_removed)
					for meta in modSI.get(DataSplitter.MetadataHeader, []):
						procMode = min(procMode, self.metaOpts.get(meta, ResyncMode.ignore))
					continue # dont increase filelist index!

				(oldFI, newFI) = fast_search(filesMatched, lambda x: x[0][DataProvider.URL], url)
				if DataProvider.Metadata in newFI:
					newMetadata.append(newFI[DataProvider.Metadata])
					for (oldMI, newMI, metaProc) in metaIdxLookup:
						if (oldMI is None) or (newMI is None):
							procMode = min(procMode, metaProc) # Metadata was removed
						elif (oldFI[DataProvider.Metadata][oldMI] != newFI[DataProvider.Metadata][newMI]):
							procMode = min(procMode, metaProc) # Metadata was changed
				if oldFI[DataProvider.NEntries] == newFI[DataProvider.NEntries]:
					idx += 1
					continue
				oldEvts = modSI[DataSplitter.NEntries]
				oldSkip = modSI[DataSplitter.Skipped]

				if changeFile(idx, oldFI, newFI):
					idx += 1

				mode = utils.QM(oldFI[DataProvider.NEntries] < newFI[DataProvider.NEntries], self.mode_expanded, self.mode_shrunken)
				if mode == ResyncMode.changed:
					changed = (oldEvts != modSI[DataSplitter.NEntries]) or (oldSkip != modSI[DataSplitter.Skipped])
					mode = utils.QM(changed, ResyncMode.complete, ResyncMode.ignore)
				procMode = min(procMode, mode)
				continue

			# Disable invalid / invalidated splittings
			if (len(modSI[DataSplitter.FileList]) == 0) or (modSI[DataSplitter.NEntries] <= 0):
				procMode = ResyncMode.disable

			if procMode == ResyncMode.disable:
				modSI[DataSplitter.Invalid] = True
				return (modSI, ResyncMode.disable, []) # Discard extensions

			# Update metadata
			if DataSplitter.Metadata in modSI:
				modSI.pop(DataSplitter.MetadataHeader)
				modSI.pop(DataSplitter.Metadata)
			if newMetadata:
				modSI[DataSplitter.MetadataHeader] = newBlock.get(DataProvider.Metadata)
				modSI[DataSplitter.Metadata] = newMetadata

			return (modSI, procMode, extended)

		# Process splittings
		def resyncIterator_raw():
			extList = []
			# Perform resync of existing splittings
			for jobNum in irange(self.getMaxJobs()):
				splitInfo = self.getSplitInfo(jobNum)
				if DataSplitter.Comment not in splitInfo:
					splitInfo[DataSplitter.Comment] = 'src: %d ' % jobNum
				(modSplitInfo, procMode, extended) = resyncSplitting(splitInfo, True, jobNum)
				if (self.resyncOrder == ResyncOrder.append) and (procMode == ResyncMode.complete):
					extList.append(modSplitInfo)
					modSplitInfo = copy.copy(splitInfo)
					modSplitInfo[DataSplitter.Invalid] = True
					procMode = ResyncMode.disable
				extList.extend(extended)
				yield (jobNum, modSplitInfo, procMode)
			# Yield collected extensions of existing splittings
			for extSplitInfo in extList:
				yield (None, extSplitInfo, ResyncMode.ignore)
			# Yield completely new splittings
			if self.mode_new == ResyncMode.complete:
				for newSplitInfo in self.splitDatasetInternal(blocksAdded):
					yield (None, newSplitInfo, ResyncMode.ignore)

		def getSplitContainer():
			(rawInfo, extInfo) = ([], [])
			for (jobNum, splitInfo, procMode) in resyncIterator_raw():
				if jobNum is not None: # Separate existing and new splittings
					rawInfo.append((jobNum, splitInfo, procMode))
				else:
					extInfo.append((None, splitInfo, None))
			return (rawInfo, extInfo)

		def getReorderIterator(mainIter, altIter): # alt source is used if main source contains invalid entries
			for (jobNum, splitInfo, procMode) in mainIter:
				if splitInfo.get(DataSplitter.Invalid, False) or (procMode == ResyncMode.disable):
					extInfo = next(altIter, None)
					while extInfo and extInfo[1].get(DataSplitter.Invalid, False):
						extInfo = next(altIter, None)
					if extInfo:
						yield (jobNum, extInfo[1], ResyncMode.complete) # Overwrite invalid splittings
						continue
				yield (jobNum, splitInfo, procMode)
			for extInfo in altIter:
				yield (None, extInfo[1], ResyncMode.ignore)

		# Use reordering if setup - log interventions (disable, redo) according to procMode
		resultRedo = []
		resultDisable = []
		def resyncIterator():
			if self.resyncOrder == ResyncOrder.fillgap:
				rawInfo, extInfo = getSplitContainer()
				resyncIter = getReorderIterator(rawInfo, iter(extInfo))
			elif self.resyncOrder == ResyncOrder.reorder:
				rawInfo, extInfo = getSplitContainer()
				tsi = utils.TwoSidedIterator(rawInfo + extInfo)
				resyncIter = getReorderIterator(tsi.forward(), tsi.backward())
			else:
				resyncIter = resyncIterator_raw()

			for (jobNum, splitInfo, procMode) in resyncIter:
				if jobNum:
					if procMode == ResyncMode.complete:
						resultRedo.append(jobNum)
					if procMode == ResyncMode.disable:
						resultDisable.append(jobNum)
				yield splitInfo

		# User overview and setup starts here
		newSplitPathTMP = newSplitPath + '.tmp'
		self.saveState(newSplitPathTMP, resyncIterator(), sourceLen = self.getMaxJobs(),
			message = 'Performing resynchronization of dataset map (progress is estimated)')

		if self.interactive:
			# TODO: print info and ask
			if not utils.getUserBool('Do you want to use the new dataset splitting?', False):
				return None
		os.rename(newSplitPathTMP, newSplitPath)

		return (resultRedo, resultDisable)


	# Save as tar file to allow random access to mapping data with little memory overhead
	def saveState(self, path, source = None, sourceLen = None, message = 'Writing job mapping file'):
		if source and not sourceLen:
			source = list(source)
			sourceLen = len(source)
		elif not source:
			(source, sourceLen) = (self.splitSource, self.getMaxJobs())
		# Write metadata to allow reconstruction of data splitter
		meta = {'ClassName': self.__class__.__name__}
		meta.update(self._protocol)
		DataSplitterIO.createInstance('DataSplitterIOAuto').saveState(path, meta, source, sourceLen, message)


	def importState(self, path):
		self.splitSource = DataSplitterIO.createInstance('DataSplitterIOAuto').loadState(path)


	def loadState(path, cfg = None):
		src = DataSplitterIO.createInstance('DataSplitterIOAuto').loadState(path)
		if cfg is None:
			cfg = createConfig(configDict = src.metadata)
		splitter = DataSplitter.createInstance(src.classname, cfg)
		splitter.splitSource = src
		# Transfer config protocol (in case no split function is called)
		splitter._protocol = src.metadata['None']
		for section in ifilter(identity, src.metadata):
			def meta2prot(k, v):
				return ('[%s] %s' % (section.replace('None ', ''), k), v)
			splitter._protocol.update(dict(ismap(meta2prot, src.metadata[section].items())))
		return splitter
	loadState = staticmethod(loadState)

makeEnum(['Dataset', 'Locations', 'NEntries', 'Skipped', 'FileList', 'Nickname', 'DatasetID',
	'CommonPrefix', 'Invalid', 'BlockName', 'MetadataHeader', 'Metadata', 'Comment'], DataSplitter)
