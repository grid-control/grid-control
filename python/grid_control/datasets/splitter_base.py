from python_compat import *
import os, tarfile, time, copy, cStringIO
from grid_control import AbstractObject, AbstractError, RuntimeError, utils, ConfigError, Config
from provider_base import DataProvider

class DataSplitter(AbstractObject):
	splitInfos = ['Dataset', 'SEList', 'NEvents', 'Skipped', 'FileList', 'Nickname', 'DatasetID',
		'CommonPrefix', 'Invalid', 'BlockName', 'MetadataHeader', 'Metadata']
	for idx, splitInfo in enumerate(splitInfos):
		locals()[splitInfo] = idx

	def __init__(self, config, section = None):
		self.splitSource = None
		self._section = section
		self._protocol = {}
		self._job0Cache = None
		self._jobCache = (None, None)


	def setup(self, func, item, default = None):
		try:
			self._protocol[item] = func(self._section, item, default)
		except:
			# Support for older job mappings
			self._protocol[item] = func(self._section, item.replace(' ', ''), default)
		return self._protocol[item]


	def neededVars(self):
		return [DataSplitter.FileList]


	def cpBlockInfoToJob(self, block, job):
		for prop in ['Dataset', 'BlockName', 'DatasetID', 'Nickname', 'SEList']:
			if getattr(DataProvider, prop) in block:
				job[getattr(DataSplitter, prop)] = block[getattr(DataProvider, prop)]
		if DataProvider.Metadata in block:
			job[DataSplitter.MetadataHeader] = block[DataProvider.Metadata]
		return job


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		raise AbstractError


	def splitDataset(self, path, blocks):
		log = utils.ActivityLog('Splitting dataset into jobs')
		self.saveState(path, self.splitDatasetInternal(blocks))
		self.splitSource = DataSplitter.loadStateInternal(path)


	def getSplitInfo(self, jobNum):
		# Special handling of job 0 - most wanted jobinfo ever
		if jobNum == 0:
			if self._job0Cache == None:
				self._job0Cache = self.splitSource[jobNum]
			return self._job0Cache
		if self._jobCache[0] != jobNum:
			if jobNum >= self.getMaxJobs():
				raise ConfigError('Job %d out of range for available dataset'  % jobNum)
			self._jobCache = (jobNum, self.splitSource[jobNum])
		return self._jobCache[1]


	def getMaxJobs(self):
		return self.splitSource.maxJobs


	def printInfoForJob(job):
		utils.vprint(('Dataset: %s' % job[DataSplitter.Dataset]).ljust(50), -1, newline = False)
		utils.vprint(('Events: %d' % job[DataSplitter.NEvents]).ljust(20), -1, newline = False)
		utils.vprint('  ID: %s' % job.get(DataSplitter.DatasetID, 0), -1)
		utils.vprint(('  Block: %s' % job[DataSplitter.BlockName]).ljust(50), -1, newline = False)
		utils.vprint(('  Skip: %d' % job[DataSplitter.Skipped]).ljust(20), -1, newline = False)
		if job.get(DataSplitter.Nickname, '') != '':
			utils.vprint('Nick: %s' % job[DataSplitter.Nickname], -1)
		if job.get(DataSplitter.SEList, None) != None:
			seArray = map(lambda x: str.join(', ', x), utils.lenSplit(job[DataSplitter.SEList], 70))
			utils.vprint(' SEList: %s' % str.join('\n         ', seArray), -1)
		for idx, head in enumerate(job.get(DataSplitter.MetadataHeader, [])):
			oneFileMetadata = map(lambda x: repr(x[idx]), job[DataSplitter.Metadata])
			metadataArray = map(lambda x: str.join(', ', x), utils.lenSplit(oneFileMetadata, 70))
			utils.vprint('%7s: %s' % (head, str.join('\n         ', metadataArray)), -1)
		utils.vprint('  Files: ', -1, newline = False)
		if utils.verbosity() > 2:
			utils.vprint(str.join('\n         ', job[DataSplitter.FileList]), -1)
		else:
			utils.vprint('%d files selected' % len(job[DataSplitter.FileList]), -1)
	printInfoForJob = staticmethod(printInfoForJob)


	def printAllJobInfo(self):
		for jobNum in range(self.getMaxJobs()):
			utils.vprint('Job number: %d' % jobNum, -1)
			DataSplitter.printInfoForJob(self.getSplitInfo(jobNum))
			utils.vprint('------------', -1)


	def resyncMapping(self, newSplitPath, oldBlocks, newBlocks, config):
		log = utils.ActivityLog('Resynchronization of dataset blocks')
		(blocksAdded, blocksMissing, blocksChanged) = DataProvider.resyncSources(oldBlocks, newBlocks)
		debug = open('%d' % time.time(), 'w')
		debug.write('\n\nADDED\n')
		debug.write(repr(blocksAdded))
		debug.write('\n\nMISSING\n')
		debug.write(repr(blocksMissing))
		debug.write('\n\nCHANGED\n')
		debug.write(repr(blocksChanged))
		del log

		# Variables for later
		(splitAdded, splitProcList, splitProcMode) = ([], {}, {})
		class Resync:
			enum = ['disable', 'append', 'replace', 'ignore']
			for idx, var in enumerate(enum):
				locals()[var] = idx

		interactive = config.getBool('dataset', 'resync interactive', True, volatile = True)

		# Get processing mode (interactively)
		def getMode(item, default, desc):
			def parser(x):
				for opt in desc:
					if (x.lower() == Resync.enum[opt]) or (x.lower() == Resync.enum[opt][0]):
						return opt
			if not interactive:
				value = Resync.__dict__[config.get('dataset', 'resync mode %s' % item, Resync.enum[default], volatile = True).lower()]
				if value in desc.keys():
					return value
			utils.vprint(level = -1)
			for choice in desc:
				utils.vprint('  %s)' % Resync.enum[choice].rjust(max(map(lambda x: len(Resync.enum[x]), desc.keys()))), desc[choice], -1)
			return utils.getUserInput('\nPlease select how to proceed!', Resync.enum[default], desc.keys(), parser)

		# Select processing mode for job (disable > append > replace) [ie. disable overrides all]
		setMode = min

		# Get Dictionary with affected jobs and corresponding old+new fileinfos
		def getAffectedJobs(blockTuple):
			(result, affectedOldInfos) = ({}, set())
			for jobNum in range(self.getMaxJobs()):
				jobLFNs = self.getSplitInfo(jobNum)[DataSplitter.FileList]
				(jobOldFileInfos, jobNewFileInfos) = ([], [])
				for (old, new) in blockTuple:
					jobOldFileInfos.extend(filter(lambda x: x[DataProvider.lfn] in jobLFNs, old.get(DataProvider.FileList, [])))
					jobNewFileInfos.extend(filter(lambda x: x[DataProvider.lfn] in jobLFNs, new.get(DataProvider.FileList, [])))
				if jobOldFileInfos != []:
					result[jobNum] = (jobOldFileInfos, jobNewFileInfos, new)
					affectedOldInfos.update(map(lambda x: x[DataProvider.lfn], jobOldFileInfos))
			return (result, len(affectedOldInfos))

		# Add to processing chain
		def addSplitProc(jobDict, mode):
			if mode != Resync.ignore:
				for jobNum in jobDict:
					splitProcMode[jobNum] = setMode(splitProcMode.get(jobNum, mode), mode)
					if mode != Resync.disable:
						splitProcList.setdefault(jobNum, []).append(jobDict[jobNum])
			utils.vprint(level = -1)

		# Function for fast access to file infos via lfn
		def getFileDict(blocks):
			result = {}
			for block in blocks:
				for fileInfo in block[DataProvider.FileList]:
					result[fileInfo[DataProvider.lfn]] = fileInfo
			return result
		oldFileInfoMap = getFileDict(oldBlocks)

		# Return lists with expanded / shrunk files
		# Input is the affected set taken from newBlocks
		# Return block compatible with (newBlocks, oldBlocks)
		def splitAndClassifyChanges(changedBlocks):
			def getCorrespondingBlock(blockA, allBlocks):
				for blockB in allBlocks:
					if blockA[DataProvider.Dataset] == blockB[DataProvider.Dataset] and \
						blockA[DataProvider.BlockName] == blockB[DataProvider.BlockName]:
						return blockB
				raise RuntimeError('Block %s not found!' % str(blockA))

			(shrunken, expanded) = ([], [])
			for changedBlock in changedBlocks:
				oldBlock = getCorrespondingBlock(changedBlock, oldBlocks)
				for changedFileInfo in changedBlock[DataProvider.FileList]:
					# Search old info and create new block container for return value
					oldFileInfo = oldFileInfoMap[changedFileInfo[DataProvider.lfn]]
					def copyBlock(orig, files):
						result = copy.copy(orig)
						result[DataProvider.FileList] = files
						result[DataProvider.NEvents] = sum(map(lambda x: x[DataProvider.NEvents], files))
						return result
					copyOldBlock = copyBlock(oldBlock, [oldFileInfo])
					copyNewBlock = copyBlock(changedBlock, [changedFileInfo])
					# Classify into expanded and shrunken
					if oldFileInfo[DataProvider.NEvents] < changedFileInfo[DataProvider.NEvents]:
						expanded.append((copyOldBlock, copyNewBlock))
					elif oldFileInfo[DataProvider.NEvents] > changedFileInfo[DataProvider.NEvents]:
						shrunken.append((copyOldBlock, copyNewBlock))
			return (shrunken, expanded)

		def descBuilder(x, y = 'with', ignore = True):
			desc = { Resync.disable: 'Disable jobs with %s files' % x,
				Resync.replace: 'Replace existing jobs with jobs %s %s files' % (y, x),
				Resync.append: 'Disable existing jobs and append these jobs %s %s files' % (y, x) }
			if ignore:
				desc[Resync.ignore] = 'Ignore %s parts of files' % x
			return desc

		# User overview and setup starts here
		if blocksAdded or blocksMissing or blocksChanged:
			utils.vprint('The following changes in the dataset information were detected:\n', -1)

		head = [(DataProvider.Dataset, 'Dataset'), (DataProvider.BlockName, 'Block'),
			(DataProvider.NEvents, 'Events'), (DataProvider.FileList, 'Files')]

		if blocksAdded:
			utils.vprint('=' * 15 + 'Added files'.center(15) + '=' * 15, -1)
			utils.printTabular(head, blocksAdded, 'rlcc', {DataProvider.FileList: lambda x: len(x)})
			NFiles = sum(map(lambda x: len(x[DataProvider.FileList]), blocksAdded))
			addedJobs = list(self.splitDatasetInternal(blocksAdded))
			output = (NFiles, len(blocksAdded), len(addedJobs))
			utils.vprint('\n%d files in %d blocks were added, which corresponds to %d new jobs' % output, -1)
			desc = {Resync.ignore: 'Ignore these new jobs', Resync.append: 'Append new jobs to existing task'}
			if getMode('new', Resync.append, desc) == Resync.append:
				splitAdded.extend(addedJobs)
			utils.vprint(level = -1)

		if blocksMissing:
			utils.vprint('=' * 15 + 'Missing files'.center(15) + '=' * 15, -1)
			utils.printTabular(head, blocksMissing, 'rlcc', {DataProvider.FileList: lambda x: len(x)})
			(removeJobs, NFiles) = getAffectedJobs(map(lambda x: (x, {}), blocksMissing))
			utils.vprint('\n%d files in %d blocks are missing.' % (NFiles, len(blocksMissing)), -1)
			utils.vprint('This affects the following %d jobs:' % len(removeJobs), sorted(removeJobs.keys()), -1)
			addSplitProc(removeJobs, getMode('removed', Resync.append, descBuilder('missing', 'without', False)))

		if blocksChanged:
			utils.vprint('=' * 15 + 'Changed files'.center(15) + '=' * 15, -1)
			(changedJobs, NFiles) = getAffectedJobs(map(lambda x: (x, {}), blocksChanged))
			utils.vprint('%d files in %d blocks have changed their length' % (NFiles, len(blocksChanged)), -1)
			utils.vprint('This affects the following %d jobs:' % len(changedJobs), sorted(changedJobs.keys()), -1)
			utils.vprint(level = -1)

		# Did the file shrink or expand?
		blocksShrunken, blocksExpanded = splitAndClassifyChanges(blocksChanged)

		def getChangeOverview(blocks, title):
			utils.vprint('-' * 15 + ('%s files' % title).center(15) + '-' * 15, -1)
			(changedJobs, NFiles) = getAffectedJobs(blocksExpanded)
			utils.vprint('%d files have %s in size:' % (NFiles, title.lower()), -1)
			utils.vprint('This affects the following %d jobs: %s' % (len(changedJobs), sorted(changedJobs.keys())), -1)
			utils.vprint(level = -1)
			head = [(DataProvider.Dataset, 'Dataset'), (DataProvider.BlockName, 'Block'),
				(None, 'Events (old)'), (DataProvider.NEvents, 'Events (new)'), (DataProvider.FileList, 'Files')]
			utils.printTabular(head, map(lambda x: dict(x[0].items() + [(None, x[1][DataProvider.NEvents])]),
				blocks), 'rlcc', {DataProvider.FileList: lambda x: len(x)})
			return changedJobs

		if blocksExpanded:
			expandJobs = getChangeOverview(blocksExpanded, 'Expanded')
			desc = descBuilder('expanded')
			if DataSplitter.Skipped in self.neededVars():
				desc[Resync.append] += ' (Try to append expanded parts as new jobs)'
			addSplitProc(expandJobs, getMode('expand', Resync.append, desc))

		if blocksShrunken:
			shrinkJobs = getChangeOverview(blocksExpanded, 'Expanded')
			addSplitProc(shrinkJobs, getMode('shrink', Resync.append, descBuilder('shrunken')))

		if interactive and (splitAdded or splitProcList):
			preserve = utils.getUserBool('Preserve unchanged splittings with changed files?', True)
			reorder = utils.getUserBool('Reorder jobs to close gaps?', True)
		else:
			preserve = config.getBool('dataset', 'resync preserve', True, volatile = True)
			reorder = config.getBool('dataset', 'resync reorder', False, volatile = True)

		# ^^ Still not sure about the degrees of freedom ^^
		#     User setup is finished starting from here

		debug.write('\n\nADD\n')
		debug.write(repr(splitAdded))
		debug.write('\n\nPROC\n')
		debug.write(repr(splitProcList))
		debug.write('\n\nMODE\n')
		debug.write(repr(splitProcMode))

		# Process job modifications
		(result, resultRedo, resultDisable) = ([], [], [])

		# Zip rm+add on a file level: Input: [([rmlist], [addlist]),...] => [(rmfile, addfile),...]
		def sortedFlatReZip(modList):
			for rmEntry, addEntry, newBlock in modList:
				if not addEntry:
					addEntry = []
				addDict = dict(map(lambda x: (x[DataProvider.lfn], x), addEntry))
				for rmFile in rmEntry:
					if rmFile[DataProvider.lfn] in addDict:
						yield (rmFile, addDict.pop(rmFile[DataProvider.lfn]), newBlock)
					else:
						yield (rmFile, None, newBlock)

		# Apply modification list to old splitting
		# Input: oldSplit, modList = [(rmfile, addfile), ...], doExpandOutside
		# With doExpandOutside, gc tries to handle expanding files via the splitting function
		def processModList(oldSplit, modList, doExpandOutside):
			newSplit = copy.deepcopy(oldSplit)
			# Determine size infos and get started
			sizeInfo = map(lambda x: oldFileInfoMap[x][DataProvider.NEvents], newSplit[DataSplitter.FileList])

			for rm, add, newBlock in modList:
				try:
					idx = newSplit[DataSplitter.FileList].index(rm[DataProvider.lfn])
				except:
					continue

				def removeCompleteFile():
					newSplit[DataSplitter.NEvents] -= rm[DataProvider.NEvents]
					newSplit[DataSplitter.FileList].pop(idx)
					sizeInfo.pop(idx)

				def replaceCompleteFile():
					newSplit[DataSplitter.NEvents] += add[DataProvider.NEvents]
					newSplit[DataSplitter.NEvents] -= rm[DataProvider.NEvents]
					sizeInfo[idx] = add[DataProvider.NEvents]

				def expandOutside():
					fileList = newBlock.pop(DataProvider.FileList)
					newBlock[DataProvider.FileList] = [add]
					splitAdded.extend(self.splitDatasetInternal([newBlock], rm[DataProvider.NEvents]))
					newBlock[DataProvider.FileList] = fileList
					sizeInfo[idx] = add[DataProvider.NEvents]

				if idx == 0:
					# First file is affected
					if add and (add[DataSplitter.NEvents] > newSplit[DataSplitter.Skipped]):
						# First file changes and still lives in new splitting
						following = sizeInfo[0] - newSplit[DataSplitter.Skipped] - newSplit[DataProvider.NEvents]
						shrinkage = rm[DataProvider.NEvents] - add[DataProvider.NEvents]
						if following > 0:
							# First file not completely covered by current splitting
							if following < shrinkage:
								# Covered area of first file shrinks
								newSplit[DataSplitter.NEvents] += following
								replaceCompleteFile()
							else:
								# First file changes outside of current splitting
								sizeInfo[idx] = add[DataProvider.NEvents]
						else:
							# Change of first file ending in current splitting - One could try to
							# 'reverse fix' expanding files to allow expansion via adding only the expanding part
							replaceCompleteFile()
					else:
						# Removal of first file from current splitting
						newSplit[DataSplitter.NEvents] += max(0, sizeInfo[idx] - newSplit[DataSplitter.Skipped] - newSplit[DataSplitter.NEvents])
						newSplit[DataSplitter.NEvents] += newSplit[DataSplitter.Skipped]
						newSplit[DataSplitter.Skipped] = 0
						removeCompleteFile()

				elif idx == len(newSplit[DataSplitter.FileList]) - 1:
					# Last file is affected
					if add:
						coverLast = newSplit[DataSplitter.Skipped] + newSplit[DataProvider.NEvents] - sum(sizeInfo[:-1])
						if coverLast == rm[DataProvider.NEvents]:
							# Change of last file, which ends in current splitting
							if doExpandOutside and (rm[DataProvider.NEvents] < add[DataProvider.NEvents]):
								expandOutside()
							else:
								replaceCompleteFile()
						elif coverLast > add[DataProvider.NEvents]:
							# Change of last file, which changes current coverage
							newSplit[DataSplitter.NEvents] -= coverLast
							newSplit[DataSplitter.NEvents] += rm[DataProvider.NEvents]
							replaceCompleteFile()
						else:
							# Change of last file outside of current splitting
							sizeInfo[idx] = add[DataProvider.NEvents]
					else:
						# Removal of last file from current splitting
						newSplit[DataSplitter.NEvents] = sum(sizeInfo) - newSplit[DataSplitter.Skipped]
						removeCompleteFile()

				else:
					# File in the middle is affected - solution very simple :)
					if add:
						# Replace file - expanding files could be swapped to the (fully contained) end
						# to allow expansion via adding only the expanding part
						replaceCompleteFile()
					else:
						# Remove file
						removeCompleteFile()

			return newSplit

		firstFileSELookup = {}

		# Iterate over existing job splittings and modifiy them as specified
		doExpandOutside = DataSplitter.Skipped in self.neededVars()
		for jobNum in range(self.getMaxJobs()):
			splitInfo = self.getSplitInfo(jobNum)
			mode = splitProcMode.get(jobNum, None)
			if mode:
				modList = sortedFlatReZip(splitProcList[jobNum])

			# Create new splittings
			newSplitInfo = None
			if mode == Resync.append:
				newSplitInfo = processModList(splitInfo, modList, doExpandOutside)
			elif mode == Resync.replace:
				newSplitInfo = processModList(splitInfo, modList, False)

			# Quality control of new splittings
			if newSplitInfo:
				if len(newSplitInfo[DataSplitter.FileList]) == 0:
					mode = Resync.disable
				# Keep unchanged splittings
				if preserve and (newSplitInfo == splitInfo):
					mode = None

			# Sort jobs according to guidelines
			if mode == Resync.append:
				mode = Resync.disable
				splitAdded.append(newSplitInfo)
			if mode == Resync.replace:
				splitInfo = newSplitInfo
				resultRedo.append(jobNum)

			# Try to reassign job splittings or simply disable them
			if (mode == Resync.disable) or splitInfo.get(DataSplitter.Invalid, False):
				if reorder and len(splitAdded) > 0:
					splitInfo = splitAdded.pop()
					resultRedo.append(jobNum)
				elif mode == Resync.disable:
					resultDisable.append(jobNum)
					splitInfo[DataSplitter.Invalid] = True

			result.append(splitInfo)

			# To update SE list: Create mapping between first filename and splitting
			if len(splitInfo[DataSplitter.FileList]) > 0:
				firstFileSELookup[splitInfo[DataSplitter.FileList][0]] = splitInfo

		# Update SE list of jobs
		for block in newBlocks:
			for fileInfo in block[DataProvider.FileList]:
				if fileInfo[DataProvider.lfn] in firstFileSELookup:
					firstFileSELookup[fileInfo[DataProvider.lfn]][DataSplitter.SEList] = block.get(DataProvider.SEList, None)
					break

		for splitInfo in splitAdded:
			result.append(splitInfo)

		debug.write('\n\nREDO\n')
		debug.write(repr(resultRedo))
		debug.write('\n\nDISABLE\n')
		debug.write(repr(resultDisable))

		self.saveState(newSplitPath, result)
		return (resultRedo, resultDisable)


	# Save as tar file to allow random access to mapping data with little memory overhead
	def saveState(self, path, entries = None):
		tar = tarfile.open(path, 'w:')
		fmt = utils.DictFormat()
		source = (list(entries), self.splitSource)[entries == None] # list(): for status display

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
				log = utils.ActivityLog('Writing job mapping file [%d / %d]' % (jobNum, len(source)))
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
					return (x, y, str.join(",", z))
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
		meta = {'ClassName': self.__class__.__name__, 'MaxJobs': jobNum + 1}
		meta.update(self._protocol)
		info, file = utils.VirtualFile('Metadata', fmt.format(meta)).getTarInfo()
		tar.addfile(info, file)
		file.close()
		tar.close()


	def loadStateInternal(path):
		class JobFileTarAdaptor(object):
			def __init__(self, path):
				log = utils.ActivityLog('Reading job mapping file')
				self._fmt = utils.DictFormat()
				self._tar = tarfile.open(path, 'r:')
				(self._cacheKey, self._cacheTar) = (None, None)

				metadata = self._tar.extractfile('Metadata').readlines()
				self.metadata = self._fmt.parse(metadata, lowerCaseKey = False)
				self.maxJobs = self.metadata.pop('MaxJobs')
				self.classname = self.metadata.pop('ClassName')
				del log

			def __getitem__(self, key):
				if not self._cacheKey == key / 100:
					self._cacheKey = key / 100
					subTarFileObj = self._tar.extractfile('%03dXX.tgz' % (key / 100))
					self._cacheTar = tarfile.open(mode = 'r:gz', fileobj = subTarFileObj)
				data = self._fmt.parse(self._cacheTar.extractfile('%05d/info' % key).readlines())
				if DataSplitter.SEList in data:
					tmp = map(str.strip, data[DataSplitter.SEList].split(','))
					data[DataSplitter.SEList] = filter(lambda x: x != '', tmp)
				if DataSplitter.MetadataHeader in data:
					data[DataSplitter.MetadataHeader] = eval(data[DataSplitter.MetadataHeader])
					data[DataSplitter.Metadata] = eval(data[DataSplitter.Metadata])
				fileList = self._cacheTar.extractfile('%05d/list' % key).readlines()
				if DataSplitter.CommonPrefix in data:
					fileList = map(lambda x: '%s/%s' % (data[DataSplitter.CommonPrefix], x), fileList)
				data[DataSplitter.FileList] = map(str.strip, fileList)
				return data

		try:
			return JobFileTarAdaptor(path)
		except:
			raise ConfigError("No valid dataset splitting found in '%s'." % path)
	loadStateInternal = staticmethod(loadStateInternal)


	def loadState(path):
		src = DataSplitter.loadStateInternal(path)
		cfg = Config(configDict={None: src.metadata})
		splitter = DataSplitter.open(src.classname, cfg, section = None)
		splitter.splitSource = src
		return splitter
	loadState = staticmethod(loadState)

DataSplitter.dynamicLoaderPath()
