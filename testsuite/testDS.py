import copy, random
from testFwk import create_config
from grid_control import utils
from grid_control.datasets import DataProvider, DataSplitter
from grid_control.utils.parsing import str_dict
from python_compat import ifilter, imap, irange, izip, lmap, md5_hex, set, sorted


def mkmd5(x):
	return md5_hex(repr(x))

def create_source(datadict_list, settings = None):
	dp_list = []
	config_dict = {'dataset': settings or {}}
	for (name, datadict) in datadict_list:
		config_dict['datasource %s' % name] = datadict
	config = create_config(config_dict = config_dict).change_view(setSections = ['dataset'])
	for (name, datadict) in datadict_list:
		dp_list.append(DataProvider.create_instance('ConfigDataProvider', config, 'dataset', name))
	provider = DataProvider.get_class('MultiDatasetProvider')(config, 'dataset', None, None, dp_list)
	provider.testsuite_config = config
	return provider

def getRandomDatasets(nDS = 9, nBlocks = 100, nFiles = 99, nEvents = 12500, nSE = 6, nSETotal = 10,
		nDirPrefixes = 4, nDirPrefixesTotal = 9, dsSeed = 0, blockSeedCounter = 1512, wildSeed = 0):

	random.seed(dsSeed)
	ds = lmap(lambda x: '/dataset/DS_%s' % mkmd5(random.random())[:16], irange(nDS))
	prefixes = lmap(lambda x: 'DIR_%s' % mkmd5(x)[:10], irange(nDirPrefixesTotal))
	SEs = lmap(lambda x: 'SE_%04d' % x, irange(nSETotal))

	for nBlock in irange(nBlocks):
		random.seed(nBlock + blockSeedCounter + dsSeed + random.randint(0, wildSeed))
		dsName = random.choice(ds)
		block_name = mkmd5(random.random())[:8]
		pf = random.sample(prefixes, random.randint(1, nDirPrefixes))
		selist = random.sample(SEs, random.randint(0, nSE))
		if random.random() < 0.05:
			selist = None
		flist = lmap(lambda x: {
			DataProvider.URL: '/%s/FILE_%s' % (str.join('/', pf), mkmd5(random.random())),
			DataProvider.NEntries: random.randint(1, nEvents)
		}, irange(random.randint(1, nFiles)))
		yield {
			DataProvider.Dataset: dsName,
			DataProvider.BlockName: block_name,
			DataProvider.Locations: selist,
			DataProvider.FileList: flist
		}

def printSplit(x, meta = False):
	head = [(10, 'x'), (DataSplitter.NEntries, 'SIZE'), (DataSplitter.Skipped, 'SKIP'), (DataSplitter.FileList, 'Files')]
	if meta:
		head.append((DataSplitter.Metadata, 'Metadata'))
		head.append((DataSplitter.Invalid, 'Invalid'))
	sinfo = lmap(lambda id_part: dict(list(id_part[1].items()) + [(10, id_part[0])]), enumerate(x.iter_partitions()))
	utils.display_table(head, sinfo, 'cccll')

def getLFNMap(src = None, blocks = None):
	result = {}
	if src:
		blocks = src.get_block_list_cached(show_stats = False)
	for b in blocks:
		for fi in b[DataProvider.FileList]:
			result[fi[DataProvider.URL]] = abs(fi[DataProvider.NEntries])
	return result

def printSplitNice(sp, src, intro = True, printIdx = False, printComment = False, reuse = None, printLocation = False):
	last = 0
	charSrc = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
	allFiles = set()
	for partition in sp.iter_partitions():
		allFiles.update(partition[DataSplitter.FileList])
	assert(len(allFiles) <= len(charSrc))
	if reuse is not None:
		allFiles = allFiles.difference(list(reuse.keys()))
		for char in reuse.values():
			charSrc = charSrc.replace(char, '')
		charMap = dict(reuse)
		charMap.update(dict(izip(sorted(allFiles), charSrc)))
	else:
		charMap = dict(izip(sorted(allFiles), charSrc))
	if not intro:
		for lfn in sorted(src):
			print('%s: %s' % (lfn, charMap.get(lfn, '?')))
	fileoffset = {}
	for partition_num, partition in enumerate(sp.iter_partitions()):
		if printLocation:
			print('sites = %r' % partition.get(DataSplitter.Locations))
		if not partition[DataSplitter.FileList]:
			if partition.get(DataSplitter.Invalid):
				msg = '<disabled partition without files>'
			else:
				msg = '<partition without files>'
			print(msg)
			continue
		fm = dict(imap(lambda x: (x, charMap[x]), partition[DataSplitter.FileList]))
		seen = []
		for lfn in fm:
			if fm[lfn] in seen:
				fm[lfn] = fm[lfn].lower()
			seen.append(fm[lfn])
		allev = sum(imap(lambda fi: src.get(fi, 0), partition[DataSplitter.FileList]))
		pos = 0
		value = ''
		if printIdx:
			if partition.get(DataSplitter.Invalid):
				value += '%04d: ' % partition_num
			else:
				value += '%4d: ' % partition_num
		for lfn in partition[DataSplitter.FileList]:
			if lfn not in fileoffset:
				fileoffset[lfn] = pos
			if lfn not in src:
				value += '!'
				pos += 1
				continue
			while pos < fileoffset[lfn]:
				value += ' '
				pos += 1
			value += fm[lfn] * src[lfn]
			pos += src[lfn]
		value += '  => %d' % allev

		if partition.get(DataSplitter.Invalid):
			value += '    <disabled>'
		if partition.get(DataSplitter.Comment) and printComment:
			value += ' [%s]' % partition.get(DataSplitter.Comment)
		if intro:
			if printIdx:
				for fi in partition[DataSplitter.FileList]:
					print('      %s: %s' % (fi, fm[fi]))
			else:
				print(str.join(', ', imap(lambda fi: '%s: %s' % (fi, fm[fi]), partition[DataSplitter.FileList])))
		print(value)
		left = allev - partition.get(DataSplitter.Skipped, 0) - abs(partition[DataSplitter.NEntries])
		firstFile = partition[DataSplitter.FileList][0]
		msg = ''
		if printIdx:
			msg = '     '
		msg += ' ' * (fileoffset[firstFile] + partition.get(DataSplitter.Skipped, 0))
		msg += '-' * abs(partition[DataSplitter.NEntries])
		msg += ' ' * left
		msg += '  => %d,%d' % (partition.get(DataSplitter.Skipped, 0), partition[DataSplitter.NEntries])
		print(msg)
	if reuse is not None:
		return charMap

def checkCoverage(splitting, datasrc):
	sizeMap = getLFNMap(blocks = datasrc)
	coverMap = {}
	for lfn in sizeMap:
		coverMap[lfn] = lmap(lambda x: [], irange(sizeMap[lfn]))

	try:
		for (splitNum, si) in enumerate(splitting.iter_partitions()):
			try:
				si = splitting.get_partition(splitNum)
				if si.get(DataSplitter.Invalid):
					continue

				posSplit = 0
				posLFN = si.get(DataSplitter.Skipped, 0)
				for lfn in si[DataSplitter.FileList]:
					while (posSplit < si[DataSplitter.NEntries]) and (posLFN < sizeMap[lfn]):
						coverMap[lfn][posLFN].append(splitNum)
						posSplit += 1
						posLFN += 1
					posLFN = 0
				assert(posSplit == si[DataSplitter.NEntries])
			except:
				msg = 'Invalid splitting!'
				msg += ' splitNum %d' % splitNum
				msg += ' posSplit %d' % posSplit
				msg += ' nEv %d' % si[DataSplitter.NEntries]
				msg += ' posLFN %s' % posLFN
				msg += ' %s' % si
				print(msg)
				raise

		failed = []
		for lfn in coverMap:
			try:
				splitCoverage = lmap(len, coverMap[lfn])
				assert(min(splitCoverage) == 1)
				assert(max(splitCoverage) == 1)
			except:
				failed.append((lfn, coverMap[lfn]))
		if failed:
			for lfn, m in failed:
				print('problem with %s %s' % (lfn, m))
			raise Exception()
	except Exception:
		print('Problem found!')
		printSplitNice(splitting, sizeMap, True, True, True)
		raise

def ss2bl(ss):
	files = set()
	for x in ss:
		if x.strip() != '-' and x.strip():
			files.add(x)
	block = {DataProvider.Dataset: 'Dataset', DataProvider.BlockName: 'Block'}
	block[DataProvider.FileList] = lmap(lambda lfn: {DataProvider.URL: lfn, DataProvider.NEntries: ss.count(lfn)}, sorted(files))
	return [block]

def modDS(ds, modstr):
	modDict = {}
	for mod in modstr.split():
		lfn, newSize = mod.split(':')
		modDict[lfn] = int(newSize)

	usedLFN = []
	newFileList = []
	ds = copy.deepcopy(ds)
	for fi in ds[0][DataProvider.FileList]:
		usedLFN.append(fi[DataProvider.URL])
		fi[DataProvider.NEntries] = modDict.get(fi[DataProvider.URL], fi[DataProvider.NEntries])
		if fi[DataProvider.NEntries]:
			newFileList.append(fi)

	for unusedLFN in ifilter(lambda lfn: lfn not in usedLFN, modDict):
		newFileList.append({DataProvider.URL: unusedLFN, DataProvider.NEntries: modDict[unusedLFN]})
	ds[0][DataProvider.FileList] = newFileList
	return ds

def display_source(src, keys = None, show_stats = False):
	first = True
	for block in src.get_block_list_cached(show_stats = show_stats):
		if not first:
			print('====')
		first = False
		for key in sorted(block, key = lambda x: DataProvider.enum2str(x)):
			if keys and key not in keys:
				continue
			if key == DataProvider.FileList:
				entries = []
				for fi in block[DataProvider.FileList]:
					fentries = []
					for fkey in fi:
						if keys and fkey not in keys:
							continue
						if fkey == DataProvider.Metadata:
							fentries.append('Metadata={%s}' % str_dict(dict(izip(block[DataProvider.Metadata], fi[fkey]))))
						else:
							fentries.append('%s=%s' % (DataProvider.enum2str(fkey), fi[fkey]))
					fentries.sort()
					fentries.reverse()
					entries.append(str.join(' ', fentries))
				value = entries
			elif key == DataProvider.Metadata:
				value = '{%s}' % str.join(', ', sorted(block[key]))
			else:
				value = block[key]
			print('%-10s = %s' % (DataProvider.enum2str(key), value))

def create_scan(start, scanners = None, settings = None, restrict_keys = True, provider = 'ScanProvider'):
	config_dict = {'dataset': settings or {}}
	config_dict['dataset']['dataset processor'] = 'sort'
	config_dict['dataset']['dataset sort'] = True
	config_dict['dataset']['dataset files sort'] = True
	config_dict['dataset']['dataset block sort'] = True
	if scanners:
		config_dict['dataset']['scanner'] = scanners
	dp = DataProvider.create_instance(provider, create_config(config_dict = config_dict), 'dataset', start)
	keys = None
	if restrict_keys:
		keys = [DataProvider.Dataset, DataProvider.BlockName, DataProvider.Locations, DataProvider.FileList, DataProvider.URL, DataProvider.NEntries]
	display_source(dp, keys = keys)
