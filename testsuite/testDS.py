import copy
from testfwk import create_config
from grid_control.datasets import DataProvider, DataSplitter
from grid_control.utils.parsing import str_dict_linear
from grid_control.utils.table import ConsoleTable
from python_compat import ifilter, imap, irange, izip, lmap, set, sorted


def checkCoverage(reader, datasrc):
	sizeMap = get_lfn_map(blocks = datasrc)
	coverMap = {}
	for lfn in sizeMap:
		coverMap[lfn] = lmap(lambda x: [], irange(sizeMap[lfn]))

	try:
		for (splitNum, si) in enumerate(reader.iter_partitions()):
			try:
				si = reader.get_partition_checked(splitNum)
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
		display_partitions(sizeMap, reader.iter_partitions(), True, True, True)
		raise


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


def create_source(datadict_list, settings = None):
	dp_list = []
	config_dict = {'dataset': settings or {}}
	for (name, datadict) in datadict_list:
		config_dict['datasource %s' % name] = datadict
	config = create_config(config_dict = config_dict).change_view(set_sections = ['dataset'])
	for (name, datadict) in datadict_list:
		dp_list.append(DataProvider.create_instance('ConfigDataProvider', config, 'dataset', name))
	provider = DataProvider.get_class('MultiDatasetProvider')(config, 'dataset', None, None, dp_list)
	provider.testsuite_config = config
	return provider


def display_partitions(lfnmap, part_iter, intro = True, printIdx = False, printComment = False, reuse = None, printLocation = False):
	last = 0
	charSrc = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
	allFiles = set()
	part_list = list(part_iter)
	for partition in part_list:
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
		for lfn in sorted(lfnmap):
			print('%s: %s' % (lfn, charMap.get(lfn, '?')))
	fileoffset = {}
	for partition_num, partition in enumerate(part_list):
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
		allev = sum(imap(lambda fi: lfnmap.get(fi, 0), partition[DataSplitter.FileList]))
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
			if lfn not in lfnmap:
				value += '!'
				pos += 1
				continue
			while pos < fileoffset[lfn]:
				value += ' '
				pos += 1
			value += fm[lfn] * lfnmap[lfn]
			pos += lfnmap[lfn]
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


def display_reader(x, meta = False):
	head = [(10, 'x'), (DataSplitter.NEntries, 'SIZE'), (DataSplitter.Skipped, 'SKIP'), (DataSplitter.FileList, 'Files')]
	if meta:
		head.append((DataSplitter.Metadata, 'Metadata'))
		head.append((DataSplitter.Invalid, 'Invalid'))
	sinfo = lmap(lambda id_part: dict(list(id_part[1].items()) + [(10, id_part[0])]), enumerate(x.iter_partitions()))
	ConsoleTable.create(head, sinfo, 'cccll', title=None)


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
							fentries.append('Metadata={%s}' % str_dict_linear(dict(izip(block[DataProvider.Metadata], fi[fkey]))))
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


def get_lfn_map(src = None, blocks = None):
	result = {}
	if src:
		blocks = src.get_block_list_cached(show_stats = False)
	for b in blocks:
		for fi in b[DataProvider.FileList]:
			result[fi[DataProvider.URL]] = abs(fi[DataProvider.NEntries])
	return result


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


def ss2bl(ss):
	files = set()
	for x in ss:
		if x.strip() != '-' and x.strip():
			files.add(x)
	block = {DataProvider.Dataset: 'Dataset', DataProvider.BlockName: 'Block'}
	block[DataProvider.FileList] = lmap(lambda lfn: {DataProvider.URL: lfn, DataProvider.NEntries: ss.count(lfn)}, sorted(files))
	return [block]
