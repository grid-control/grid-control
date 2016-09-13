#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
import copy, random
from grid_control.utils.activity import Activity
from hpfwk import Plugin
from testDS import checkCoverage, getRandomDatasets
from testResync import doResync, do_initSplit
from python_compat import irange, lmap, md5_hex


DataSplitter = Plugin.get_class('DataSplitter')
DataProvider = Plugin.get_class('DataProvider')

def modifyBlock(block, seed = 0):
	block = copy.deepcopy(block)
	random.seed(seed)
	rng = lambda x: random.random() < x
	fl = block[DataProvider.FileList]
	if rng(0.05):
		fl = list(random.sample(fl, max(1, len(fl) - random.randint(0, 4))))
	avgEvents = 0
	for fi in fl:
		if rng(0.1):
			if rng(0.5):
				fi[DataProvider.NEntries] += random.randint(0, 5)
			else:
				fi[DataProvider.NEntries] -= random.randint(0, 5)
			fi[DataProvider.NEntries] = max(1, fi[DataProvider.NEntries])
		avgEvents += fi[DataProvider.NEntries]
	avgEvents = int(avgEvents / len(fl))
	if rng(0.05):
		for x in irange(random.randint(0, 4)):
			lfn = fl[0][DataProvider.URL].split('FILE')[0]
			lfn += 'FILE_%s' % md5_hex(str(random.random()))
			nev = random.randint(max(1, avgEvents - 5), max(1, avgEvents + 5))
			fl.append({DataProvider.URL: lfn, DataProvider.NEntries: nev})
	if rng(0.1):
		random.shuffle(fl)
	block[DataProvider.FileList] = fl
	return block

def cascade_lb(nDS = 2, nMod = 2, nEvents = 2000):
	# Check large
	print("multiple blocks - Cascaded resyncs...")
	activity = Activity('Sync test')
	for t in irange(nDS):
		initialData = list(getRandomDatasets(nDS = 5, nBlocks = 10, nFiles = 50, nEvents = nEvents, nSE = 3, dsSeed = t + 123))

		# Check for cascaded resyncs
		DataProvider.save_to_file('data-old.dbs', initialData)
		do_initSplit(False, 200)
		checkCoverage(DataSplitter.load_partitions_for_script("datamap-resync.tar"), initialData)

		modified = initialData
		for x in irange(nMod):
			activity.update('Sync test %s/%s %s/%s' % (t, nDS, x, nMod))
			modified = lmap(lambda b: modifyBlock(b, x + 1000*t), modified)
			DataProvider.save_to_file('data-new.dbs', modified)
#			DataProvider.save_to_file('data-new.dbs.tmp', modified)
			doResync(False)
			try:
				checkCoverage(DataSplitter.load_partitions_for_script("datamap-resync.tar"), modified)
			except Exception:
				print("Resync id %d %d" % (t, x))
				raise
	activity.finish()

def cascade_1b(nTests = 1000, nEvents = 15):
	initialData = list(getRandomDatasets(nDS = 1, nBlocks = 1, nFiles = 50, nEvents = nEvents, nSE = 1, dsSeed = 1))

	# Check for cascaded resyncs
#	DataProvider.save_to_file('data-ori.dbs', initialData)
	DataProvider.save_to_file('data-old.dbs', initialData)
	do_initSplit(False, 5)
	checkCoverage(DataSplitter.load_partitions_for_script("datamap-resync.tar"), initialData)

#	print "1 block - Cascaded resyncs..."
	activity = Activity('Sync test')
	modified = initialData
	for x in irange(nTests):
		activity.update('Sync test %s/%s' % (x, nTests))
		modified = lmap(lambda b: modifyBlock(b, x), modified)
		DataProvider.save_to_file('data-new.dbs', modified)
#		DataProvider.save_to_file('data-new.dbs.%d' % x, modified)
		doResync(False)
		try:
			checkCoverage(DataSplitter.load_partitions_for_script("datamap-resync.tar"), modified)
		except Exception:
			print("Resync id %d" % x)
			raise
	activity.finish()

def single_1b(nTests = 1000, nEvents = 15):
	initialData = list(getRandomDatasets(nDS = 1, nBlocks = 1, nFiles = 50, nEvents = nEvents, nSE = 1, dsSeed = 1))

	# Check for single resyncs
	print("1 block - Single resyncs...")
	activity = Activity('Sync test')
	for x in irange(nTests):
		activity.update('Sync test %s/%s' % (x, nTests))
		DataProvider.save_to_file('data-old.dbs', initialData)
		DataProvider.save_to_file('data-ori.dbs', initialData)
		do_initSplit(False, 5)
		checkCoverage(DataSplitter.load_partitions_for_script("datamap-resync.tar"), initialData)

		modified = lmap(lambda b: modifyBlock(b, x), initialData)
		DataProvider.save_to_file('data-new.dbs', modified)
#		DataProvider.save_to_file('data-new.dbs.%d' % x, modified)
		doResync(False)
		try:
			checkCoverage(DataSplitter.load_partitions_for_script("datamap-resync.tar"), modified)
		except Exception:
			print("Resync seed %d" % x)
			print(initialData)
			print(modified)
			raise
	activity.finish()

cascade_1b(20, 1)
single_1b(100, 1)
cascade_lb(nDS = 5, nMod = 20, nEvents = 1)

single_1b(100)
cascade_1b(20)
cascade_lb(nDS = 2, nMod = 20)

single_1b(5)
cascade_1b(2)
cascade_lb(nDS = 2, nMod = 10, nEvents = 10)
