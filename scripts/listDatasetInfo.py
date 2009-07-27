#!/usr/bin/env python
import sys, os, signal, optparse

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.insert(0, os.path.join(_root, "..", 'python'))

# and include grid_control python module
from grid_control import *
import time

_verbosity = 0

def main(args):
	parser = optparse.OptionParser()
	parser.add_option("-l", "--list-datasets", dest="listdatasets", default=False, action="store_true")
	parser.add_option("-f", "--list-files",    dest="listfiles",    default=False, action="store_true")
	parser.add_option("-s", "--list-storage",  dest="liststorage",  default=False, action="store_true")
	parser.add_option("-b", "--list-blocks",   dest="listblocks",   default=False, action="store_true")
	parser.add_option("-c", "--config-entry",  dest="configentry",  default=False, action="store_true")
	parser.add_option("-S", "--save",          dest="save",         default=False, action="store_true")
	(opts, args) = parser.parse_args()

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		return 1

	class ConfigDummy(object):
		def get(self, x,y,z):
			return z
		def getPath(self, x,y,z):
			return z

	if os.path.exists(args[0]):
		fromfile = True
		dir, file = os.path.split(args[0])
		provider = DataProvider.loadState(ConfigDummy(), dir, file)
	else:
		fromfile = False
		provider = DataProvider.open('DBSApiv2', ConfigDummy(), args[0], None)
	blocks = provider.getBlocks()

	def unique(seq): 
		set = {} 
		map(set.__setitem__, seq, []) 
		return set.keys()

	datasets = unique(map(lambda x: x[DataProvider.Dataset], blocks))
	if len(datasets) > 1:
		headerbase = [(DataProvider.Dataset, "Dataset")]
	else:
		print "Dataset: %s" % blocks[0][DataProvider.Dataset]
		headerbase = []

	if opts.configentry:
		infos = {}
		maxnick = 0
		for block in blocks:
			blockID = block.get(DataProvider.DatasetID, 0)
			if not infos.get(blockID, None):
				infos[blockID] = {
					DataProvider.Nickname : block[DataProvider.Nickname],
					DataProvider.Dataset : block[DataProvider.Dataset]
				}
				maxnick = max(maxnick, len(block[DataProvider.Nickname]))
#		printTabular([(DataProvider.Nickname, "Nickname"), (DataProvider.Dataset, "Dataset")], infos.itervalues())
		for info in infos.itervalues():
			print "%s: DBS:%s" % (info[DataProvider.Nickname].center(maxnick + 2), info[DataProvider.Dataset])


	if opts.listdatasets:
		infos = {}
		infosum = {
			DataProvider.NEvents : 0,
			DataProvider.Dataset : 'Sum'
		}
		order = []
		for block in blocks:
			blockID = block.get(DataProvider.Dataset, '')
			if not infos.get(blockID, None):
				order.append(blockID)
				infos[blockID] = {
					DataProvider.NEvents : 0,
					DataProvider.Dataset : block[DataProvider.Dataset]
				}
			infos[blockID][DataProvider.NEvents] += block[DataProvider.NEvents]
			infosum[DataProvider.NEvents] += block[DataProvider.NEvents]
		utils.printTabular([(DataProvider.Dataset, "Dataset"), (DataProvider.NEvents, "Events")],
			map(lambda x: infos[x], order) + [None, infosum])

	if opts.listfiles:
		for block in blocks:
			if len(datasets) > 1:
				print "Dataset: %s" % block[DataProvider.Dataset]
			print "Blockname: %s" % block[DataProvider.BlockName]
			utils.printTabular([(DataProvider.lfn, "Filename"), (DataProvider.NEvents, "Events")], block[DataProvider.FileList])
			print

	if opts.liststorage:
		infos = {}
		for block in blocks:
			blockID = block.get(DataProvider.DatasetID, 0)
			if not infos.get(blockID, None):
				infos[blockID] = {1:1}
				if len(headerbase) > 0:
					print "Dataset: %s" % block[DataProvider.Dataset]
				for se in block[DataProvider.SEList]:
					print "\t%s" % se

	if opts.listblocks:
		utils.printTabular(headerbase + [(DataProvider.BlockName, "Block"), (DataProvider.NEvents, "Events")], blocks)

	if opts.save:
		provider.saveState(".", "datacache.dat")
		print "Dataset information saved to ./datacache.dat"

	# everything seems to be in order
	return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
