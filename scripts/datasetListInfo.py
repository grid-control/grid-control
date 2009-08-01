#!/usr/bin/env python
import sys, os, signal, optparse, gcSupport
from grid_control import *
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

	if os.path.exists(args[0].split("%")[0]):
		fromfile = True
		dir, file = os.path.split(args[0])
		provider = DataProvider.loadState(ConfigDummy(), dir, file)
	else:
		fromfile = False
		provider = DataProvider.open('DBSApiv2', ConfigDummy(), args[0], None)
	blocks = provider.getBlocks()
	if len(blocks) == 0:
		raise DatasetError("No blocks!")

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
		print
		infos = {}
		order = []
		maxnick = 3
		for block in blocks:
			dsName = block[DataProvider.Dataset]
			if not infos.get(dsName, None):
				order.append(dsName)
				infos[dsName] = dict([(DataProvider.Dataset, dsName)])
				if block.has_key(DataProvider.Nickname):
					nick = block[DataProvider.Nickname]
					infos[dsName][DataProvider.Nickname] = nick
					maxnick = max(maxnick, len(nick))
		for dsID, dsName in enumerate(order):
			info = infos[dsName]
			print "", info.get(DataProvider.Nickname, str(dsID)).center(maxnick), ":",
			if info[DataProvider.Dataset].startswith('/PRIVATE'):
				print 'list : %s%%%s' % (args[0].split("%")[0], info[DataProvider.Dataset])
			else:
				print 'DBS : %s' % info[DataProvider.Dataset]


	if opts.listdatasets:
		print
		infos = {}
		infosum = {
			DataProvider.NEvents : 0,
			DataProvider.Dataset : 'Sum'
		}
		order = []
		for block in blocks:
			dsName = block.get(DataProvider.Dataset, '')
			if not infos.get(dsName, None):
				order.append(dsName)
				infos[dsName] = {
					DataProvider.NEvents : 0,
					DataProvider.Dataset : block[DataProvider.Dataset]
				}
			infos[dsName][DataProvider.NEvents] += block[DataProvider.NEvents]
			infosum[DataProvider.NEvents] += block[DataProvider.NEvents]
		utils.printTabular([(DataProvider.Dataset, "Dataset"), (DataProvider.NEvents, "Events")],
			map(lambda x: infos[x], order) + [None, infosum])

	if opts.listfiles:
		print
		for block in blocks:
			if len(datasets) > 1:
				print "Dataset: %s" % block[DataProvider.Dataset]
			print "Blockname: %s" % block[DataProvider.BlockName]
			utils.printTabular([(DataProvider.lfn, "Filename"), (DataProvider.NEvents, "Events")], block[DataProvider.FileList])
			print

	if opts.liststorage:
		print
		infos = {}
		print "Storage elements:"
		for block in blocks:
			dsName = block[DataProvider.Dataset]
			if not infos.get(dsName, None):
				infos[dsName] = {1:1}
				if len(headerbase) > 0:
					print "Dataset: %s" % dsName
				for se in block[DataProvider.SEList]:
					print "\t%s" % se

	if opts.listblocks:
		print
		utils.printTabular(headerbase + [(DataProvider.BlockName, "Block"), (DataProvider.NEvents, "Events")], blocks)

	if opts.save:
		print
		provider.saveState(".", "datacache.dat")
		print "Dataset information saved to ./datacache.dat"

	# everything seems to be in order
	print
	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
