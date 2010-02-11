#!/usr/bin/env python
import gcSupport, sys, os, signal, optparse
from grid_control import *

def main(args):
	parser = optparse.OptionParser()
	parser.add_option("-l", "--list-datasets", dest="listdatasets", default=False, action="store_true",
		help="Show list of all datasets in query / file")
	parser.add_option("-f", "--list-files",    dest="listfiles",    default=False, action="store_true",
		help="Show list of all files grouped according to blocks")
	parser.add_option("-s", "--list-storage",  dest="liststorage",  default=False, action="store_true",
		help="Show list of locations where data is stored")
	parser.add_option("-b", "--list-blocks",   dest="listblocks",   default=False, action="store_true",
		help="Show list of blocks of the dataset(s)")
	parser.add_option("-c", "--config-entry",  dest="configentry",  default=False, action="store_true",
		help="Gives config file entries to run over given dataset(s)")
	parser.add_option("-C", "--config-guess",  dest="configguess",  default=False, action="store_true",
		help="Gives config file entries to run over given dataset(s). " +
			"Will try to guess where the dataset information was coming from.")
	parser.add_option("-S", "--save",          dest="save",         default=False, action="store_true",
		help="Saves dataset information to the file 'datacache.dat'")
	(opts, args) = parser.parse_args()

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		sys.stderr.write("Syntax: %s <DBS dataset path> | <dataset cache file>\n\n" % sys.argv[0])
		sys.stderr.write("Use --help to get a list of options!\n")
		sys.exit(1)

	class ConfigDummy(object):
		def get(self, x,y,z):
			return z
		def getPath(self, x,y,z):
			return z

	dataset = args[0].strip()
	if os.path.exists(dataset.split("%")[0]):
		dbsArg = False
		dir, file = os.path.split(dataset)
		provider = DataProvider.loadState(ConfigDummy(), dir, file)
	else:
		dbsArg = True
		provider = DataProvider.open('DBSApiv2', ConfigDummy(), dataset, None)
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

	if opts.configentry or opts.configguess:
		print
		infos = {}
		order = []
		maxnick = 5
		for block in blocks:
			dsName = block[DataProvider.Dataset]
			if not infos.get(dsName, None):
				order.append(dsName)
				infos[dsName] = dict([(DataProvider.Dataset, dsName)])
				if block.has_key(DataProvider.Nickname):
					nick = block[DataProvider.Nickname]
					infos[dsName][DataProvider.Nickname] = nick
					maxnick = max(maxnick, len(nick))
				if len(block[DataProvider.FileList]):
					infos[dsName][DataProvider.lfn] = block[DataProvider.FileList][0][DataProvider.lfn]
		for dsID, dsName in enumerate(order):
			info = infos[dsName]
			print "", info.get(DataProvider.Nickname, "nick%d" % dsID).rjust(maxnick), ":",

			if dbsArg or (opts.configguess and info[DataProvider.lfn].startswith('/store')):
				print 'DBS : %s' % info[DataProvider.Dataset]
			else:
				print 'list : %s%%%s' % (dataset.split("%")[0], info[DataProvider.Dataset])


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
			if len(headerbase) > 0:
				print "Dataset: %s" % dsName
			if block.get(DataProvider.BlockName, None):
				print "Blockname: %s" % block[DataProvider.BlockName]
			if block[DataProvider.SEList] == None:
				print "\tNo location contraint specified"
			elif block[DataProvider.SEList] == []:
				print "\tNot located at anywhere"
			else:
				for se in block[DataProvider.SEList]:
					print "\t%s" % se
			print

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
