#!/usr/bin/env python
import sys, os, signal, optparse

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.insert(0, os.path.join(_root, "..", 'python'))

# and include grid_control python module
from grid_control import *
import time

_verbosity = 0

def printTabular(head, entries):
	maxlen = {}
	head = [ x for x in head ]
	entries = [ x for x in entries ]

	for entry in entries:
		for id, name in head:
			maxlen[id] = max(maxlen.get(id, 0), len(str(entry[id])))

	formatlist = map(lambda (id, name): "%%%ds" % maxlen[id], head)
	print(" %s " % (str.join(" | ", formatlist) % tuple(map(lambda (id, name): name.center(maxlen[id]), head))))
	print("=%s=" % (str.join("=+=", formatlist) % tuple(map(lambda (id, name): '=' * maxlen[id], head))))

	for entry in entries:
		print(" %s " % (str.join(" | ", formatlist) % tuple(map(lambda (id, name): entry[id], head))))


def main(args):
	parser = optparse.OptionParser()
	parser.add_option("-l", "--list-datasets", dest="list", default=False, action="store_true")
	(opts, args) = parser.parse_args()

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		return 1

	if os.path.exists(args[0]):
		provider = DataProvider.open('ListProvider', None, args[0], None)
	else:
		provider = DataProvider.open('DBSListProvider', None, args[0], None)
	blocks = provider.getBlocks()

	if opts.list:
		infos = {}
		for block in blocks:
			blockID = block.get(DataProvider.DatasetID, 0)
			if not infos.get(blockID, None):
				infos[blockID] = {
					DataProvider.NEvents : 0,
					DataProvider.Dataset : block[DataProvider.Dataset]
				}
			infos[blockID][DataProvider.NEvents] += block[DataProvider.NEvents]
		printTabular([(DataProvider.Dataset, "Dataset"), (DataProvider.NEvents, "Events")], infos.itervalues())

	# everything seems to be in order
	return 0


if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
