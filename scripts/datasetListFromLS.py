#!/usr/bin/env python
import os, sys, glob, random, optparse

def iterateFiles():
	for pattern in opts.selection:
		for path in map(os.path.abspath, glob.glob(os.path.join(opts.path, pattern))):
			shortPath = path.replace(opts.path, "").lstrip("/")
			if opts.eventscmd:
				events = int(os.popen("%s %s" % (opts.eventscmd, path)).readlines()[-1])
				sys.stderr.write("%s %s %s\n" % (opts.eventscmd, path, events))
				yield (shortPath, events)
			else:
				yield (shortPath, opts.events)

def printIntro(dataset, onEmpty, onGiven, block = None):
	if opts.dataset == "":
		dataset = onEmpty
	else:
		dataset = onGiven
	if not dataset.startswith("/"):
		dataset = "/PRIVATE/%s" % dataset
	if not block:
		block = "%.8x" % random.randint(0, 2**32)
	print "[%s#%s]" % (dataset, block)
	print "prefix = %s" % opts.path.rstrip("/")

def printEntry(entry):
	print "%s = %s" % (entry[0], entry[1])

def splitParse(opt):
	params = opt.split(":")
	(delim, ds, de) = ("_", None, None)
	if len(params) == 3:
		(delim, ds, de) = params
	elif len(params) == 2:
		(delim, ds) = params
	elif len(params) == 1:
		(delim,) = params
	if ds: ds = int(ds)
	if de: de = int(de)
	return (delim, ds, de)

def processFiles():
	opts.path = os.path.abspath(opts.path)

	if not opts.multi:
		printIntro(opts.dataset, os.path.basename(opts.path), opts.dataset)
		map(printEntry, iterateFiles())
		print
	else:
		(delim, ds, de) = splitParse(opts.multi)
		if opts.multiblock:
			(bdelim, bds, bde) = splitParse(opts.multiblock)
		files = {}
		for (path, events) in iterateFiles():
			dataset = str.join(delim, os.path.splitext(path)[0].split(delim)[ds:de])
			block = None
			if opts.multiblock:
				block = str.join(delim, os.path.splitext(path)[0].split(bdelim)[bds:bde])
			files.setdefault(dataset, {}).setdefault(block, []).append((path, events))

		for dataset in files:
			for block in files[dataset]:
				printIntro(opts.dataset, dataset, "%s/%s" % (opts.dataset, dataset), block)
				map(printEntry, files[dataset][block])
				print


if __name__ == '__main__':
	usage = "%s [OPTIONS] <dataset name> <data path> <pattern (*.root) / files>" % sys.argv[0]
	parser = optparse.OptionParser(usage=usage)
	parser.add_option("-d", "--dataset", dest="dataset", default="", help="Name of dataset")
	parser.add_option("-p", "--path", dest="path", default=".", help="Path to dataset files")
	parser.add_option("-m", "--multi", dest="multi", default=None,
		help="Multi dataset mode - files are sorted into different datasets according to <delimeter>:<start>:<end>")
	parser.add_option("-M", "--multiblock", dest="multiblock", default=None,
		help="Multi block mode - files are sorted into different blocks according to <delimeter>:<start>:<end>")
	parser.add_option("-s", "--selection", dest="selection", default="*.root",
		help="File to include in dataset (Default: *.root)")
	parser.add_option("-e", "--events", dest="events", default="-1", help="Number of events in files")
	parser.add_option("-E", "--events-cmd", dest="eventscmd", default=None,
		help="Application used to determine number of events in files")
	(opts, args) = parser.parse_args()

	# Positional parameters override options
	if len(args) > 0:
		opts.dataset = args[0]
	if len(args) > 1:
		opts.path = args[1]
	if len(args) > 2:
		opts.selection = args[2:]
	else:
		opts.selection = [opts.selection]

	processFiles()
