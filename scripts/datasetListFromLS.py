#!/usr/bin/env python
import os, sys, glob, random, optparse

def iterateFiles():
	for pattern in opts.selection:
		for path in glob.glob(os.path.join(opts.path, pattern)):
			shortPath = os.path.abspath(path).replace(opts.path, "").lstrip("/")
			yield (shortPath, opts.events)

def printIntro(dataset, onEmpty, onGiven):
	if opts.dataset == "":
		dataset = onEmpty
	else:
		dataset = onGiven
	if not dataset.startswith("/"):
		dataset = "/PRIVATE/%s" % dataset
	print "[%s#%s]" % (dataset, "%.8x" % random.randint(0, 2**32))
	print "prefix = %s" % opts.path.rstrip("/")

def printEntry(entry):
	print "%s = %s" % (entry[0], entry[1])

def processFiles():
	opts.path = os.path.abspath(opts.path)

	if not opts.multi:
		printIntro(opts.dataset, os.path.basename(opts.path), opts.dataset)
		map(printEntry, iterateFiles())
		print
	else:
		params = opts.multi.split(":")
		(delim, ds, de) = ("_", None, None)
		if len(params) == 3:
			(delim, ds, de) = params
		elif len(params) == 2:
			(delim, ds) = params
		elif len(params) == 1:
			(delim,) = params
		if ds: ds = int(ds)
		if de: de = int(de)

		files = {}
		for (path, events) in iterateFiles():
			dataset = str.join(delim, os.path.splitext(path)[0].split(delim)[ds:de])
			if dataset not in files:
				files[dataset] = []
			files[dataset].append((path, events))

		for dataset in files:
			printIntro(opts.dataset, dataset, "%s/%s" % (opts.dataset, dataset))
			map(printEntry, files[dataset])
			print


if __name__ == '__main__':
	usage = "%s [OPTIONS] <dataset name> <data path> <pattern (*.root) / files>" % sys.argv[0]
	parser = optparse.OptionParser(usage=usage)
	parser.add_option("-d", "--dataset", dest="dataset", default="", help="Name of dataset")
	parser.add_option("-p", "--path", dest="path", default=".", help="Path to dataset files")
	parser.add_option("-m", "--multi", dest="multi", default=None,
		help="Multi dataset mode - files are sorted into different datasets according to <delimeter>:<start>:<end>")
	parser.add_option("-s", "--selection", dest="selection", default="*",
		help="File to include in dataset (Default: *.root)")
	parser.add_option("-e", "--events", dest="events", default="1", help="Number of events in files")
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
