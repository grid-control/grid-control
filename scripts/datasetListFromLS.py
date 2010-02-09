#!/usr/bin/env python
import os, sys, glob, random

def main(args):
	if len(args) < 2:
		sys.stderr.write("Syntax: %s <dataset name> <data path> [pattern (*.root)]\n" % sys.argv[0])
		sys.exit(1)
	pat = "*.root"
	if len(args) == 3:
		pat = args[2]

	print "[%s#%s]" % (args[0], "%.8x" % random.randint(0, 2**32))
	print "prefix =", args[1].lstrip("/")
	for path in glob.glob(os.path.join(args[1], pat)):
		print path.replace(args[1], "").lstrip("/"), "= 1"
	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
