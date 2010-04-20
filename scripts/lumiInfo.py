#!/usr/bin/env python
import gcSupport, sys, optparse, os
from grid_control.CMSSW import formatLumi, parseLumiFilter

def fail(msg):
	print msg
	sys.exit(1)

parser = optparse.OptionParser()
parser.add_option("-g", "--gc", dest="save_gc", default=False, action="store_true",
	help="Output grid-control compatible lumi expression")
parser.add_option("-j", "--json", dest="save_json", default=False, action="store_true",
	help="Output JSON file with lumi expression")
(opts, args) = parser.parse_args()

if len(args) == 0:
	fail("Usage: %s <lumi filter expression>" % sys.argv[0])
try:
	lumis = parseLumiFilter(str.join(" ", args))
except:
	fail("Could not parse: %s" % str.join(" ", args))
try:
	if opts.save_gc:
		# Wrap after 60 characters
		for line in map(lambda x: str.join(", ", x), gcSupport.utils.lenSplit(formatLumi(lumis), 65)):
			print line
	if opts.save_json:
		tmp = {}
		for rlrange in lumis:
			start, end = rlrange
			if start[0] != end[0]:
				raise
			if start[0] not in tmp:
				tmp[start[0]] = []
			tmp[start[0]].append([start[1], end[1]])
		print "{"
		print str.join(',\n', map(lambda run: '\t"%d": %s' % (run, tmp[run]), sorted(tmp)))
		print "}"
except:
	fail("Could format lumi sections!" % args)
