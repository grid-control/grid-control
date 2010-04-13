#!/usr/bin/env python
import gcSupport, sys, optparse, os
from grid_control.CMSSW import formatLumi, parseLumiFilter

def fail(msg):
	print msg
	sys.exit(1)

if len(sys.argv) == 1:
	fail("Usage: %s <lumi filter expression>" % sys.argv[0])
try:
	lumis = parseLumiFilter(str.join(" ", sys.argv[1:]))
except:
	fail("Could not parse: %s" % str.join(" ", sys.argv[1:]))
try:
	# Wrap after 60 characters
	for line in map(lambda x: str.join(", ", x), gcSupport.utils.lenSplit(formatLumi(lumis), 60)):
		print line
except:
	fail("Could format lumi sections!" % sys.argv[1])
