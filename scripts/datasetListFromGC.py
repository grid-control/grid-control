#!/usr/bin/env python
import os, gcSupport, sys, optparse, datasetListFromX

usage = '[options] <config file / work directory>'
parser = optparse.OptionParser(usage='%%prog %s' % usage)
parser.add_option('-J', '--job-selector', dest='external job selector', default='',
	help='Specify which jobs to process')
parser.add_option('-m', '--event-mode',   dest='mode',         default='CMSSW-Out',
	help='Specify how to determine events - available: [CMSSW-Out], CMSSW-In, DataMod')
parser.add_option('-l', '--lfn',          dest='lfn marker',   default='/store/',
	help='Assume everything starting with marker to be a logical file name')
parser.add_option('-c', '--config',       dest='cmsswConfig',  default=False, action="store_true",
	help='CMSSW specific: Add configuration data to metadata')
parser.add_option('-p', '--parents',      dest='cmsswParents', default=False, action="store_true",
	help='CMSSW specific: Add parent infos to metadata')
datasetListFromX.addOptions(parser)
(opts, args) = parser.parse_args()

# Positional parameters override options
if len(args) == 0:
	gcSupport.utils.exitWithUsage('%s %s' % (sys.argv[0], usage))
tmp = {'cmssw-out': 'CMSSW_EVENTS_WRITE', 'cmssw-in': 'CMSSW_EVENTS_READ', 'datamod': 'MAX_EVENTS'}
setattr(opts, 'events key', tmp.get(opts.mode.lower(), ''))
setattr(opts, 'include config infos', str(opts.cmsswConfig))
setattr(opts, 'include parent infos', str(opts.cmsswParents))
datasetListFromX.discoverDataset(opts, parser, 'GCProvider', args[0])
