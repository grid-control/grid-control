#!/usr/bin/env python
#-#  Copyright 2010-2011 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, gcSupport, sys, optparse, datasetListFromX

usage = '[options] <config file / work directory>'
parser = optparse.OptionParser(usage='%%prog %s' % usage)
parser.add_option('-J', '--job-selector', dest='external job selector', default='',
	help='Specify which jobs to process')
parser.add_option('-m', '--event-mode',   dest='mode',                  default='CMSSW-Out',
	help='Specify how to determine events - available: [CMSSW-Out], CMSSW-In, DataMod')
parser.add_option('-l', '--lfn',          dest='lfn marker',            default='/store/',
	help='Assume everything starting with marker to be a logical file name')
parser.add_option('-c', '--config',       dest='include config infos',  default='False',
	help='CMSSW specific: Add configuration data to metadata', action='store_const', const='True')
parser.add_option('-p', '--parents',      dest='include parent infos',  default='False', 
	help='CMSSW specific: Add parent infos to metadata',       action='store_const', const='True')
datasetListFromX.addOptions(parser)
(opts, args) = gcSupport.parseOptions(parser)

# Positional parameters override options
if len(args) == 0:
	gcSupport.utils.exitWithUsage('%s %s' % (sys.argv[0], usage))
tmp = {'cmssw-out': 'CMSSW_EVENTS_WRITE', 'cmssw-in': 'CMSSW_EVENTS_READ', 'datamod': 'MAX_EVENTS'}
setattr(opts, 'events key', tmp.get(opts.mode.lower(), ''))
datasetListFromX.discoverDataset(opts, parser, 'GCProvider', args[0])
