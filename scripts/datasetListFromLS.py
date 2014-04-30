#!/usr/bin/env python
#-#  Copyright 2010-2012 Karlsruhe Institute of Technology
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

import gcSupport, sys, optparse, datasetListFromX

parser = optparse.OptionParser('%prog [options] <data path> <dataset name> <pattern (*.root) / files>')
parser.add_option('-p', '--path', dest='path', default='.', help='Path to dataset files')
datasetListFromX.addOptions(parser)
(opts, args) = gcSupport.parseOptions(parser)

# Positional parameters override options
if len(args) > 0:
	opts.path = args[0]
if len(args) > 1:
	setattr(opts, 'dataset name pattern', args[1])
if len(args) > 2:
	setattr(opts, 'filename filter', str.join(' ', args[2:]))
def conditionalSet(name, source, sourceKey):
	if not getattr(opts, name) and getattr(opts, source):
		setattr(opts, name, sourceKey)
conditionalSet('dataset name pattern', 'delimeter dataset key', '/PRIVATE/@DELIMETER_DS@')
conditionalSet('block name pattern', 'delimeter block key', '@DELIMETER_B@')
datasetListFromX.discoverDataset(opts, parser, 'ScanProvider', opts.path)
