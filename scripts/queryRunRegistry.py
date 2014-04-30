#!/usr/bin/env python
#-#  Copyright 2010 Karlsruhe Institute of Technology
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

from gcSupport import utils
from grid_control_cms.lumi_tools import formatLumi, parseLumiFromJSON, mergeLumi
import xmlrpclib

server = xmlrpclib.ServerProxy('http://pccmsdqm04.cern.ch/runregistry/xmlrpc')
data = server.DataExporter.export('RUNLUMISECTION', 'GLOBAL', 'json', {'groupName': 'Collisions10'})
runs = parseLumiFromJSON(data)
utils.vprint("lumi filter = %s" % utils.wrapList(formatLumi(mergeLumi(runs)), 60, ',\n\t'), -1)
