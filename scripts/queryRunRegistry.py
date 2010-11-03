#!/usr/bin/env python

import gcSupport
from grid_control.CMSSW import formatLumi, parseLumiFromJSON, mergeLumi
import xmlrpclib

server = xmlrpclib.ServerProxy('http://pccmsdqm04.cern.ch/runregistry/xmlrpc')
data = server.DataExporter.export('RUNLUMISECTION', 'GLOBAL', 'json', {'groupName': 'Collisions10'})
runs = parseLumiFromJSON(data)
print("lumi filter = %s" % gcSupport.utils.wrapList(formatLumi(mergeLumi(runs)), 60, ',\n\t'))
