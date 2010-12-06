#!/usr/bin/env python

from gcSupport import utils
from grid_control_cms.lumi_tools import formatLumi, parseLumiFromJSON, mergeLumi
import xmlrpclib

server = xmlrpclib.ServerProxy('http://pccmsdqm04.cern.ch/runregistry/xmlrpc')
data = server.DataExporter.export('RUNLUMISECTION', 'GLOBAL', 'json', {'groupName': 'Collisions10'})
runs = parseLumiFromJSON(data)
utils.vprint("lumi filter = %s" % utils.wrapList(formatLumi(mergeLumi(runs)), 60, ',\n\t'), -1)
