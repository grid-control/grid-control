#!/usr/bin/env python
#-#  Copyright 2013 Karlsruhe Institute of Technology
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

from gcSupport import *
from grid_control_cms.webservice_api import readJSON

def lfn2pfn(node, lfn):
	return readJSON('https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		{'node': node, 'protocol': 'srmv2', 'lfn': lfn})['phedex']['mapping'][0]['pfn']

parser = optparse.OptionParser()
parser.add_option("-s", "--SE", dest="SE", default=None, help="Resolve LFN on CMS SE into PFN")
parser.add_option("", "--lfn", dest="lfn", default="/store/user/<hypernews name>", help="Name of default LFN")
parser.add_option("", "--se-prot", dest="seprot", default="srmv2", help="Name of default SE protocol")
(opts, args) = parseOptions(parser)

if opts.SE:
	if '<hypernews name>' in opts.lfn:
		proxy = Proxy.open('VomsProxy', Config())
		hnName = readJSON('https://cmsweb.cern.ch/sitedb/json/index/dnUserName',
			{'dn': proxy.getFQUsername()})
		if not hnName:
			raise ConfigError('Unable to map grid certificate to hypernews name!')
		opts.lfn = opts.lfn.replace('<hypernews name>', hnName['user'])

	tmp = readJSON('https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		{'node': opts.SE, 'protocol': opts.seprot, 'lfn': opts.lfn})['phedex']['mapping']
	for entry in tmp:
		if len(tmp) > 1:
			print entry['node'],
		print entry['pfn']
