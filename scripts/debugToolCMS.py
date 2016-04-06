#!/usr/bin/env python
# | Copyright 2013-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

from gcSupport import Options, Plugin, getConfig, scriptOptions
from grid_control.utils.webservice import readJSON
from grid_control_cms.provider_sitedb import SiteDB

def lfn2pfn(node, lfn):
	return readJSON('https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		{'node': node, 'protocol': 'srmv2', 'lfn': lfn})['phedex']['mapping'][0]['pfn']

parser = Options()
parser.addText(None, 's', 'SE', default = None,         help = 'Resolve LFN on CMS SE into PFN')
parser.addText(None, ' ', 'se-prot', default = 'srmv2', help = 'Name of default SE protocol')
parser.addText(None, ' ', 'lfn',     default = '/store/user/<hypernews name>', help = 'Name of default LFN')
options = scriptOptions(parser)

if options.opts.SE:
	if '<hypernews name>' in options.opts.lfn:
		token = Plugin.getClass('AccessToken').createInstance('VomsProxy', getConfig(), None)
		site_db = SiteDB()
		hnName = site_db.dn_to_username(dn=token.getFQUsername())
		if not hnName:
			raise Exception('Unable to map grid certificate to hypernews name!')
		options.opts.lfn = options.opts.lfn.replace('<hypernews name>', hnName)

	tmp = readJSON('https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		{'node': options.opts.SE, 'protocol': options.opts.se_prot, 'lfn': options.opts.lfn})['phedex']['mapping']
	for entry in tmp:
		if len(tmp) > 1:
			print(entry['node'] + ' ' + entry['pfn'])
		print(entry['pfn'])
