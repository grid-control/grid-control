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
from grid_control.utils.webservice import JSONRestClient
from grid_control_cms.sitedb import SiteDB

def lfn2pfn(node, lfn, prot = 'srmv2'):
	return JSONRestClient().get(url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		params = {'node': node, 'protocol': prot, 'lfn': lfn})['phedex']['mapping']

parser = Options()
parser.addText(None, 's', 'se',      default = None,    help = 'Resolve LFN on CMS SE into PFN')
parser.addText(None, ' ', 'se-prot', default = 'srmv2', help = 'Name of default SE protocol')
parser.addText(None, ' ', 'lfn',     default = '/store/user/<hypernews name>', help = 'Name of default LFN')
options = scriptOptions(parser)

if options.opts.se:
	if '<hypernews name>' in options.opts.lfn:
		token = Plugin.getClass('AccessToken').create_instance('VomsProxy', getConfig(), 'token')
		site_db = SiteDB()
		hnName = site_db.dn_to_username(dn=token.getFQUsername())
		if not hnName:
			raise Exception('Unable to map grid certificate to hypernews name!')
		options.opts.lfn = options.opts.lfn.replace('<hypernews name>', hnName)

	tmp = lfn2pfn(node = options.opts.se, prot = options.opts.se_prot, lfn = options.opts.lfn)
	for entry in tmp:
		if len(tmp) > 1:
			print(entry['node'] + ' ' + entry['pfn'])
		print(entry['pfn'])
