#!/usr/bin/env python
# | Copyright 2017 Karlsruhe Institute of Technology
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

import sys, logging
from gc_scripts import Plugin, ScriptOptions, gc_create_config
from grid_control.utils.webservice import JSONRestClient
from grid_control_cms.sitedb import SiteDB


def _lfn2pfn(node, lfn, prot='srmv2'):
	return JSONRestClient().get(url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		params={'node': node, 'protocol': prot, 'lfn': lfn})['phedex']['mapping']


def _lookup_pfn(options):
	if '<hypernews_name>' in options.opts.lfn:
		token = Plugin.get_class('AccessToken').create_instance('VomsProxy', gc_create_config(), 'token')
		site_db = SiteDB()
		hn_name = site_db.dn_to_username(dn=token.get_fq_user_name())
		if not hn_name:
			raise Exception('Unable to map grid certificate to hypernews name!')
		options.opts.lfn = options.opts.lfn.replace('<hypernews_name>', hn_name)

	tmp = _lfn2pfn(node=options.opts.se, prot=options.opts.se_prot, lfn=options.opts.lfn)
	for entry in tmp:
		entry_str = entry['pfn']
		if len(tmp) > 1:  # write node name if multiple mappings are returned
			entry_str = '%s %s' % (entry['node'], entry_str)
		logging.getLogger('script').info(entry_str)


def _main():
	parser = ScriptOptions()
	parser.add_text(None, 's', 'se', default=None,
		help='Resolve LFN on CMS SE into PFN')
	parser.add_text(None, ' ', 'se-prot', default='srmv2',
		help='Name of default SE protocol [Default:%s]')
	parser.add_text(None, ' ', 'lfn', default='/store/user/<hypernews_name>',
		help='Name of default LFN [Default:%s]')
	options = parser.script_parse()

	if options.opts.se:
		return _lookup_pfn(options)


if __name__ == '__main__':
	sys.exit(_main())
