#!/usr/bin/env python
# | Copyright 2010-2017 Karlsruhe Institute of Technology
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
from gc_scripts import ScriptOptions
from python_compat import json, resolve_fun


def _main():
	parser = ScriptOptions()
	parser.add_text(None, None, 'url', default='http://pccmsdqm04.cern.ch/runregistry/xmlrpc',
		help='URL to runregistry [Default:%s]')
	parser.add_text(None, None, 'run', default='Collisions10',
		help='Specify run era that will be queried for the lumi json file [Default:%s]')
	options = parser.script_parse()
	server_proxy_cls = resolve_fun('xmlrpc.client:ServerProxy', 'xmlrpclib:ServerProxy')
	server = server_proxy_cls(options.opts.url).DataExporter
	data = server.export('RUNLUMISECTION', 'GLOBAL', 'json', {'groupName': options.opts.run})
	logging.getLogger('script').info(json.dumps(data))


if __name__ == '__main__':
	sys.exit(_main())
