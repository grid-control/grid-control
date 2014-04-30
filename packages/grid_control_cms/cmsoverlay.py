#-#  Copyright 2011-2013 Karlsruhe Institute of Technology
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

from grid_control import Proxy
from webservice_api import readJSON

class CMSOverlay:
	def __init__(self, config):
		ConfigOverlay.__init__(self, config)
		config.set('jobs', 'monitor', 'dashboard', override = False)
		config.set('grid', 'sites', '-samtest -cmsprodhi', append = True)

		# Rewrite cms storage urls:
		proxy = Proxy.open('VomsProxy', config)
		hnName = readJSON('https://cmsweb.cern.ch/sitedb/json/index/dnUserName', {'dn': proxy.getFQUsername()})
		if not hnName:
			raise ConfigError('Unable to map grid certificate to hypernews name!')

		def rewriteSEurl(url):
			if url.startswith('cms://'): # cms://T2_DE_DESY/project/subdir
				site, url = (url.rstrip('/') + '/').replace('cms://', '').lstrip('/').split('/', 1)
				return readJSON('https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
					{'node': site, 'protocol': 'srm', 'lfn': '/store/user/%s/%s' % (hnName, url.rstrip('/'))})['phedex']['mapping']
			return url
		#self.rewriteList(lambda (s, i): i.startswith('se') and i.endswith('path'), rewriteSEurl)
