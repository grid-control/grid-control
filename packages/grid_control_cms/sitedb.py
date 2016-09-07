# | Copyright 2015-2016 Karlsruhe Institute of Technology
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

import re
from grid_control.utils.webservice import GridJSONRestClient
from python_compat import ifilter, imap, izip, lmap, set

class SiteDB(object):
	def __init__(self, url = None):
		self._url = url or 'https://cmsweb.cern.ch/sitedb/data/prod'
		self._gjrc = GridJSONRestClient(self._url, 'VOMS proxy needed to query siteDB!')

	def _query(self, api, **kwargs):
		key = (self._url, api, tuple(kwargs.items()))
		if key not in SiteDB.queryCache:
			SiteDB.queryCache[key] = self._gjrc.get(api = api, params = kwargs or None)
		data = SiteDB.queryCache[key]
		columns = data['desc']['columns']
		for row in data['result']:
			yield dict(izip(columns, row))

	def cms_name_to_se(self, cms_name):
		cms_name_regex = re.compile(cms_name.replace('*', '.*').replace('%', '.*'))
		psn_site_names = ifilter(lambda site: site['type'] == 'psn' and cms_name_regex.match(site['alias']), self._query('site-names'))
		site_names = set(imap(lambda x: x['site_name'], psn_site_names))
		return lmap(lambda x: x['fqdn'], ifilter(lambda x: (x['type'] == 'SE') and (x['site_name'] in site_names), self._query('site-resources')))

	def se_to_cms_name(self, se):
		site_names = []
		for site_resource in ifilter(lambda resources: resources['fqdn'] == se, self._query('site-resources')):
			site_names.extend(self._query('site-names', match = site_resource['site_name']))
		return lmap(lambda x: x['alias'], ifilter(lambda site: site['type'] == 'cms', site_names))

	def dn_to_username(self, dn):
		for user in ifilter(lambda this_user: this_user['dn'] == dn, self._query('people')):
			return user['username']

	def username_to_dn(self, username):
		for user in self._query('people', match = username):
			return user['dn']
SiteDB.queryCache = {}
