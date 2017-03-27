# | Copyright 2015-2017 Karlsruhe Institute of Technology
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
from grid_control_cms.access_cms import get_cms_cert
from python_compat import ifilter, imap, izip, lmap, set


class SiteDB(object):
	query_cache = {}

	def __init__(self, url=None):
		self._url = url or 'https://cmsweb.cern.ch/sitedb/data/prod'
		self._gjrc = GridJSONRestClient(get_cms_cert(), self._url, 'VOMS proxy needed to query siteDB!')

	def cms_name_to_se(self, cms_name):
		cms_name_regex = re.compile(cms_name.replace('*', '.*').replace('%', '.*'))

		def _select_psn_site(site):
			return site['type'] == 'psn' and cms_name_regex.match(site['alias'])
		psn_site_names = ifilter(_select_psn_site, self._query('site-names'))
		site_names = set(imap(lambda x: x['site_name'], psn_site_names))

		def _select_se(resource):
			return (resource['type'] == 'SE') and (resource['site_name'] in site_names)
		return lmap(lambda x: x['fqdn'], ifilter(_select_se, self._query('site-resources')))

	def dn_to_username(self, dn):
		for user in ifilter(lambda this_user: this_user['dn'] == dn, self._query('people')):
			return user['username']

	def se_to_cms_name(self, se_name):
		site_names = []
		resource_iter = self._query('site-resources')
		for site_resource in ifilter(lambda resource: resource['fqdn'] == se_name, resource_iter):
			site_names.extend(self._query('site-names', match=site_resource['site_name']))
		return lmap(lambda x: x['alias'], ifilter(lambda site: site['type'] == 'cms', site_names))

	def username_to_dn(self, username):
		for user in self._query('people', match=username):
			return user['dn']

	def _query(self, api, **kwargs):
		key = (self._url, api, tuple(kwargs.items()))
		if key not in SiteDB.query_cache:
			SiteDB.query_cache[key] = self._gjrc.get(api=api, params=kwargs or None)
		data = SiteDB.query_cache[key]
		columns = data['desc']['columns']
		for row in data['result']:
			yield dict(izip(columns, row))
