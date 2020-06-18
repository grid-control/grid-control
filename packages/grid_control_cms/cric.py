# | Copyright 2020 Karlsruhe Institute of Technology
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


class CRIC(object):
	query_cache = {}

	def __init__(self, url = None):
		if url:
			raise Exception('Custom url currently not supportet, please contact the grid-control developers!')
		self._url = url or 'http://cms-cric.cern.ch/api'
		self._gjrc = GridJSONRestClient(get_cms_cert(), self._url, 'VOMS proxy needed to query siteDB!')

		self._url_people = 'http://cms-cric.cern.ch/api/accounts/user/query/?json&preset=people'
		self._gjrc_people = GridJSONRestClient(get_cms_cert(), self._url_people, 'VOMS proxy needed to query CRIC!')

		self._url_names = 'http://cms-cric.cern.ch/api/cms/site/query/?json&preset=site-names'
		self._gjrc_names = GridJSONRestClient(get_cms_cert(), self._url_names, 'VOMS proxy needed to query CRIC!')

		self._url_recources = 'http://wlcg-cric.cern.ch/api/core/service/query/?json&groupby=rcsite'
		self._gjrc_recources = GridJSONRestClient(get_cms_cert(), self._url_recources, 'VOMS proxy needed to query siteDB!')

	def cms_name_to_se(self, cms_name):
		cms_name_regex = re.compile(cms_name.replace('*', '.*').replace('%', '.*'))
		def _select_psn_site(site):
			return site['type'] == 'psn' and cms_name_regex.match(site['alias'])
		psn_site_names = ifilter(_select_psn_site, self._query('site-names'))
		site_aliases = set(imap(lambda x: x['alias'], psn_site_names))
		def _select_se(resource):
			return (resource['type'] == 'SE') and (resource['alias'] in site_aliases)
		return lmap(lambda x: x['fqdn'], ifilter(_select_se, self._query('site-resources')))

	def dn_to_username(self, dn):
		for user in ifilter(lambda this_user: this_user['dn'] == dn, self._query('people')):
			return user['username']

	def se_to_cms_name(self, se_name):
		site_names = []
		resource_iter = self._query('site-resources')
		for site_resource in ifilter(lambda resource: resource['fqdn'] == se_name, resource_iter):
			site_names.extend(self._query('site-names', name=site_resource['alias']))
		return lmap(lambda x: x['alias'], ifilter(lambda site: site['type'] == 'cms', site_names))

	def username_to_dn(self, username):
		for user in self._query('people', username=username):
			return user['dn']

	def _query(self, api, **kwargs):
		key = (self._url, api, tuple(kwargs.items()))
		if key not in CRIC.query_cache:
			if api == 'people':
				CRIC.query_cache[key] = self._gjrc_people.get(api=None, params=kwargs or None)
			elif api == 'site-names':
				CRIC.query_cache[key] = self._gjrc_names.get(api=None, params=kwargs or None)
			elif api == 'site-resources':
				CRIC.query_cache[key] = self._gjrc_recources.get(api=None, params=kwargs or None)
			else:
				CRIC.query_cache[key] = self._gjrc.get(api=api, params=kwargs or None)
		data = CRIC.query_cache[key]
		# workaround for site-resources query
		name = ''
		fqdn = ''
		thetype = ''
		flavour = ''
		alias = ''
		if api == 'site-resources':
			for d in data.keys():
				name = d
				for e in data[d]:
					if 'cms' in e['usage'].keys() and len(e['usage']['cms'])>0:
						alias = e['usage']['cms'][0]
						thetype = e['type']
						if thetype == 'CE':
							fqdn = e['endpoint'].split(':')[0]
							flavour = e['flavour']
							yield {'name':name, 'type':thetype,'fqdn':fqdn,'flavour':flavour,'alias': alias}
						elif thetype == 'SE':
							for pk in e['protocols'].keys():
								p = e['protocols'][pk]
								fqdn = p['endpoint'].split(':')[0]
								flavour = p['flavour']
								yield {'name':name, 'type':thetype,'fqdn':fqdn,'flavour':flavour,'alias': alias}
		else:
			columns = data['desc']['columns']
			for row in data['result']:
				yield dict(izip(columns, row))
