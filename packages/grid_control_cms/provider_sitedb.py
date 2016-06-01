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

import re, logging
from grid_control.utils.webservice import GridJSONRestClient
from python_compat import ifilter, imap, izip, lmap, set

def unflatten_json(data):
	"""Tranform input to unflatten JSON format"""
	columns = data['desc']['columns']
	for row in data['result']:
		yield dict(izip(columns, row))


class SiteDB(object):
	def __init__(self, url = None):
		self._gjrc = GridJSONRestClient(url = url or 'https://cmsweb.cern.ch/sitedb/data/prod/')

	def _query(self, api, match = None):
		params = None
		if match:
			params = {'match': match}
		return unflatten_json(self._gjrc.get(api, params = params))

	def _people(self, username = None):
		return self._query('people', username)

	def _site_names(self, site_name = None):
		return self._query('site-names', site_name)

	def _site_resources(self):
		return self._query('site-resources')

	def cms_name_to_se(self, cms_name):
		cms_name = cms_name.replace('*', '.*')
		cms_name = cms_name.replace('%', '.*')
		cms_name_regex = re.compile(cms_name)

		psn_site_names = ifilter(lambda site: site['type'] == 'psn' and cms_name_regex.match(site['alias']), self._site_names())
		site_names = set(imap(lambda x: x['site_name'], psn_site_names))
		return lmap(lambda x: x['fqdn'], ifilter(lambda x: (x['type'] == 'SE') and (x['site_name'] in site_names), self._site_resources()))

	def se_to_cms_name(self, se):
		site_names = []
		for site_resource in ifilter(lambda resources: resources['fqdn'] == se, self._site_resources()):
			site_names.extend(self._site_names(site_name=site_resource['site_name']))
		return lmap(lambda x: x['alias'], ifilter(lambda site: site['type'] == 'cms', site_names))

	def dn_to_username(self, dn):
		for user in ifilter(lambda this_user: this_user['dn'] == dn, self._people()):
			return user['username']

	def username_to_dn(self, username):
		for user in self._people(username = username):
			return user['dn']


if __name__ == '__main__':
	site_db = SiteDB()
	logging.critical(site_db.dn_to_username(dn='/C=DE/O=GermanGrid/OU=KIT/CN=Manuel Giffels'))
	logging.critical(site_db.username_to_dn(username='giffels'))
	logging.critical(site_db.cms_name_to_se(cms_name='T*_PL_Warsaw'))
	logging.critical(site_db.se_to_cms_name(se='se.polgrid.pl'))
	logging.critical(site_db.se_to_cms_name(se='se.grid.icm.edu.pl'))
