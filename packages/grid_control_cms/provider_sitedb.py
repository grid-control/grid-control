#-#  Copyright 2015 Karlsruhe Institute of Technology
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
from grid_control.exceptions import UserError
from grid_control.utils.webservice import parseJSON
from grid_control.utils.webservice import RestClient

import os
import re


def unflatten_json(data):
    """Tranform input to unflatten JSON format"""
    columns = data['desc']['columns']
    for row in data['result']:
        yield dict(zip(columns, row))


class SiteDB(object):
    def __init__(self, url='https://cmsweb.cern.ch/sitedb/data/prod/'):
        self._proxy_path = os.environ.get('X509_USER_PROXY', '')
        if not os.path.exists(self._proxy_path):
            raise UserError('VOMS proxy needed to query SiteDB! Environment variable X509_USER_PROXY is "%s"'
                            % self._proxy_path)

        self.rest_client = RestClient(cert=self._proxy_path)
        self._url = url

    def _people(self, username=None):
        if username:
            response = self.rest_client.get(self._url, api='people', params=dict(match=username))
        else:
            response = self.rest_client.get(self._url, api='people')
        return unflatten_json(parseJSON(response))

    def _site_names(self, site_name=None):
        if site_name:
            response = self.rest_client.get(self._url, api='site-names', params=dict(match=site_name))
        else:
            response = self.rest_client.get(self._url, api='site-names')
        return unflatten_json(parseJSON(response))

    def _site_resources(self):
        response = self.rest_client.get(self._url, api='site-resources')
        return unflatten_json(parseJSON(response))

    def cms_name_to_se(self, cms_name):
        cms_name = cms_name.replace('*','.*')
        cms_name = cms_name.replace('%','.*')
        cms_name_regex = re.compile(cms_name)

        psn_site_names = filter(lambda site: site['type'] == 'psn' and cms_name_regex.match(site[u'alias']),
                                self._site_names())
        site_names = set(map(lambda x: x['site_name'], psn_site_names))
        site_resources = filter(lambda x: x['site_name'] in site_names, self._site_resources())
        host_list = filter(lambda x: x['type'] == 'SE', site_resources)
        host_list = map(lambda x: x['fqdn'], host_list)
        return host_list

    def se_to_cms_name(self, se):
        site_resources = filter(lambda resources: resources['fqdn'] == se, self._site_resources())
        site_names = []
        for site_resource in site_resources:
            site_names.extend(self._site_names(site_name=site_resource['site_name']))
        return [x['alias'] for x in filter(lambda x: x['type']=='cms', site_names)]

    def dn_to_username(self, dn):
        user_info = filter(lambda user: user['dn']==dn, self._people())
        try:
            return user_info[0]['username']
        except IndexError:
            return None

    def username_to_dn(self, username):
        user_info = self._people(username=username)
        try:
            return next(user_info)['dn']
        except StopIteration:
            return None


if __name__ == '__main__':
    site_db = SiteDB()
    print site_db.dn_to_username(dn='/C=DE/O=GermanGrid/OU=KIT/CN=Manuel Giffels')
    print site_db.username_to_dn(username='giffels')
    print site_db.cms_name_to_se(cms_name='T*_PL_Warsaw')
    print site_db.se_to_cms_name(se='se.polgrid.pl')
    print site_db.se_to_cms_name(se='se.grid.icm.edu.pl')