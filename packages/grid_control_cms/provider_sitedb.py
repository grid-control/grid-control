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

    def _people(self, user_name=None):
        if user_name:
            response = self.rest_client.get(self._url, api='people', params=dict(match=user_name))
        else:
            response = self.rest_client.get(self._url, api='people')
        return unflatten_json(parseJSON(response))

    def dn2username(self, dn):
        user_info = filter(lambda user: user['dn']==dn, self._people())
        try:
            return user_info[0]['username']
        except IndexError:
            return None

    def username2dn(self, user_name):
        user_info = self._people(user_name=user_name)
        try:
            return next(user_info)['dn']
        except StopIteration:
            return None


if __name__ == '__main__':
    site_db = SiteDB()
    print site_db.dn2username(dn='/C=DE/O=GermanGrid/OU=KIT/CN=Manuel Giffels')
    print site_db.username2dn(user_name='giffels')