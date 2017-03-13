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

from grid_control.backends import AccessToken
from grid_control.config import ConfigError, create_config
from grid_control.utils.webservice import JSONRestClient
from grid_control_cms.sitedb import SiteDB


class SEPathParser(object):
	# TODO: integrate this into the SEManager infrastructure
	def __init__(self, config):
		config.set('jobs', 'monitor', 'dashboard', override=False)
		config.set('grid', 'sites', '-samtest -cmsprodhi', append=True)

		site_db = SiteDB()
		token = AccessToken.create_instance('VomsProxy', create_config(), 'token')
		self._hn_name = site_db.dn_to_username(token.get_fq_user_name())
		if not self._hn_name:
			raise ConfigError('Unable to map grid certificate to hn name!')

	def rewrite_se_path(self, se_path):
		if se_path.startswith('cms://'):  # cms://T2_DE_DESY/project/subdir
			(site, lfn) = (se_path.rstrip('/') + '/').replace('cms://', '').lstrip('/').split('/', 1)
			lfn = '/store/user/%s/%s' % (self._hn_name, lfn.rstrip('/'))
			return _lfn2pfn(site, lfn)
		return se_path


def _lfn2pfn(node, lfn, prot='srmv2'):
	return JSONRestClient().get(url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		params={'node': node, 'protocol': prot, 'lfn': lfn})['phedex']['mapping']
