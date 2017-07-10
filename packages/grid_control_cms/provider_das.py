# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import time
from grid_control.config import TriggerResync
from grid_control.datasets import DataProvider
from grid_control.gc_exceptions import UserError
from grid_control.utils.webservice import GridJSONRestClient
from grid_control_cms.access_cms import get_cms_cert
from grid_control_cms.provider_cms import CMSBaseProvider
from hpfwk import clear_current_exception
from python_compat import lmap


class DASRetry(Exception):
	pass


class DASProvider(CMSBaseProvider):
	# required format: <dataset path>[@<instance>][#<block>]
	alias_list = ['das']

	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None, dataset_proc=None):
		CMSBaseProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick, dataset_proc)
		self._url = config.get('das instance', 'https://cmsweb.cern.ch/das/cache',
			on_change=TriggerResync(['datasets', 'parameters']))
		if self._dataset_instance.startswith('http'):
			self._url = self._dataset_instance
			self._dataset_instance = ''
		self._gjrc = DASRestClient(get_cms_cert(config), self._url,
			'VOMS proxy needed to query DAS!', UserError)

	def _get_cms_dataset_list(self, dataset_path):
		for cms_dataset_info in self._query_das('dataset dataset=%s' % dataset_path):
			for cms_service_dataset_info in cms_dataset_info['dataset']:
				yield cms_service_dataset_info['name']

	def _iter_blocks_raw(self):
		return self._get_gc_block_list(use_phedex=False)

	def _iter_cms_blocks(self, dataset_path, do_query_sites):
		for cms_block in self._query_das('block dataset=%s' % dataset_path):
			replica_infos = None
			origin = []
			name = None
			for cms_service_block in cms_block['block']:
				name = cms_service_block.get('name', name)
				if 'replica' in cms_service_block:
					replica_infos = lmap(lambda r: (r['site'], r['se'], r['complete'] == 'y'),
						cms_service_block['replica'])
				if 'origin_site_name' in cms_service_block:
					origin = [(cms_service_block['origin_site_name'], None, True)]
			if replica_infos is None:
				replica_infos = origin
			if name:
				yield (name, replica_infos)

	def _iter_cms_files(self, block_path, query_only_valid, query_lumi):
		for cms_fi in self._query_das('file block=%s' % block_path):
			for cms_service_fi in cms_fi['file']:
				if 'nevents' in cms_service_fi:
					fi = {
						DataProvider.URL: cms_service_fi['name'],
						DataProvider.NEntries: cms_service_fi['nevents']
					}
					yield (fi, None)

	def _query_das(self, query):
		if self._dataset_instance not in ('', 'prod/global'):
			query += ' instance=%s' % self._dataset_instance
		(start, sleep) = (time.time(), 0.4)
		while time.time() - start < 60:
			try:
				return self._gjrc.get(params={'input': query})['data']
			except DASRetry:
				clear_current_exception()
				time.sleep(sleep)
				sleep += 0.4


class DASRestClient(GridJSONRestClient):
	def _process_json_result(self, value):
		if len(value) == 32:
			raise DASRetry
		return GridJSONRestClient._process_json_result(self, value)
