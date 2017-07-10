# | Copyright 2013-2017 Karlsruhe Institute of Technology
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

from grid_control.datasets import DataProvider
from grid_control.gc_exceptions import UserError
from grid_control.utils.webservice import GridJSONRestClient
from grid_control_cms.access_cms import get_cms_cert
from grid_control_cms.provider_cms import CMSBaseProvider
from python_compat import lmap


class DBS3Provider(CMSBaseProvider):
	# required format: <dataset path>[@<instance>][#<block>]
	alias_list = ['dbs3', 'dbs']

	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None, dataset_proc=None):
		CMSBaseProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick, dataset_proc)
		if self._dataset_instance.startswith('http'):
			self._url = self._dataset_instance
		else:
			self._url = 'https://cmsweb.cern.ch/dbs/%s/DBSReader' % self._dataset_instance
		# Use DBS locality for private samples
		self._use_phedex = (self._url == 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
		self._gjrc = GridJSONRestClient(get_cms_cert(config), self._url,
			'VOMS proxy needed to query DBS3!', UserError)

	def _get_cms_dataset_list(self, dataset_path):
		dataset_path_parts = (dataset_path.lstrip('/') + '/*/*/*').split('/')[:3]
		primary_ds_name, processed_ds_name, data_tier_name = dataset_path_parts
		tmp = self._query_dbsv3('datasets', primary_ds_name=primary_ds_name,
			processed_ds_name=processed_ds_name, data_tier_name=data_tier_name)
		return lmap(lambda x: x['dataset'], tmp)

	def _get_cms_lumi_dict(self, block_path):
		result = {}
		for lumi_info_dict in self._query_dbsv3('filelumis', block_name=block_path):
			tmp = (int(lumi_info_dict['run_num']), lmap(int, lumi_info_dict['lumi_section_num']))
			result.setdefault(lumi_info_dict['logical_file_name'], []).append(tmp)
		return result

	def _iter_blocks_raw(self):
		return self._get_gc_block_list(use_phedex=self._use_phedex)

	def _iter_cms_blocks(self, dataset_path, do_query_sites):
		def _get_name_locationinfo_list(blockinfo):
			if do_query_sites:
				return (blockinfo['block_name'], [(blockinfo['origin_site_name'], None, True)])
			return (blockinfo['block_name'], None)
		return lmap(_get_name_locationinfo_list,
			self._query_dbsv3('blocks', dataset=dataset_path, detail=do_query_sites))

	def _iter_cms_files(self, block_path, query_only_valid, query_lumi):
		for cms_fi in self._query_dbsv3('files', block_name=block_path, detail=True):
			if (cms_fi['is_file_valid'] == 1) or not query_only_valid:
				fi = {DataProvider.URL: cms_fi['logical_file_name'],
					DataProvider.NEntries: cms_fi['event_count']}
				yield (fi, None)

	def _query_dbsv3(self, api, **kwargs):
		return self._gjrc.get(api=api, params=kwargs)
