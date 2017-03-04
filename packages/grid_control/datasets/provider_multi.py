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

from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider, DatasetError
from hpfwk import ExceptionCollector
from python_compat import imap, reduce, set


class MultiDatasetProvider(DataProvider):
	def __init__(self, config, datasource_name, dataset_expr, dataset_nick, provider_list):
		for provider in provider_list:
			provider.disable_stream_singletons()
		DataProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick)
		self._stats = DataProcessor.create_instance('SimpleStatsDataProcessor', config,
			'dataset', self._log, 'Summary: Running over ')
		self._provider_list = provider_list

	def check_splitter(self, splitter):
		def _get_proposal(splitter):
			return reduce(lambda prop, prov: prov.check_splitter(prop), self._provider_list, splitter)
		if _get_proposal(splitter) != _get_proposal(_get_proposal(splitter)):
			raise DatasetError('Dataset providers could not agree on valid dataset splitter!')
		return _get_proposal(splitter)

	def get_block_list_cached(self, show_stats):
		exc = ExceptionCollector()

		def _iter_all_blocks():
			for provider in self._provider_list:
				try:
					for block in provider.iter_blocks_normed():
						yield block
				except Exception:
					exc.collect()
		result = self._create_block_cache(show_stats, _iter_all_blocks)
		exc.raise_any(DatasetError('Could not retrieve all datasets!'))
		return result

	def get_dataset_expr(self):
		return str.join(' ', imap(lambda p: p.get_dataset_expr(), self._provider_list))

	def get_dataset_name_list(self):
		if self._cache_dataset is None:
			self._cache_dataset = set()
			exc = ExceptionCollector()
			for provider in self._provider_list:
				try:
					self._cache_dataset.update(provider.get_dataset_name_list())
				except Exception:
					exc.collect()
			exc.raise_any(DatasetError('Could not retrieve all datasets!'))
		return list(self._cache_dataset)

	def get_query_interval(self):
		return max(imap(lambda x: x.get_query_interval(), self._provider_list))
