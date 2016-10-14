# | Copyright 2016 Karlsruhe Institute of Technology
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

from grid_control.config import join_config_locations
from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider
from hpfwk import clear_current_exception
from python_compat import iidfilter


class PartitionEstimator(DataProcessor):
	alias_list = ['estimate', 'SplitSettingEstimator']

	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._target_jobs = config.get_int(
			join_config_locations(['', datasource_name], 'target partitions'), -1)
		self._target_jobs_ds = config.get_int(
			join_config_locations(['', datasource_name], 'target partitions per nickname'), -1)
		self._entries = {None: 0}
		self._files = {None: 0}
		self._config = None
		if self.enabled():
			self._config = config

	def enabled(self):
		return (self._target_jobs > 0) or (self._target_jobs_ds > 0)

	def process(self, block_iter):
		if self.enabled() and self._config:
			block_list = list(DataProcessor.process(self, block_iter))
			if self._target_jobs > 0:
				self._set_split_opt(self._config, 'files per job', self._files[None], self._target_jobs)
				self._set_split_opt(self._config, 'events per job', self._entries[None], self._target_jobs)
			if self._target_jobs_ds > 0:
				for nick in iidfilter(self._files):
					block_config = self._config.change_view(set_sections=['dataset %s' % nick])
					self._set_split_opt(block_config, 'files per job', self._files[nick], self._target_jobs_ds)
					self._set_split_opt(block_config, 'events per job', self._entries[nick], self._target_jobs_ds)
			self._config = None
			return block_list
		return block_iter

	def process_block(self, block):
		def _inc(key):
			self._files[key] = self._files.get(key, 0) + len(block[DataProvider.FileList])
			self._entries[key] = self._entries.get(key, 0) + block[DataProvider.NEntries]
		_inc(None)
		if block.get(DataProvider.Nickname):
			_inc(block.get(DataProvider.Nickname))
		return block

	def _set_split_opt(self, config, name, work_units, target_partitions):
		try:
			config.set_int(name, max(1, int(work_units / float(target_partitions) + 0.5)))
		except Exception:
			clear_current_exception()
