# | Copyright 2016-2017 Karlsruhe Institute of Technology
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
from grid_control.datasets.provider_base import DataProvider


class StatsDataProcessor(DataProcessor):
	def __init__(self, config, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		(self._entries, self._blocks, self._files) = (0, 0, 0)

	def process_block(self, block):
		if block:
			self._blocks += 1
			self._files += len(block[DataProvider.FileList])
			self._entries += block[DataProvider.NEntries]
			return block

	def _reset(self):
		self._entries = 0
		self._blocks = 0
		self._files = 0


class SimpleStatsDataProcessor(StatsDataProcessor):
	alias_list = ['stats']

	def __init__(self, config, datasource_name, log, msg):
		StatsDataProcessor.__init__(self, config, datasource_name)
		(self._log, self._msg) = (log, msg)

	def process(self, block_iter):
		self._reset()
		for block in StatsDataProcessor.process(self, block_iter):
			yield block
		self._log.info('%s%s', self._msg, self._get_stats() or 'nothing!')

	def _get_stats(self):
		stats = []

		def _add_stat(value, singular, plural):
			if stats:
				stats.append('with')
			if value > 0:
				stats.append(str(value))
				if value == 1:
					stats.append(singular)
				else:
					stats.append(plural)

		_add_stat(self._blocks, 'block', 'blocks')
		_add_stat(self._files, 'file', 'files')
		_add_stat(self._entries, 'entry', 'entries')
		return str.join(' ', stats)
