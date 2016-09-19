# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

from grid_control.datasets import DataProvider, DataSplitter
from python_compat import imap

class RunSplitter(DataSplitter.get_class('FileClassSplitter')):
	alias = ['runs']

	def _configure_splitter(self, config):
		self._run_range = self._query_config(config.getInt, 'run range', 1)

	def _get_fi_class(self, fi, block):
		run_range = self._setup(self._run_range, block)
		metadata_idx = block[DataProvider.Metadata].index('Runs')
		return tuple(imap(lambda r: int(r / run_range), fi[DataProvider.Metadata][metadata_idx]))
