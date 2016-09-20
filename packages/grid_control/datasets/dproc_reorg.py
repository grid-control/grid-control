# | Copyright 2009-2016 Karlsruhe Institute of Technology
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
from grid_control.utils.data_structures import make_enum
from hpfwk import AbstractError


DataProcessorMergeMode = make_enum(['intersection', 'union', 'separate'])


class MergeDataProcessor(DataProcessor):
	alias_list = ['merge']

	def process(self, block_iter):
		pass


class SplitDataProcessor(DataProcessor):
	alias_list = ['split']

	def process_block(self, block):
		raise AbstractError
