# | Copyright 2015-2016 Karlsruhe Institute of Technology
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

import logging
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils import prune_processors
from hpfwk import AbstractError
from python_compat import imap, lchain

# Class used by DataParameterSource to convert dataset splittings into parameter data
class PartitionProcessor(ConfigurablePlugin):
	def __init__(self, config):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('dataset.partition.processor')

	def enabled(self):
		return True

	def getKeys(self):
		return []

	def getNeededKeys(self, splitter):
		return []

	def process(self, pNum, splitInfo, result):
		raise AbstractError


class MultiPartitionProcessor(PartitionProcessor):
	def __init__(self, config, processorList):
		PartitionProcessor.__init__(self, config)
		do_prune = config.getBool('partition processor prune', True, onChange = None)
		self._processorList = prune_processors(do_prune, processorList, self._log, 'Removed %d inactive partition processors!')

	def getKeys(self):
		return lchain(imap(lambda p: p.getKeys() or [], self._processorList))

	def getNeededKeys(self, splitter):
		return lchain(imap(lambda p: p.getNeededKeys(splitter) or [], self._processorList))

	def process(self, pNum, splitInfo, result):
		for processor in self._processorList:
			processor.process(pNum, splitInfo, result)
