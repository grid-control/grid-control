# | Copyright 2012-2016 Karlsruhe Institute of Technology
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
from hpfwk import AbstractError

class InfoScanner(ConfigurablePlugin):
	def __init__(self, config):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('infoscanner')

	def getGuards(self):
		return ([], [])

	def getEntriesVerbose(self, depth, path, metadata, events, seList, objStore):
		self._log.log(logging.DEBUG, '    ' * depth + 'Collecting information with %s...', self.__class__.__name__)
		for level, content, name in [
				(logging.DEBUG, path, 'Path'),
				(logging.DEBUG1, metadata, 'Metadata'),
				(logging.DEBUG, events, 'Events'),
				(logging.DEBUG1, seList, 'SE list'),
				(logging.DEBUG1, objStore, 'Objects')]:
			self._log.log(level, '    ' * depth + '  %s: %s', name, content)
		return self.getEntries(path, metadata, events, seList, objStore)

	def getEntries(self, path, metadata, events, seList, objStore):
		raise AbstractError
