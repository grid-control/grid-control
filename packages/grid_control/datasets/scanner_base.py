#-#  Copyright 2012-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from grid_control import utils, LoadableObject, AbstractError

class InfoScanner(LoadableObject):
	def __init__(self, config):
		pass

	def getGuards(self):
		return ([], [])

	def getEntriesVerbose(self, level, *args):
		utils.vprint('    ' * level + 'Collecting information with %s...' % self.__class__.__name__, 1)
		for c, n, l in zip(args, ['Path', 'Metadata', 'Events', 'SE list', 'Objects'], [1, 2, 1, 2, 2]):
			utils.vprint('    ' * level + '  %s: %s' % (n, c), l)
		return self.getEntries(*args)

	def getEntries(self, path, metadata, events, seList, objStore):
		raise AbstractError
InfoScanner.registerObject()
