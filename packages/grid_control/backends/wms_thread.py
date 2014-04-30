#-#  Copyright 2012 Karlsruhe Institute of Technology
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

import threading
from grid_control import utils
from wms_multi import MultiWMS

class ThreadedMultiWMS(MultiWMS):
	def _forwardCall(self, args, assignFun, callFun):
		argMap = self._assignArgs(args, assignFun)
		makeGenerator = lambda wmsPrefix: (wmsPrefix, callFun(self.wmsMap[wmsPrefix], argMap[wmsPrefix]))
		activeWMS = filter(lambda wmsPrefix: wmsPrefix in argMap, self.wmsMap)
		for result in utils.getThreadedGenerator(map(makeGenerator, activeWMS)):
			yield result
