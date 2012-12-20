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
