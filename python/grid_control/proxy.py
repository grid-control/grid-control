# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

import sys

class Proxy:
	def __init__(self):
		self._critical = 300

	# static method to open instance
	def open(name = 'VomsProxy'):
		cls = getattr(sys.modules['grid_control'], name)
		return cls()
	open = staticmethod(open)


	# check for time left (includes critical time)
	def check(self, timeleft):
		timeleft += self._critical
		return self.timeleft(timeleft) >= timeleft


	def critical(self):
		return not self.check(0)
