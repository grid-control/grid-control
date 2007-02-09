# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)

from grid_control import AbstractObject

class Proxy(AbstractObject):
	def __init__(self):
		self._critical = 300

	# check for time left (includes critical time)
	def check(self, timeleft):
		timeleft += self._critical
		return self.timeleft(timeleft) >= timeleft

	def critical(self):
		return not self.check(0)
