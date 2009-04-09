# Generic base class for grid proxies
# instantiates named class instead (default is VomsProxy)
import sys
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

	def warn(self, secleft):
		print >> sys.stderr, \
			"Proxy lifetime (%d seconds) does not meet the walltime requirements of %d seconds (%d hours)!\n" \
			"INFO: Disabling job submission." % (self.timeleft(), secleft, secleft / 60 / 60)

	def getUsername(self):
		pass


class TrivialProxy(Proxy):
	def timeleft(self, critical = None):
		if critical != None:
			return critical + 1
		else:
			return self._critical

	def getUsername(self):
		return os.popen3('whoami')[1].read().strip()
