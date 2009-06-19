# Generic base class for grid proxies
import sys, os, time, popen2
from grid_control import AbstractObject, InstallationError, AbstractError, utils

class Proxy(AbstractObject):
	def __init__(self):
		self.lowerLimit = 300
		self._lastUpdate = 0

	# check for time left (includes lower time limit)
	def check(self, checkedForTime):
		checkedForTime += self.lowerLimit
		return self.timeleft(checkedForTime) >= checkedForTime

	# return (possibly cached) time left
	def timeleft(self, checkedForTime = None):
		if not checkedForTime:
			checkedForTime = self.lowerLimit
		delta = time.time() - self._lastUpdate
		cachedTimeleft = max(0, self.getTimeleft(False, checkedForTime) - delta)
		# recheck proxy:
		#  * when time is running out (but at most once per minute)
		#  * after at least 30min have passed
		if (cachedTimeleft < checkedForTime and delta > 60) or delta > 30*60:
			self._lastUpdate = time.time()
			result = self.getTimeleft(True, checkedForTime)
			vprint("The Proxy now has %s left" % utils.strTime(result), time = True)
			return result
		else:
			return cachedTimeleft

	def getTimeleft(self, cached, checkedForTime = None):
		raise AbstractError

	def getUsername(self):
		return os.environ['LOGNAME']

	def getVO(self):
		raise AbstractError


class TrivialProxy(Proxy):
	def getTimeleft(self, cached, checkedForTime = None):
		# Always return more than requested...
		if checkedForTime != None:
			return checkedForTime + 1
		return self.lowerLimit


class VomsProxy(Proxy):
	def __init__(self):
		Proxy.__init__(self)
		self._infoExec = utils.searchPathFind('voms-proxy-info')
		self._info = None
		self._cache = None

	def _getInfo(self, cached = True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call voms-proxy-info and parse results
		proc = popen2.Popen3("%s --all" % self._infoExec, True)
		retCode = proc.wait()
		if retCode != 0:
			sys.stderr.write(proc.fromchild.read())
			raise InstallationError("voms-proxy-info failed with return code %d" % retCode)
		self._cache = utils.DictFormat(':').parse(proc.fromchild.readlines())
		return self._cache

	def getTimeleft(self, cached, checkedForTime = None):
		return utils.parseTime(self._getInfo(cached)['timeleft'])

	def getUsername(self):
		return '/CN=%s' % self._getInfo()['identity'].split('CN=')[1].strip()

	def getVO(self):
		return self._getInfo()['vo']
