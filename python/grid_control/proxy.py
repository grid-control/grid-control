# Generic base class for grid proxies
import os, time
from grid_control import AbstractObject, InstallationError, AbstractError, UserError, utils

class Proxy(AbstractObject):
	def __init__(self, config):
		self.lowerLimit = 300
		self._lastUpdate = 0

	def canSubmit(self, length, flag):
		if not self.check(0):
			raise UserError('Your proxy only has %d seconds left!' % self.timeleft())
		if not self.check(length) and flag:
			utils.vprint("Proxy lifetime (%s) does not meet the walltime requirements (%s)!"
				% (utils.strTime(self.timeleft()), utils.strTime(length)), -1, printTime = True)
			utils.vprint("Disabling job submission", -1, printTime = True)
			return False
		return True

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
		if (cachedTimeleft < checkedForTime and delta > 60) or delta > 60*60:
			self._lastUpdate = time.time()
			result = self.getTimeleft(True, checkedForTime)
			if cachedTimeleft < checkedForTime:
				verbosity = -1
			else:
				verbosity = 0
			utils.vprint("The proxy now has %s left" % utils.strTime(result), verbosity, printTime = True)
			return result
		else:
			return cachedTimeleft

	def getTimeleft(self, cached, checkedForTime = None):
		raise AbstractError

	def getUsername(self):
		return os.environ['LOGNAME']

	def getVO(self):
		return 'None'

Proxy.dynamicLoaderPath()


class TrivialProxy(Proxy):
	def canSubmit(self, length, flag):
		return True


class VomsProxy(Proxy):
	def __init__(self, config):
		Proxy.__init__(self, config)
		self._infoExec = utils.searchPathFind('voms-proxy-info')
		self.ignoreWarning = config.getBool('proxy', 'ignore warnings', False, volatile=True)
		self._info = None
		self._cache = None

	def _getInfo(self, cached = True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call voms-proxy-info and parse results
		proc = utils.LoggedProcess(self._infoExec, "--all")
		retCode = proc.wait()
		if (retCode != 0) and not self.ignoreWarning:
			msg = ("%s\n%s\n" % (proc.getOutput(), proc.getError())).replace('\n\n', '\n')
			msg += "If job submission is still possible, you can set [proxy] ignore warnings = True\n"
			raise InstallationError(msg + "voms-proxy-info failed with return code %d" % retCode)
		self._cache = utils.DictFormat(':').parse(proc.getOutput())
		return self._cache

	def getTimeleft(self, cached, checkedForTime = None):
		info = self._getInfo(cached)
		try:
			return utils.parseTime(info['timeleft'])
		except:
			print info
			raise RuntimeError("Can't parse proxy information!")

	def getUsername(self):
		try:
			return '/CN=%s' % self._getInfo()['identity'].split('CN=')[1].strip()
		except:
			print self._getInfo(cached)
			raise RuntimeError("Can't parse proxy information!")

	def getVO(self):
		try:
			return self._getInfo()['vo']
		except:
			print self._getInfo()
			raise RuntimeError("Can't parse proxy information!")
