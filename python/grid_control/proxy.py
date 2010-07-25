# Generic base class for grid proxies
import os, time
from grid_control import AbstractObject, InstallationError, AbstractError, UserError, utils

class Proxy(AbstractObject):
	def __init__(self, config):
		self.lowerLimit = 300
		self._lastUpdate = 0

	def canSubmit(self, neededTime, canCurrentlySubmit):
		if not self._check(self.lowerLimit):
			raise UserError('Your proxy only has %d seconds left!' % self.getTimeleft(cached = True))
		if not self._check(self.lowerLimit + neededTime) and canCurrentlySubmit:
			utils.vprint("Proxy lifetime (%s) does not meet the walltime requirements (%s)!"
				% (utils.strTime(self.getTimeleft(cached = True)), utils.strTime(neededTime)), -1, printTime = True)
			utils.vprint("Disabling job submission", -1, printTime = True)
			return False
		return True

	# check for time left
	def _check(self, neededTime):
		delta = time.time() - self._lastUpdate
		timeleft = max(0, self.getTimeleft(cached = True) - delta)
		# recheck proxy => after > 30min have passed or when time is running out (max every 5 minutes)
		if (delta > 30 * 60) or (timeleft < neededTime and delta > 5 * 60):
			self._lastUpdate = time.time()
			timeleft = self.getTimeleft(cached = False)
			verbosity = (0, -1)[timeleft < neededTime]
			utils.vprint("The proxy now has %s left" % utils.strTime(timeleft), verbosity, printTime = True)
		return timeleft >= neededTime

	def getTimeleft(self, cached):
		raise AbstractError

	def getUsername(self):
		return os.environ['LOGNAME']

	def getVO(self):
		return 'None'

	def getAuthFile(self):
		return None

Proxy.dynamicLoaderPath()


class TrivialProxy(Proxy):
	def canSubmit(self, neededTime, canCurrentlySubmit):
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

	def get(self, key, parse = lambda x: x, cached = True):
		info = self._getInfo(cached)
		try:
			return parse(info[key])
		except:
			print info
			raise RuntimeError("Can't parse proxy information!")

	def getTimeleft(self, cached):
		return self.get('timeleft', utils.parseTime, cached)

	def getUsername(self):
		return self.get('identity', lambda x: '/CN=%s' % x.split('CN=')[1].strip())

	def getVO(self):
		return self.get('vo')

	def getAuthFile(self):
		return self.get('path')
