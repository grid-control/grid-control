# Generic base class for authentication proxies
import os, time, getpass, shutil, stat
from grid_control import QM, NamedObject, InstallationError, AbstractError, UserError, utils

class Proxy(NamedObject):
	getConfigSections = NamedObject.createFunction_getConfigSections(['proxy'])

	def getUsername(self):
		raise AbstractError

	def getFQUsername(self):
		return self.getUsername()

	def getGroup(self):
		raise AbstractError

	def getAuthFile(self):
		raise AbstractError

	def canSubmit(self, neededTime, canCurrentlySubmit):
		raise AbstractError
Proxy.registerObject()


class MultiProxy(Proxy):
	def __init__(self, config, name, subproxies):
		Proxy.__init__(self, config, name)
		self._subproxies = map(lambda pbuilder: pbuilder(), subproxies)

	def getUsername(self):
		return self._subproxies[0].getUsername()

	def getFQUsername(self):
		return self._subproxies[0].getFQUsername()

	def getGroup(self):
		return self._subproxies[0].getGroup()

	def getAuthFile(self):
		return self._subproxies[0].getAuthFile()

	def canSubmit(self, neededTime, canCurrentlySubmit):
		for subproxy in self._subproxies:
			canCurrentlySubmit = canCurrentlySubmit and subproxy.canSubmit(neededTime, canCurrentlySubmit)
		return canCurrentlySubmit


class TrivialProxy(Proxy):
	def getUsername(self):
		return getpass.getuser()

	def getGroup(self):
		return os.environ.get('GROUP', 'None')

	def getAuthFile(self):
		return None

	def canSubmit(self, neededTime, canCurrentlySubmit):
		return True


class TimedProxy(Proxy):
	def __init__(self, config, name):
		Proxy.__init__(self, config, name)
		self._lowerLimit = config.getTime('min lifetime', 300, onChange = None)
		self._maxQueryTime = config.getTime('max query time',  5 * 60, onChange = None)
		self._minQueryTime = config.getTime('min query time', 30 * 60, onChange = None)
		self._lastUpdate = 0

	def canSubmit(self, neededTime, canCurrentlySubmit):
		if not self._checkTimeleft(self._lowerLimit):
			raise UserError('Your proxy only has %d seconds left! (Required are %s)' %
				(self._getTimeleft(cached = True), utils.strTime(self._lowerLimit)))
		if not self._checkTimeleft(self._lowerLimit + neededTime) and canCurrentlySubmit:
			utils.vprint('Proxy lifetime (%s) does not meet the proxy and walltime (%s) requirements!' %
				(utils.strTime(self._getTimeleft(cached = False)), utils.strTime(self._lowerLimit + neededTime)), -1, printTime = True)
			utils.vprint('Disabling job submission', -1, printTime = True)
			return False
		return True

	def _getTimeleft(self, cached):
		raise AbstractError

	def _checkTimeleft(self, neededTime): # check for time left
		delta = time.time() - self._lastUpdate
		timeleft = max(0, self._getTimeleft(cached = True) - delta)
		# recheck proxy => after > 30min have passed or when time is running out (max every 5 minutes)
		if (delta > self._minQueryTime) or (timeleft < neededTime and delta > self._maxQueryTime):
			self._lastUpdate = time.time()
			timeleft = self._getTimeleft(cached = False)
			verbosity = QM(timeleft < neededTime, -1, 0)
			utils.vprint('The proxy now has %s left' % utils.strTime(timeleft), verbosity, printTime = True)
		return timeleft >= neededTime


class VomsProxy(TimedProxy):
	def __init__(self, config, name):
		TimedProxy.__init__(self, config, name)
		self._infoExec = utils.resolveInstallPath('voms-proxy-info')
		self._ignoreWarning = config.getBool('ignore warnings', False, onChange = None)
		self._cache = None
		print self._getTimeleft(False)

	def getUsername(self):
		return self._getProxyInfo('identity').split('CN=')[1].strip()

	def getFQUsername(self):
		return self._getProxyInfo('identity')

	def getGroup(self):
		return self._getProxyInfo('vo')

	def getAuthFile(self):
		return self._getProxyInfo('path')

	def _getTimeleft(self, cached):
		return self._getProxyInfo('timeleft', utils.parseTime, cached)

	def _parseProxy(self, cached = True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call voms-proxy-info and parse results
		proc = utils.LoggedProcess(self._infoExec, '--all')
		retCode = proc.wait()
		if (retCode != 0) and not self._ignoreWarning:
			msg = ('voms-proxy-info output:\n%s\n%s\n' % (proc.getOutput(), proc.getError())).replace('\n\n', '\n')
			msg += 'If job submission is still possible, you can set [proxy] ignore warnings = True\n'
			raise RuntimeError(msg + 'voms-proxy-info failed with return code %d' % retCode)
		self._cache = utils.DictFormat(':').parse(proc.getOutput())
		return self._cache

	def _getProxyInfo(self, key, parse = lambda x: x, cached = True):
		info = self._parseProxy(cached)
		try:
			return parse(info[key])
		except:
			raise RuntimeError("Can't access %s in proxy information:\n%s" % (key, info))
