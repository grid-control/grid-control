# Generic base class for authentication proxies GCSCF:
import os, time, getpass, shutil, stat
from grid_control import QM, NamedObject, InstallationError, AbstractError, UserError, utils
from python_compat import parsedate

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


class RefreshableProxy(TimedProxy):
	def __init__(self, config, name):
		TimedProxy.__init__(self, config, name)
		self._refresh = config.getList('proxy refresh', '1:00:00', onChange = None)

	def _refreshProxy(self):
		raise AbstractError

	def _checkTimeleft(self, neededTime): # check for time left
		if self._getTimeleft(True) < self._refresh:
			self._refreshProxy()
			self._getTimeleft(False)
		return TimedProxy._checkTimeleft(self, neededTime)


class AFSProxy(RefreshableProxy):
	def __init__(self, config, name):
		RefreshableProxy.__init__(self, config, name)
		self._kinitExec = utils.resolveInstallPath('kinit')
		self._klistExec = utils.resolveInstallPath('klist')
		self._aklogExec = utils.resolveInstallPath('aklog')
		self._cache = None
		self._proxyPaths = {}
		for name in ['KRB5CCNAME', 'KRBTKFILE']:
			self._proxyPaths[name] = config.getWorkPath('proxy.%s' % name)
		self._backupTickets()
		self._tickets = config.getList('tickets', [], onChange = None)

	def _backupTickets(self):
		for name in ['KRB5CCNAME', 'KRBTKFILE']:
			if name not in os.environ:
				raise RuntimeError('Environment variable "%s" not found!' % name)
			oldFN = os.environ[name].replace('FILE:', '')
			newFN = self._proxyPaths[name]
			shutil.copyfile(oldFN, newFN)
			os.chmod(newFN, stat.S_IRUSR | stat.S_IWUSR)
			os.environ[name] = newFN

	def _refreshProxy(self, cached):
		return utils.LoggedProcess(self._kinitExec, '-R').wait()
		
	def _parseTickets(self, cached = True):
		# Return cached results if requested
		if cached and self._cache:
			return self._cache
		# Call klist and parse results
		proc = utils.LoggedProcess(self._klistExec, '-v')
		retCode = proc.wait()
		self._cache = {}
		for sectionInfo in utils.accumulate(proc.getOutput(), '', lambda x, buf: buf.endswith('\n\n')):
			parseDate = lambda x: time.mktime(parsedate(x))
			tmp = utils.DictFormat(':').parse(sectionInfo, valueParser = {'auth time': parseDate,
				'start time': parseDate, 'end time': parseDate, 'renew till': parseDate})
			if 'server' in tmp:
				self._cache[tmp['server']] = tmp
			else:
				self._cache[None] = tmp
		return self._cache

	def _getTimeleft(self, cached):
		info = self._parseTickets(cached)
		time_current = time.time()
		time_end = time_current
		for ticket in info:
			if ((ticket not in self._tickets) and self._tickets) or not ticket:
				continue
			time_end = max(info[ticket]['end time'], time_end)
		return time_end - time_current
