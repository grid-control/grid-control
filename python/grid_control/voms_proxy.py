import os, time, popen2
from grid_control import InstallationError, Proxy, utils

class VomsProxy(Proxy):
	def __init__(self):
		Proxy.__init__(self)
		self._infoExec = utils.searchPathFind('voms-proxy-info')
		self._info = None


	# Call voms-proxy-info and returns results
	def _getInfo(self):
		proc = popen2.Popen3("%s --all" % self._infoExec, True)
		lines = proc.fromchild.readlines()
		retCode = proc.wait()
		if retCode != 0:
			sys.stderr.write(str.join('', lines))
			raise InstallationError("voms-proxy-info failed with return code %d" % retCode)
		return utils.DictFormat(':').parse(lines)


	# return possibly cached information
	def _getInfoCached(self, recheck = False):
		if self._info == None or recheck:
			self._info = self._getInfo()
			self._info['time'] = time.time()
		return self._info


	def timeleft(self, critical = None):
		if critical == None:
			critical = self._critical

		info = self._getInfoCached()
		# time elapsed since last call to voms-proxy-info
		delta = time.time() - info['time']

		while True:
			# subtract time since last call to voms-proxy-info
			timeleft = max(0, utils.parseTime(info['timeleft']) - delta)

			# recheck proxy if critical timeleft reached
			# at most once per minute
			if timeleft < critical and delta > 60:
				info = self._getInfoCached(True)
				continue
			break # leave while loop
		return timeleft


	def getUsername(self):
		info = self._getInfoCached()
		return '/CN=' + info['identity'].split('CN=')[1].strip()


	def getVO(self):
		info = self._getInfoCached()
		return info['vo']
