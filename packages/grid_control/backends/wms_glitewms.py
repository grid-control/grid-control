# | Copyright 2007-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, time, random
from grid_control import utils
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_grid import GridWMS
from grid_control.utils.parsing import parseStr
from grid_control.utils.process_base import LocalProcess
from python_compat import md5_hex, sort_inplace

def choice_exp(sample, p = 0.5):
	for x in sample:
		if random.random() < p:
			return x
	return sample[-1]

class DiscoverWMS_Lazy(object): # TODO: Move to broker infrastructure
	def __init__(self, config):
		self.statePath = config.getWorkPath('glitewms.info')
		(self.wms_ok, self.wms_all, self.pingDict, self.pos) = self.loadState()
		self.wms_timeout = {}
		self._full = config.getBool('wms discover full', True, onChange = None)
		self._exeLCGInfoSites = utils.resolveInstallPath('lcg-infosites')
		self._exeGliteWMSJobListMatch = utils.resolveInstallPath('glite-wms-job-list-match')

	def loadState(self):
		try:
			assert(os.path.exists(self.statePath))
			tmp = utils.PersistentDict(self.statePath, ' = ')
			pingDict = {}
			for wms in tmp:
				isOK, ping, ping_time = tuple(tmp[wms].split(',', 2))
				if utils.parseBool(isOK):
					pingDict[wms] = (parseStr(ping, float), parseStr(ping_time, float, 0))
			return (pingDict.keys(), tmp.keys(), pingDict, 0)
		except Exception:
			return ([], [], {}, None)

	def updateState(self):
		tmp = {}
		for wms in self.wms_all:
			pingentry = self.pingDict.get(wms, (None, 0))
			tmp[wms] = '%r,%s,%s' % (wms in self.wms_ok, pingentry[0], pingentry[1])
		utils.PersistentDict(self.statePath, ' = ').write(tmp)

	def listWMS_all(self):
		result = []
		proc = LocalProcess(self._exeLCGInfoSites, 'wms')
		for line in proc.stdout.iter(timeout = 10):
			result.append(line.strip())
		proc.status_raise(timeout = 0)
		random.shuffle(result)
		return result

	def matchSites(self, endpoint):
		log = utils.ActivityLog('Discovering available WMS services - testing %s' % endpoint)
		checkArgs = ['-a']
		if endpoint:
			checkArgs.extend(['-e', endpoint])
		checkArgs.append(utils.pathShare('null.jdl'))

		proc = LocalProcess(self._exeGliteWMSJobListMatch, *checkArgs)
		result = []
		for line in proc.stdout.iter(timeout = 3):
			if line.startswith(' - '):
				result.append(line[3:].strip())
		if proc.status(timeout = 0) is None:
			self.wms_timeout[endpoint] = self.wms_timeout.get(endpoint, 0) + 1
			if self.wms_timeout.get(endpoint, 0) > 10: # remove endpoints after 10 failures
				self.wms_all.remove(endpoint)
			log.finish()
			return []
		log.finish()
		return result

	def getSites(self):
		return self.matchSites(self.getWMS())

	def listWMS_good(self):
		if (self.pos is None) or (len(self.wms_all) == 0): # initial discovery
			self.pos = 0
			self.wms_all = self.listWMS_all()
		if self._full:
			if not self.wms_ok:
				for wms in self.wms_all:
					match = self.matchSites(wms)
					if match:
						self.wms_ok.append(wms)
				self.updateState()
			return self.wms_ok
		if self.pos == len(self.wms_all): # self.pos = None => perform rediscovery in next step
			self.pos = 0
		else:
			wms = self.wms_all[self.pos]
			if wms in self.wms_ok:
				self.wms_ok.remove(wms)
			if len(self.matchSites(wms)):
				self.wms_ok.append(wms)
			self.pos += 1
			if self.pos == len(self.wms_all): # mark finished
				self.wms_ok.append(None)
		return self.wms_ok

	def getWMS(self):
		log = utils.ActivityLog('Discovering available WMS services')
		wms_best_list = []
		for wms in self.listWMS_good():
			log = utils.ActivityLog('Discovering available WMS services - pinging %s' % wms)
			if wms is None:
				continue
			ping, pingtime = self.pingDict.get(wms, (None, 0))
			if time.time() - pingtime > 30 * 60: # check every ~30min
				ping = utils.ping_host(wms.split('://')[1].split('/')[0].split(':')[0])
				self.pingDict[wms] = (ping, time.time() + 10 * 60 * random.random()) # 10 min variation
			if ping is not None:
				wms_best_list.append((wms, ping))
			log.finish()
		log.finish()
		if not wms_best_list:
			return None
		sort_inplace(wms_best_list, key = lambda name_ping: name_ping[1])
		result = choice_exp(wms_best_list)
		if result is not None:
			log = utils.ActivityLog('Discovering available WMS services - using %s' % result)
			wms, ping = result # reduce timeout by 5min for chosen wms => re-ping every 6 submits
			self.pingDict[wms] = (ping, self.pingDict[wms][1] + 5*60)
			result = wms
			log.finish()
		self.updateState()
		return result


class GliteWMS(GridWMS):
	configSections = GridWMS.configSections + ['glite-wms', 'glitewms'] # backwards compatibility

	def __init__(self, config, name):
		GridWMS.__init__(self, config, name)

		self._delegateExec = utils.resolveInstallPath('glite-wms-job-delegate-proxy')
		self._submitExec = utils.resolveInstallPath('glite-wms-job-submit')
		self._statusExec = utils.resolveInstallPath('glite-wms-job-status')
		self._outputExec = utils.resolveInstallPath('glite-wms-job-output')
		self._cancelExec = utils.resolveInstallPath('glite-wms-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config': self._configVO})
		self._useDelegate = config.getBool('try delegate', True, onChange = None)
		self._forceDelegate = config.getBool('force delegate', False, onChange = None)
		self._discovery_module = None
		if config.getBool('discover wms', True, onChange = None):
			self._discovery_module = DiscoverWMS_Lazy(config)
		self._discover_sites = config.getBool('discover sites', False, onChange = None)


	def getSites(self):
		if self._discover_sites and self._discovery_module:
			return self._discovery_module.getSites()


	def bulkSubmissionBegin(self):
		self._submitParams.update({ '-d': None })
		if self._discovery_module:
			self._submitParams.update({ '-e': self._discovery_module.getWMS() })
		if self._useDelegate is False:
			self._submitParams.update({ '-a': ' ' })
			return True
		dID = 'GCD' + md5_hex(str(time.time()))[:10]
		activity = utils.ActivityLog('creating delegate proxy for job submission')
		deletegateArgs = []
		if self._configVO:
			deletegateArgs.extend(['--config', self._configVO])
		proc = LocalProcess(self._delegateExec, '-d', dID, '--noint', '--logfile', '/dev/stderr', *deletegateArgs)
		output = proc.get_output(timeout = 10, raise_errors = False)
		if ('glite-wms-job-delegate-proxy Success' in output) and (dID in output):
			self._submitParams.update({ '-d': dID })
		del activity

		if proc.status(timeout = 0, terminate = True) != 0:
			self._log.log_process(proc)
		return (self._submitParams.get('-d', None) is not None)


	def submitJobs(self, jobNumList, module):
		if not self.bulkSubmissionBegin(): # Trying to delegate proxy failed
			if self._forceDelegate: # User switched on forcing delegation => exception
				raise BackendError('Unable to delegate proxy!')
			utils.eprint('Unable to delegate proxy! Continue with automatic delegation...')
			self._submitParams.update({ '-a': ' ' })
			self._useDelegate = False
		for submitInfo in GridWMS.submitJobs(self, jobNumList, module):
			yield submitInfo
