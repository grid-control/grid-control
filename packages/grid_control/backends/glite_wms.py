#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from python_compat import md5
import tempfile, time, random, os
from grid_control import utils, RuntimeError
from grid_wms import GridWMS

def choice_exp(sample, p = 0.5):
	for x in sample:
		if random.random() < p:
			return x
	return sample[-1]

class DiscoverWMS_Lazy: # TODO: Move to broker infrastructure
	def __init__(self, config):
		self.statePath = config.getWorkPath('glitewms.info')
		(self.wms_ok, self.wms_all, self.pingDict, self.pos) = self.loadState()
		self.wms_timeout = {}
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
					pingDict[wms] = (utils.parseStr(ping, float), utils.parseStr(ping_time, float, 0))
			return (pingDict.keys(), tmp.keys(), pingDict, 0)
		except:
			return ([], [], {}, None)

	def updateState(self):
		tmp = {}
		for wms in self.wms_all:
			pingentry = self.pingDict.get(wms, (None, 0))
			tmp[wms] = '%r,%s,%s' % (wms in self.wms_ok, pingentry[0], pingentry[1])
		utils.PersistentDict(self.statePath, ' = ').write(tmp)

	def listWMS_all(self):
		result = []
		for line in utils.LoggedProcess(self._exeLCGInfoSites, 'wms').iter():
			result.append(line.strip())
		random.shuffle(result)
		return result

	def matchSites(self, endpoint):
		result = []
		checkArgs = '-a' 
		if endpoint:
			checkArgs += ' -e %s' % endpoint
		proc = utils.LoggedProcess(self._exeGliteWMSJobListMatch, checkArgs + ' %s' % utils.pathShare('null.jdl'))
		def matchThread(): # TODO: integrate timeout into loggedprocess
			for line in proc.iter():
				if line.startswith(' - '):
					result.append(line[3:].strip())
		thread = utils.gcStartThread('Matching jobs with WMS %s' % endpoint, matchThread)
		thread.join(timeout = 3)
		if thread.isAlive():
			proc.kill()
			thread.join()
			self.wms_timeout[endpoint] = self.wms_timeout.get(endpoint, 0) + 1
			if self.wms_timeout.get(endpoint, 0) > 10: # remove endpoints after 10 failures
				self.wms_all.remove(endpoint)
			return []
		return result

	def getSites(self):
		return self.matchSites(self.getWMS())

	def listWMS_good(self):
		if (self.pos == None) or (len(self.wms_all) == 0): # initial discovery
			self.pos = 0
			self.wms_all = self.listWMS_all()
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
			if wms == None:
				continue
			ping, pingtime = self.pingDict.get(wms, (None, 0))
			if time.time() - pingtime > 30 * 60: # check every ~30min
				ping = utils.ping_host(wms.split('://')[1].split('/')[0].split(':')[0])
				self.pingDict[wms] = (ping, time.time() + 10 * 60 * random.random()) # 10 min variation
			if ping != None:
				wms_best_list.append((wms, ping))
		wms_best_list.sort(key = lambda (name, ping): ping)
		result = choice_exp(wms_best_list)
		if result != None:
			wms, ping = result # reduce timeout by 5min for chosen wms => re-ping every 6 submits
			self.pingDict[wms] = (ping, self.pingDict[wms][1] + 5*60)
			result = wms
		self.updateState()
		del log
		return result


class GliteWMS(GridWMS):
	def __init__(self, config, wmsName = 'glite-wms'):
		GridWMS.__init__(self, config, wmsName)

		self._delegateExec = utils.resolveInstallPath('glite-wms-job-delegate-proxy')
		self._submitExec = utils.resolveInstallPath('glite-wms-job-submit')
		self._statusExec = utils.resolveInstallPath('glite-wms-job-status')
		self._outputExec = utils.resolveInstallPath('glite-wms-job-output')
		self._cancelExec = utils.resolveInstallPath('glite-wms-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config': self._configVO})
		self._useDelegate = config.getBool('try delegate', True, onChange = None)
		self._forceDelegate = config.getBool('force delegate', False, onChange = None)
		self._discovery_module = None
		if config.getBool('discover wms', False, onChange = None):
			self._discovery_module = DiscoverWMS_Lazy(config)
		self._discover_sites = config.getBool('discover sites', False, onChange = None)


	def getSites(self):
		if self._discover_sites and self._discovery_module:
			return self._discovery_module.getSites()


	def bulkSubmissionBegin(self):
		self._submitParams.update({ '-d': None })
		if self._discovery_module:
			self._submitParams.update({ '-e': self._discovery_module.getWMS() })
		if self._useDelegate == False:
			self._submitParams.update({ '-a': ' ' })
			return True
		log = tempfile.mktemp('.log')
		try:
			dID = 'GCD' + md5(str(time.time())).hexdigest()[:10]
			activity = utils.ActivityLog('creating delegate proxy for job submission')
			proc = utils.LoggedProcess(self._delegateExec, '%s -d %s --noint --logfile "%s"' %
				(utils.QM(self._configVO, '--config "%s"' % self._configVO, ''), dID, log))

			output = proc.getOutput(wait = True)
			if ('glite-wms-job-delegate-proxy Success' in output) and (dID in output):
				self._submitParams.update({ '-d': dID })
			del activity

			if proc.wait() != 0:
				proc.logError(self.errorLog, log = log)
			return (self._submitParams.get('-d', None) != None)
		finally:
			utils.removeFiles([log])


	def submitJobs(self, jobNumList, module):
		if not self.bulkSubmissionBegin(): # Trying to delegate proxy failed
			if self._forceDelegate: # User switched on forcing delegation => exception
				raise RuntimeError('Unable to delegate proxy!')
			utils.eprint('Unable to delegate proxy! Continue with automatic delegation...')
			self._submitParams.update({ '-a': ' ' })
			self._useDelegate = False
		for submitInfo in GridWMS.submitJobs(self, jobNumList, module):
			yield submitInfo
