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

from grid_control import QM, Job, RuntimeError, utils, AbstractError, LoadableObject

class Report(LoadableObject):
	def __init__(self, jobDB, task, jobs = None, configString = ''):
		if jobs == None:
			jobs = jobDB.getJobs()
		(self._jobDB, self._task, self._jobs) = (jobDB, task, jobs)
		# FIXME: really store task for later access? maybe just use task during init run?
		self._header = self._getHeader(45)

	def _getHeader(self, maxLen = 45):
		tmp = self._task.taskConfigName + ' / ' + self._task.taskID
		if len(tmp) < maxLen:
			return tmp
		tmp = self._task.taskConfigName
		if len(tmp) < maxLen:
			return tmp
		return self._task.taskID

	def getHeight(self):
		raise AbstractError

	def display(self):
		raise AbstractError
Report.registerObject()


class BasicReport(Report):
	def _printHeader(self, message, level = -1):
		utils.vprint('-'*65, level)
		utils.vprint(message + self._header.rjust(65 - len(message)), level)
		utils.vprint(('-'*15).ljust(65), level)

	def getHeight(self):
		return 14

	def display(self):
		summary = map(lambda x: 0.0, Job.states)
		defaultJob = Job()
		for jobNum in self._jobs:
			summary[self._jobDB.get(jobNum, defaultJob).state] += 1
		makeSum = lambda *states: sum(map(lambda z: summary[z], states))
		makePer = lambda *states: [makeSum(*states), round(makeSum(*states) / len(self._jobDB) * 100.0)]

		# Print report summary
		self._printHeader('REPORT SUMMARY:')
		utils.vprint('Total number of jobs:%9d     Successful jobs:%8d  %3d%%' % \
			tuple([len(self._jobDB)] + makePer(Job.SUCCESS)), -1)
		utils.vprint('Jobs assigned to WMS:%9d        Failing jobs:%8d  %3d%%' % \
			tuple([makeSum(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)] +
			makePer(Job.ABORTED, Job.CANCELLED, Job.FAILED)), -1)
		utils.vprint(' ' * 65 + '\nDetailed Status Information:      ', -1, newline = False)
		ignored = len(self._jobDB) - sum(summary)
		if ignored:
			utils.vprint('(Jobs    IGNORED:%8d  %3d%%)' % (ignored, ignored / len(self._jobDB) * 100.0), -1)
		else:
			utils.vprint(' ' * 31, -1)
		for stateNum, category in enumerate(Job.states):
			utils.vprint('Jobs  %9s:%8d  %3d%%     ' % tuple([category] + makePer(stateNum)), \
				-1, newline = stateNum % 2)
		utils.vprint('-' * 65, -1)
		return 0


class CategoryReport(Report):
	def __init__(self, jobDB, task, jobs = None, configString = ''):
		Report.__init__(self, jobDB, task, jobs, configString)
		catJobs = {}
		catDescDict = {}
		# Assignment of jobs to categories (depending on variables and using datasetnick if available)
		for jobNum in self._jobs:
			jobConfig = task.getJobConfig(jobNum)
			varList = sorted(filter(lambda var: '!' not in repr(var), jobConfig.keys()))
			if 'DATASETSPLIT' in varList:
				varList.remove('DATASETSPLIT')
				varList.append('DATASETNICK')
			catKey = str.join('|', map(lambda var: '%s=%s' % (var, jobConfig[var]), varList))
			catJobs.setdefault(catKey, []).append(jobNum)
			if catKey not in catDescDict:
				catDescDict[catKey] = dict(map(lambda var: (var, jobConfig[var]), varList))
		# Kill redundant keys from description
		commonVars = dict(map(lambda var: (var, jobConfig[var]), varList)) # seed with last varList
		for catKey in catDescDict:
			for key in commonVars.keys():
				if key not in catDescDict[catKey].keys():
					commonVars.pop(key)
				elif commonVars[key] != catDescDict[catKey][key]:
					commonVars.pop(key)
		for catKey in catDescDict:
			for commonKey in commonVars:
				catDescDict[catKey].pop(commonKey)
		# Generate job-category map with efficient int keys - catNum becomes the new catKey
		self._job2cat = {}
		self._catDescDict = {}
		for catNum, catKey in enumerate(sorted(catJobs)):
			self._catDescDict[catNum] = catDescDict[catKey]
			self._job2cat.update(dict.fromkeys(catJobs[catKey], catNum))

	def _formatDesc(self, desc, others):
		if isinstance(desc, str):
			result = desc
		else:
			desc = dict(desc) # perform copy to allow dict.pop(...) calls
			tmp = []
			if 'DATASETNICK' in desc:
				tmp = ['Dataset: %s' % desc.pop('DATASETNICK')]
			result = str.join(', ', tmp + map(lambda key: '%s = %s' % (key, desc[key]), desc))
		if others > 1:
			result += ' (%d subtasks)' % others
		return result

	def _getCategoryStateSummary(self):
		catStateDict = {}
		defaultJob = Job()
		for jobNum in self._jobs:
			jobState = self._jobDB.get(jobNum, defaultJob).state
			catKey = self._job2cat[jobNum]
			catStateDict[catKey][jobState] = catStateDict.setdefault(catKey, {}).get(jobState, 0) + 1
		return (catStateDict, dict(self._catDescDict), {}) # (<state overview>, <descriptions>, <#subcategories>)


class LocationReport(Report):
	def display(self):
		reports = []
		for jobNum in self._jobs:
			jobObj = self._jobDB.get(jobNum)
			if not jobObj or (jobObj.state == Job.INIT):
				continue
			reports.append({0: jobNum, 1: Job.states[jobObj.state], 2: jobObj.wmsId})
			if utils.verbosity() > 0:
				history = jobObj.history.items()
				history.reverse()
				for at, dest in history:
					if dest != 'N/A':
						reports.append({1: at, 2: ' -> ' + dest})
			elif jobObj.get('dest', 'N/A') != 'N/A':
				reports.append({2: ' -> ' + jobObj.get('dest')})
		utils.printTabular(zip(range(3), ['Job', 'Status / Attempt', 'Id / Destination']), reports, 'rcl')
		utils.vprint()
