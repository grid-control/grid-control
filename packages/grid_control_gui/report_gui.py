# | Copyright 2014-2016 Karlsruhe Institute of Technology
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

import sys
from grid_control import utils
from grid_control.job_db import Job
from grid_control.report import Report
from grid_control.utils.parsing import parseStr
from grid_control_gui.ansi import Console
from python_compat import ifilter, imap, irange, lfilter, lmap, set, sorted

class JobProgressBar(object):
	def __init__(self, total = 100, width = 16, jobsOnFinish = False):
		(self._total, self._width, self._jobsOnFinish) = (total, width, jobsOnFinish)
		self.update()

	def update(self, success = 0, queued = 0, running = 0, failed = 0):
		# Compute variables
		complete = self._width - 31
		blocks_ok   = int(round(success / float(self._total) * complete))
		blocks_proc = int(round(queued / float(self._total) * complete))
		blocks_run  = int(round(running / float(self._total) * complete))
		blocks_fail = min(complete - blocks_ok - blocks_proc - blocks_run,
			int(round(failed / float(self._total) * complete)))
		self._bar  = str(Console.fmt('=' * blocks_ok, [Console.COLOR_GREEN]))
		self._bar += str(Console.fmt('=' * blocks_run, [Console.COLOR_BLUE]))
		self._bar += str(Console.fmt('=' * blocks_proc, [Console.COLOR_WHITE]))
		self._bar += ' ' * (complete - blocks_ok - blocks_proc - blocks_run - blocks_fail)
		self._bar += str(Console.fmt('=' * blocks_fail, [Console.COLOR_RED]))
		self._bar = '[%s] ' % self._bar
		if success == self._total and self._jobsOnFinish:
			self._bar += '(%s |%s)' % (Console.fmt('%5d' % self._total, [Console.COLOR_GREEN]),
				Console.fmt('finished'.center(14), [Console.COLOR_GREEN]))
		elif success == self._total:
			self._bar += '(%s)' % (Console.fmt('finished'.center(21), [Console.COLOR_GREEN]))
		else:
			fmt = lambda x: str(x).rjust(5) # int(math.log(self._total) / math.log(10)) + 1)
			self._bar += '(%s | %s | %s | %s)' % (
				Console.fmt(fmt(success), [Console.COLOR_GREEN]),
				Console.fmt(fmt(running), [Console.COLOR_BLUE]),
				Console.fmt(fmt(queued), [Console.COLOR_WHITE]),
				Console.fmt(fmt(failed), [Console.COLOR_RED]))

	def __len__(self):
		return self._width

	def __str__(self):
		return str(self._bar)


class CategoryBaseReport(Report):
	def __init__(self, jobDB, task, jobs = None, configString = ''):
		Report.__init__(self, jobDB, task, jobs, configString)
		catJobs = {}
		catDescDict = {}
		# Assignment of jobs to categories (depending on variables and using datasetnick if available)
		jobConfig = {}
		for jobNum in self._jobs:
			if task:
				jobConfig = task.getJobConfig(jobNum)
			varList = sorted(ifilter(lambda var: '!' not in repr(var), jobConfig.keys()))
			if 'DATASETSPLIT' in varList:
				varList.remove('DATASETSPLIT')
				varList.append('DATASETNICK')
			catKey = str.join('|', imap(lambda var: '%s=%s' % (var, jobConfig[var]), varList))
			catJobs.setdefault(catKey, []).append(jobNum)
			if catKey not in catDescDict:
				catDescDict[catKey] = dict(imap(lambda var: (var, jobConfig[var]), varList))
		# Kill redundant keys from description
		commonVars = dict(imap(lambda var: (var, jobConfig[var]), varList)) # seed with last varList
		for catKey in catDescDict:
			for key in list(commonVars.keys()):
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
			result = str.join(', ', tmp + lmap(lambda key: '%s = %s' % (key, desc[key]), desc))
		if others > 1:
			result += ' (%d subtasks)' % others
		return result

	def _getCategoryStateSummary(self):
		catStateDict = {}
		defaultJob = Job()
		for jobNum in self._jobs:
			jobState = self._jobDB.get(jobNum, defaultJob).state
			catKey = self._job2cat[jobNum]
			catStateDict[catKey][jobState] = catStateDict.setdefault(catKey, dict()).get(jobState, 0) + 1
		return (catStateDict, dict(self._catDescDict), {}) # (<state overview>, <descriptions>, <#subcategories>)


class ModuleReport(CategoryBaseReport):
	alias = ['module']

	def display(self):
		(catStateDict, catDescDict, _) = CategoryBaseReport._getCategoryStateSummary(self)
		infos = []
		head = set()
		stateCat = {Job.SUCCESS: 'SUCCESS', Job.FAILED: 'FAILED', Job.RUNNING: 'RUNNING', Job.DONE: 'RUNNING'}
		for catKey in catDescDict:
			tmp = dict(catDescDict[catKey])
			head.update(tmp.keys())
			for stateKey in catStateDict[catKey]:
				state = stateCat.get(stateKey, 'WAITING')
				tmp[state] = tmp.get(state, 0) + catStateDict[catKey][stateKey]
			infos.append(tmp)

		stateCatList = ['WAITING', 'RUNNING', 'FAILED', 'SUCCESS']
		utils.printTabular(lmap(lambda x: (x, x), sorted(head) + stateCatList),
			infos, 'c' * len(head), fmt = dict.fromkeys(stateCatList, lambda x: '%7d' % parseStr(x, int, 0)))


class AdaptiveBaseReport(CategoryBaseReport):
	def __init__(self, jobDB, task, jobs = None, configString = ''):
		CategoryBaseReport.__init__(self, jobDB, task, jobs, configString)
		self._catMax = int(configString)
		self._catCur = self._catMax

	# Function to merge categories together
	def _mergeCats(self, catStateDict, catDescDict, catSubcatDict, catLenDict, newCatDesc, catKeyList):
		if not catKeyList:
			return
		newCatKey = max(catStateDict.keys()) + 1
		newStates = {}
		newLen = 0
		newSubcats = 0
		for catKey in catKeyList:
			oldStates = catStateDict.pop(catKey)
			for stateKey in oldStates:
				newStates[stateKey] = newStates.get(stateKey, 0) + oldStates[stateKey]
			catDescDict.pop(catKey)
			newLen += catLenDict.pop(catKey)
			newSubcats += catSubcatDict.pop(catKey, 1)
		catStateDict[newCatKey] = newStates
		catDescDict[newCatKey] = newCatDesc
		catLenDict[newCatKey] = newLen
		catSubcatDict[newCatKey] = newSubcats
		return newCatKey


	# Get dictionary with categories that will get merged when removing a variable
	def _getKeyMergeResults(self, catDescDict):
		# Merge parameters to reach category goal - NP hard problem, so be greedy and quick!
		def eqDict(a, b, k):
			a = dict(a)
			b = dict(b)
			a.pop(k)
			b.pop(k)
			return a == b

		varKeyResult = {}
		catKeySearchDict = {}
		for catKey in catDescDict:
			for varKey in catDescDict[catKey]:
				if varKey not in catKeySearchDict:
					catKeySearch = set(catDescDict.keys())
				else:
					catKeySearch = catKeySearchDict[varKey]
				if catKeySearch:
					matches = lfilter(lambda ck: eqDict(catDescDict[catKey], catDescDict[ck], varKey), catKeySearch)
					if matches:
						catKeySearchDict[varKey] = catKeySearch.difference(set(matches))
						varKeyResult.setdefault(varKey, []).append(matches)
		return varKeyResult


	def _mergeCatsWithGoal(self, catStateDict, catDescDict, catSubcatDict, catLenDict, hiddenDesc):
		# Merge categories till goal is reached
		while True:
			deltaGoal = max(0, (len(catDescDict) + len(hiddenDesc)) - self._catMax)
			varKeyResult = self._getKeyMergeResults(catDescDict)
			(varKeyMerge, varKeyMergeDelta) = (None, 0)
			for varKey in sorted(varKeyResult):
				delta = sum(imap(len, varKeyResult[varKey])) - len(varKeyResult[varKey])
				if (delta <= deltaGoal) and (delta > varKeyMergeDelta):
					(varKeyMerge, varKeyMergeDelta) = (varKey, delta)
			if varKeyMerge:
				for mergeList in varKeyResult[varKeyMerge]:
					varKeyMergeDesc = dict(catDescDict[mergeList[0]])
					varKeyMergeDesc.pop(varKeyMerge)
					self._mergeCats(catStateDict, catDescDict, catSubcatDict, catLenDict,
						varKeyMergeDesc, mergeList)
			else:
				break


	def _clearCategoryDesc(self, varKeyResult, catDescDict):
		for varKey in sorted(varKeyResult, reverse = True):
			if varKey == 'DATASETNICK': # Never remove dataset infos
				continue
			if sum(imap(len, varKeyResult[varKey])) - len(varKeyResult[varKey]) == 0:
				catDescSet = set()
				for catKey in catDescDict:
					descDict = dict(catDescDict[catKey])
					descDict.pop(varKey)
					desc = self._formatDesc(descDict, 0)
					if desc in catDescSet:
						break
					catDescSet.add(desc)
				if len(catDescSet) == len(catDescDict):
					for catKey in catDescDict:
						if varKey in catDescDict[catKey]:
							catDescDict[catKey].pop(varKey)


	def _getCategoryStateSummary(self):
		(catStateDict, catDescDict, catSubcatDict) = CategoryBaseReport._getCategoryStateSummary(self)
		# Used for quick calculations
		catLenDict = {}
		for catKey in catStateDict:
			catLenDict[catKey] = sum(catStateDict[catKey].values())
		# Merge successfully completed categories
		self._mergeCats(catStateDict, catDescDict, catSubcatDict, catLenDict,
			'Completed subtasks', lfilter(lambda catKey:
				(len(catStateDict[catKey]) == 1) and (Job.SUCCESS in catStateDict[catKey]), catStateDict))
		# Next merge steps shouldn't see non-dict catKeys in catDescDict
		hiddenDesc = {}
		for catKey in ifilter(lambda catKey: not isinstance(catDescDict[catKey], dict), list(catDescDict)):
			hiddenDesc[catKey] = catDescDict.pop(catKey)
		# Merge categories till goal is reached
		self._mergeCatsWithGoal(catStateDict, catDescDict, catSubcatDict, catLenDict, hiddenDesc)
		# Remove redundant variables from description
		varKeyResult = self._getKeyMergeResults(catDescDict)
		self._clearCategoryDesc(varKeyResult, catDescDict)
		# Restore hidden descriptions
		catDescDict.update(hiddenDesc)
		# Enforce category maximum - merge categories with the least amount of jobs
		if len(catStateDict) != self._catMax:
			self._mergeCats(catStateDict, catDescDict, catSubcatDict, catLenDict, 'Remaining subtasks',
				sorted(catStateDict, key = lambda catKey: -catLenDict[catKey])[self._catMax - 1:])
		# Finalize descriptions:
		if len(catDescDict) == 1:
			catDescDict[list(catDescDict.keys())[0]] = 'All jobs'
		return (catStateDict, catDescDict, catSubcatDict)


class GUIReport(AdaptiveBaseReport):
	alias = ['modern']

	def __init__(self, jobDB, task, jobs = None, configString = ''):
		(self.maxY, self.maxX) = Console().getmaxyx()
		self.maxX -= 10 # Padding
		AdaptiveBaseReport.__init__(self, jobDB, task, jobs, str(int(self.maxY / 5)))

	def getHeight(self):
		return self._catCur * 2 + 3

	def printLimited(self, value, width, rvalue = ''):
		if len(value) + len(rvalue) > width:
			value = str(value)[:width - 3 - len(rvalue)] + '...'
		sys.stdout.write(str(value) + ' ' * (width - (len(value) + len(rvalue))) + str(rvalue) + '\n')

	def printGUIHeader(self, message):
		self.printLimited('-' * (self.maxX - 24), self.maxX)
		self.printLimited('%s %s' % (message, self._getHeader(self.maxX - len(message) - 1)), self.maxX)
		self.printLimited('-' * (self.maxX - 24), self.maxX)

	def display(self):
		(catStateDict, catDescDict, catSubcatDict) = self._getCategoryStateSummary()
		self._catCur = len(catStateDict)
		def sumCat(catKey, states):
			return sum(imap(lambda z: catStateDict[catKey].get(z, 0), states))

		self.printGUIHeader('Status report for task:')
		for catKey in catStateDict: # sorted(catStateDict, key = lambda x: -self._categories[x][0]):
			desc = self._formatDesc(catDescDict[catKey], catSubcatDict.get(catKey, 0))
			completed = sumCat(catKey, [Job.SUCCESS])
			total = sum(catStateDict[catKey].values())
			self.printLimited(Console.fmt(desc, [Console.BOLD]), self.maxX - 24,
				'(%5d jobs, %6.2f%%  )' % (total, 100 * completed / float(total)))
			progressbar = JobProgressBar(sum(catStateDict[catKey].values()), width = max(0, self.maxX - 19))
			progressbar.update(completed,
				sumCat(catKey, [Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED]),
				sumCat(catKey, [Job.RUNNING, Job.DONE]),
				sumCat(catKey, [Job.ABORTED, Job.CANCELLED, Job.FAILED]))
			self.printLimited(progressbar, self.maxX)
		for dummy in irange(self._catMax - len(catStateDict)):
			sys.stdout.write(' ' * self.maxX + '\n')
			sys.stdout.write(' ' * self.maxX + '\n')
