#-#  Copyright 2014 Karlsruhe Institute of Technology
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

import sys
from python_compat import set, sorted
from grid_control import Job, utils
from grid_control.report import CategoryReport
from ansi import Console

class JobProgressBar:
	def __init__(self, total = 100, width = 16, jobsOnFinish = False):
		(self._total, self._width, self._jobsOnFinish) = (total, width, jobsOnFinish)
		self.update(0, 0)

	def update(self, success = 0, processing = 0, failed = 0):
		# Compute variables
		complete = self._width - 26
		blocks_ok = int(round(success / float(self._total) * complete))
		blocks_proc = int(round(processing / float(self._total) * complete))
		blocks_fail = min(complete - blocks_ok - blocks_proc, int(round(failed / float(self._total) * complete)))
		self._bar = str(Console.fmt('=' * blocks_ok, [Console.COLOR_GREEN]))
		self._bar += str(Console.fmt('=' * blocks_proc, [Console.COLOR_WHITE]))
		self._bar += ' ' * (complete - blocks_ok - blocks_proc - blocks_fail)
		self._bar += str(Console.fmt('=' * blocks_fail, [Console.COLOR_RED]))
		self._bar = '[%s] ' % self._bar
		if success == self._total and self._jobsOnFinish:
			self._bar += '(%s |%s)' % (Console.fmt('%5d' % self._total, [Console.COLOR_GREEN]),
				Console.fmt('finished'.center(14), [Console.COLOR_GREEN]))
		elif success == self._total:
			self._bar += '(%s)' % (Console.fmt('finished'.center(21), [Console.COLOR_GREEN]))
		else:
			fmt = lambda x: str(x).rjust(5)#int(math.log(self._total) / math.log(10)) + 1)
			self._bar += '(%s | %s | %s)' % (Console.fmt(fmt(success), [Console.COLOR_GREEN]),
				Console.fmt(fmt(processing), [Console.COLOR_WHITE]),
				Console.fmt(fmt(failed), [Console.COLOR_RED]))

	def __len__(self):
		return self._width

	def __str__(self):
		return str(self._bar)


class ModuleReport(CategoryReport):
	def display(self):
		(catStateDict, catDescDict, catSubcatDict) = CategoryReport._getCategoryStateSummary(self)

		infos = []
		head = set()
		stateCat = {Job.SUCCESS: 'SUCCESS', Job.FAILED: 'FAILED', Job.RUNNING: 'RUNNING', Job.DONE: 'RUNNING'}
		for catKey in catDescDict:
			tmp = catDescDict[catKey]
			infos.append(tmp)
			head.update(tmp.keys())
			for stateKey in catStateDict[catKey]:
				state = stateCat.get(stateKey, 'WAITING')
				tmp[state] = tmp.get(state, 0) + catStateDict[catKey][stateKey]

		stateCatList = ['WAITING', 'RUNNING', 'FAILED', 'SUCCESS']
		utils.vprint(level = -1)
		utils.printTabular(map(lambda x: (x, x), sorted(head) + stateCatList),
			infos, 'c' * len(head), fmt = dict.fromkeys(stateCatList, lambda x: '%7d' % utils.parseInt(x, 0)))
		utils.vprint(level = -1)


class AdaptiveReport(CategoryReport):
	def __init__(self, jobDB, task, jobs = None, configString = ''):
		CategoryReport.__init__(self, jobDB, task, jobs, configString)
		self._catMax = int(configString)

	def _getCategoryStateSummary(self):
		(catStateDict, catDescDict, catSubcatDict) = CategoryReport._getCategoryStateSummary(self)
		# Used for quick calculations
		catLenDict = {}
		for catKey in catStateDict:
			catLenDict[catKey] = sum(catStateDict[catKey].values())

		# Function to merge categories together
		def mergeCats(newCatDesc, catKeyList):
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

		# Merge successfully completed categories
		successKey = mergeCats('Completed subtasks', filter(lambda catKey:
			(len(catStateDict[catKey]) == 1) and (Job.SUCCESS in catStateDict[catKey]), catStateDict))

		# Next merge steps shouldn't see non-dict catKeys in catDescDict
		hiddenDesc = {}
		for catKey in filter(lambda catKey: not isinstance(catDescDict[catKey], dict), catDescDict):
			hiddenDesc[catKey] = catDescDict.pop(catKey)

		# Merge parameters to reach category goal - NP hard problem, so be greedy and quick!
		def eqDict(a, b, k):
			a = dict(a)
			b = dict(b)
			a.pop(k)
			b.pop(k)
			return a == b

		# Get dictionary with categories that will get merged when removing a variable
		def getKeyMergeResults():
			varKeyResult = {}
			catKeySearchDict = {}
			for catKey in catDescDict:
				for varKey in catDescDict[catKey]:
					if varKey not in catKeySearchDict:
						catKeySearch = set(catDescDict.keys())
					else:
						catKeySearch = catKeySearchDict[varKey]
					if catKeySearch:
						matches = filter(lambda ck: eqDict(catDescDict[catKey], catDescDict[ck], varKey), catKeySearch)
						if matches:
							catKeySearchDict[varKey] = catKeySearch.difference(set(matches))
							varKeyResult.setdefault(varKey, []).append(matches)
			return varKeyResult

		# Merge categories till 
		while True:
			deltaGoal = max(0, (len(catDescDict) + len(hiddenDesc)) - self._catMax)
			varKeyResult = getKeyMergeResults()
			(varKeyMerge, varKeyMergeDelta) = (None, 0)
			for varKey in varKeyResult:
				delta = sum(map(len, varKeyResult[varKey])) - len(varKeyResult[varKey])
				if (delta <= deltaGoal) and (delta > varKeyMergeDelta):
					(varKeyMerge, varKeyMergeDelta) = (varKey, delta)
			if varKeyMerge:
				for mergeList in varKeyResult[varKeyMerge]:
					varKeyMergeDesc = dict(catDescDict[mergeList[0]])
					varKeyMergeDesc.pop(varKeyMerge)
					mergeCats(varKeyMergeDesc, mergeList)
			else:
				break

		# Remove redundant variables from description
		varKeyResult = getKeyMergeResults()
		for varKey in sorted(varKeyResult, cmp = lambda a, b: -cmp(a, b)):
			if varKey == 'DATASETNICK': # Never remove dataset infos
				continue
			if sum(map(len, varKeyResult[varKey])) - len(varKeyResult[varKey]) == 0:
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

		# Restore hidden descriptions
		catDescDict.update(hiddenDesc)

		# Enforce category maximum - merge categories with the least amount of jobs
		if len(catStateDict) != self._catMax:
			mergeCats('Remaining subtasks', sorted(catStateDict,
				key = lambda catKey: -catLenDict[catKey])[self._catMax - 1:])

		# Finalize descriptions:
		if len(catDescDict) == 1:
			catDescDict[catDescDict.keys()[0]] = 'All jobs'

		return (catStateDict, catDescDict, catSubcatDict)


class GUIReport(AdaptiveReport):
	def __init__(self, jobDB, task, jobs = None, configString = ''):
		(self.maxY, self.maxX) = Console().getmaxyx()
		self.maxX -= 10 # Padding
		AdaptiveReport.__init__(self, jobDB, task, jobs, str(int(self.maxY / 5)))

	def getHeight(self):
		return self._catMax * 3 + 1

	def printLimited(self, value, width, rvalue = ''):
		if len(value) + len(rvalue) > width:
			value = str(value)[:width - 3 - len(rvalue)] + '...'
		sys.stdout.write(str(value) + ' ' * (width - (len(value) + len(rvalue))) + str(rvalue) + '\n')

	def printGUIHeader(self, message):
		self.printLimited('-' * (self.maxX - 24), self.maxX)
		self.printLimited('%s %s' % (message, self._getHeader(self.maxX - len(message) - 1)), self.maxX)
		self.printLimited('-' * (self.maxX - 24), self.maxX)

	def display(self):
		self.printGUIHeader('Status report for task:')
		(catStateDict, catDescDict, catSubcatDict) = self._getCategoryStateSummary()
		sumCat = lambda catKey, states: sum(map(lambda z: catStateDict[catKey].get(z, 0), states))

		for catKey in catStateDict: #sorted(catStateDict, key = lambda x: -self._categories[x][0]):
			desc = self._formatDesc(catDescDict[catKey], catSubcatDict.get(catKey, 0))
			completed = sumCat(catKey, [Job.SUCCESS])
			total = sum(catStateDict[catKey].values())
			self.printLimited(Console.fmt(desc, [Console.BOLD]), self.maxX,
				'(%5d jobs, %6.2f%%  )' % (total, 100 * completed / float(total)))
			bar = JobProgressBar(sum(catStateDict[catKey].values()), width = self.maxX)
			bar.update(completed,
				sumCat(catKey, [Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING, Job.DONE]),
				sumCat(catKey, [Job.ABORTED, Job.CANCELLED, Job.FAILED]))
			self.printLimited(bar, self.maxX)
