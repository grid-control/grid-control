# | Copyright 2016 Karlsruhe Institute of Technology
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

import math, time
from grid_control.job_db import Job
from grid_control.report import Report
from grid_control.utils import printTabular
from grid_control.utils.parsing import parseStr, strTimeShort
from python_compat import imap, itemgetter, lmap, lzip, sorted

class BackendReport(Report):
	alias = ['backend']

	def __init__(self, jobDB, task, jobs = None, configString = ''):
		Report.__init__(self, jobDB, task, jobs, configString)
		self._levelMap = {'wms': 2, 'endpoint': 3, 'site': 4, 'queue': 5}
		self._useHistory = ('history' in configString)
		configString = configString.replace('history', '')
		self._idxList = lmap(lambda x: self._levelMap[x.lower()], configString.split())
		self._idxList.reverse()
		self._stateMap = [(None, 'WAITING'), (Job.RUNNING, 'RUNNING'),
			(Job.FAILED, 'FAILED'), (Job.SUCCESS, 'SUCCESS')]

	def _getReportInfos(self):
		result = []
		defaultJob = Job()
		t_now = time.time()
		for jobNum in self._jobs:
			jobObj = self._jobDB.get(jobNum, defaultJob)
			runtime = parseStr(jobObj.get('runtime'), int, 0)
			for attempt in jobObj.history:
				if (attempt != jobObj.attempt) and not self._useHistory:
					continue
				if (attempt == jobObj.attempt) and (jobObj.state == Job.SUCCESS):
					time_info = runtime
				elif (attempt == jobObj.attempt - 1) and (jobObj.state != Job.SUCCESS):
					time_info = runtime
				elif attempt == jobObj.attempt:
					time_info = t_now - float(jobObj.submitted)
				else:
					time_info = 0
				state = jobObj.state
				if attempt != jobObj.attempt:
					state = Job.FAILED
				dest = jobObj.history[attempt]
				if dest == 'N/A':
					dest_info = [dest]
				else:
					dest_info = dest.split('/')
				wmsName = jobObj.wmsId.split('.')[1]
				endpoint = 'N/A'
				if 'http:' in jobObj.wmsId:
					endpoint = jobObj.wmsId.split(':')[1].split('/')[0]
				result.append([state, time_info, wmsName, endpoint] + dest_info)
		return result

	def _getHierachicalStats(self):
		overview = self._getReportInfos()
		def fillDict(result, items, idx_list = None, indent = 0):
			if not idx_list:
				for entry in items:
					result.setdefault(entry[0], []).append(entry[1])
				return result
			def getClassKey(entry):
				idx = idx_list[0]
				if idx < len(entry):
					return entry[idx]
				return 'N/A'
			classMap = {}
			for entry in items:
				classMap.setdefault(getClassKey(entry), []).append(entry)
			tmp = {}
			for classKey in classMap:
				childInfo = fillDict(result.setdefault(classKey, {}), classMap[classKey], idx_list[1:], indent + 1)
				for key in childInfo:
					tmp.setdefault(key, []).extend(childInfo[key])
			result[None] = tmp
			return tmp
		displayDict = {}
		fillDict(displayDict, overview, self._idxList)
		return displayDict

	def _get_entry_stats(self, stateMap, data):
		(tmp_m0, tmp_m1, tmp_m2, tmp_ma, tmp_mi) = ({}, {}, {}, {}, {})
		for rawState in data:
			state = stateMap.get(rawState, stateMap.get(None))
			tmp_m0[state] = tmp_m0.get(state, 0) + len(data[rawState])
			tmp_m1[state] = tmp_m1.get(state, 0) + sum(data[rawState])
			tmp_m2[state] = tmp_m2.get(state, 0) + sum(imap(lambda x: x * x, data[rawState]))
			tmp_ma[state] = max(data[rawState] + [tmp_ma.get(state, 0)])
			tmp_mi[state] = min(data[rawState] + [tmp_ma.get(state, 1e10)])
		for state in tmp_m0:
			mean = tmp_m1[state] / tmp_m0[state]
			stddev = math.sqrt(tmp_m2[state] / tmp_m0[state] - mean * mean)
			yield (state, tmp_m0[state], mean, stddev, tmp_mi[state], tmp_ma[state])

	def _get_entry(self, stateMap, data, label):
		(result_l1, result_l2, result_l3) = ({}, {}, {})
		if len(label) > 0:
			result_l1[''] = [label[0]]
		if len(label) > 1:
			result_l2[''] = ['  %s' % label[1]]
		if len(label) > 2:
			result_l3[''] = ['    %s' % label[2]] + label[3:]
		for (state, cnt, time_mean, time_stddev, time_min, time_max) in self._get_entry_stats(stateMap, data):
			result_l1[state] = cnt
			result_l2[state] = '%s +/- %s' % (strTimeShort(time_mean), strTimeShort(time_stddev))
			result_l3[state] = '%s ... %s' % (strTimeShort(time_min), strTimeShort(time_max))
		yield result_l1
		yield result_l2
		yield result_l3

	def display(self):
		stateMap = dict(self._stateMap)

		def transform(data, label, level):
			if None in data:
				total = data.pop(None)
				if (len(data) > 1):
					for result in self._get_entry(stateMap, total, ['Total']):
						yield result
					yield '='
			for idx, entry in enumerate(sorted(data)):
				if level == 1:
					for result in self._get_entry(stateMap, data[entry], [entry] + label):
						yield result
				else:
					for result in transform(data[entry], [entry] + label, level - 1):
						yield result
				if idx != len(data) - 1:
					yield '-'
		stats = self._getHierachicalStats()
		displayStates = lmap(itemgetter(1), self._stateMap)
		header = [('', 'Category')] + lzip(displayStates, displayStates)
		printTabular(header, transform(stats, [], len(self._idxList)),
			fmtString = 'l' + 'c'*len(stateMap), fmt = {'': lambda x: str.join(' ', x)})
		return 0
