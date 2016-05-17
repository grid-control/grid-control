# | Copyright 2012-2016 Karlsruhe Institute of Technology
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
from grid_control.report import Report
from grid_control.utils.parsing import strTime

class TimeReport(Report):
	alias = ['time']

	def __init__(self, jobDB, task, jobs = None, configString = ''):
		Report.__init__(self, jobDB, task, jobs, configString)
		self._dollar_per_hour = 0.013
		if configString:
			self._dollar_per_hour = float(configString)

	def getHeight(self):
		return 1

	def display(self):
		cpuTime = 0
		for jobNum in self._jobs:
			jobObj = self._jobDB.get(jobNum)
			if jobObj:
				cpuTime += jobObj.get('runtime', 0)
		msg = 'Consumed wall time: %-20s' % strTime(cpuTime)
		msg += 'Estimated cost: $%.2f\n' % ((cpuTime / 60. / 60.) * self._dollar_per_hour)
		sys.stdout.write(msg)
		sys.stdout.flush()
