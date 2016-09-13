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
from python_compat import ifilter, imap

class TimeReport(Report):
	alias = ['time']

	def __init__(self, jobDB, task, jobs = None, configString = ''):
		Report.__init__(self, jobDB, task, jobs, configString)
		self._dollar_per_hour = float(configString or 0.013)

	def getHeight(self):
		return 1

	def display(self, job_db):
		job_runtimes = imap(lambda jobNum: job_db.getJobTransient(jobNum).get('runtime', 0), self._jobs)
		cpuTime = sum(ifilter(lambda rt: rt > 0, job_runtimes))
		msg = 'Consumed wall time: %-20s' % strTime(cpuTime)
		msg += 'Estimated cost: $%.2f\n' % ((cpuTime / 60. / 60.) * self._dollar_per_hour)
		sys.stdout.write(msg)
		sys.stdout.flush()
