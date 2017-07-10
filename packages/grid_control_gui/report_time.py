# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

from grid_control.report import ConsoleReport
from grid_control.utils.parsing import str_time_long
from python_compat import ifilter, imap


class TimeReport(ConsoleReport):
	alias_list = ['time']

	def __init__(self, config, name, job_db, task=None):
		ConsoleReport.__init__(self, config, name, job_db, task)
		self._dollar_per_hour = config.get_float('dollar per hour', 0.013, on_change=None)

	def show_report(self, job_db, jobnum_list):
		jr_iter = imap(lambda jobnum: job_db.get_job_transient(jobnum).get('runtime', 0), jobnum_list)
		cpu_time = sum(ifilter(lambda rt: rt > 0, jr_iter))
		msg1 = 'Consumed wall time: %-20s' % str_time_long(cpu_time)
		msg2 = 'Estimated cost: $%.2f' % ((cpu_time / 60. / 60.) * self._dollar_per_hour)
		self._show_line(msg1 + msg2.rjust(65 - len(msg1)))
