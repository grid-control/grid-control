#!/usr/bin/env python
#-#  Copyright 2009-2016 Karlsruhe Institute of Technology
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
from gcSupport import JobSelector, Options, Plugin, getConfig, utils

parser = Options()
parser.addText(None, 'report',       default = 'GUIReport', short = '-R')
parser.addFlag(None, 'report-list',  default = False,       short = '-L',
	help = 'List available report classes')
parser.addText(None, 'job-selector', default = None,        short = '-J')
parser.addFlag(None, 'use-task',     default = False,       short = '-T',
	help='Forward task information to report')
parser.addText(None, 'string',       default = None)
(opts, args) = parser.parse()

Report = Plugin.getClass('Report')

if opts.report_list:
	msg = 'Available report classes:\n'
	for entry in Report.getClassList():
		msg += ' * %s\n' % str.join(' ', entry.values())
	print(msg)

if len(args) != 1:
	utils.exitWithUsage('%s [options] <config file>' % sys.argv[0])

def main():
	# try to open config file
	config = getConfig(args[0], section = 'global')

	# Initialise task module
	task = None
	if opts.use_task:
		task = config.getPlugin(['task', 'module'], cls = 'TaskModule')

	# Initialise job database
	jobDB = config.getPlugin('jobdb', 'JobDB', cls = 'JobDB')
	log = utils.ActivityLog('Filtering job entries')
	selected = jobDB.getJobs(JobSelector.create(opts.job_selector, task = task))
	del log

	report = Report.createInstance(opts.report, jobDB, task, selected, opts.string)
	report.display()

if __name__ == '__main__':
	sys.exit(main())
