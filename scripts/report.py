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

import sys, optparse
from gcSupport import JobSelector, Plugin, getConfig, parseOptions, utils

Report = Plugin.getClass('Report')

parser = optparse.OptionParser()
parser.add_option('', '--report', dest='reportClass', default='GUIReport')
parser.add_option('', '--report-list', dest='reportList', default=False, action='store_true',
	help='List available report classes')
parser.add_option('-J', '--job-selector', dest='selector', default=None)
parser.add_option('-T', '--use-task', dest='useTask', default=False, action='store_true',
	help='Forward task information to report')
parser.add_option('', '--str', dest='string', default=None)
(opts, args) = parseOptions(parser)

if opts.reportList:
	print(Report.getClassList())

if len(args) != 1:
	utils.exitWithUsage('%s [options] <config file>' % sys.argv[0])

def main():
	# try to open config file
	config = getConfig(args[0], section = 'global')

	# Initialise task module
	task = None
	if opts.useTask:
		task = config.getPlugin(['task', 'module'], cls = 'TaskModule')

	# Initialise job database
	jobDB = config.getPlugin('jobdb', 'JobDB', cls = 'JobDB', pargs = (config,))
	log = utils.ActivityLog('Filtering job entries')
	selected = jobDB.getJobs(JobSelector.create(opts.selector, task = task))
	del log

	report = Report.createInstance(opts.reportClass, jobDB, task, selected, opts.string)
	report.display()

if __name__ == '__main__':
	sys.exit(main())
