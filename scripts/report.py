#!/usr/bin/env python
# | Copyright 2009-2016 Karlsruhe Institute of Technology
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
from gcSupport import Activity, JobSelector, Options, Plugin, displayPluginList, getConfig, getPluginList, scriptOptions, utils

parser = Options(usage = '%s [OPTIONS] <config file>')
parser.addBool(None, 'L', 'report-list',  default = False, help = 'List available report classes')
parser.addBool(None, 'T', 'use-task',     default = False, help = 'Forward task information to report')
parser.addText(None, 'R', 'report',       default = 'GUIReport')
parser.addText(None, 'J', 'job-selector', default = None)
parser.addText(None, ' ', 'string',       default = '')
options = scriptOptions(parser)

if options.opts.report_list:
	sys.stderr.write('Available report classes:\n')
	displayPluginList(getPluginList('Report'))

if len(options.args) != 1:
	utils.exitWithUsage(parser.usage())

def main(opts, args):
	# try to open config file
	config = getConfig(args[0], section = 'global')

	# Initialise task module
	task = None
	if opts.use_task:
		task = config.getPlugin('workflow', 'Workflow:global', cls = 'Workflow', pargs = ('task',)).task

	# Initialise job database
	jobDB = config.getPlugin('job database', 'TextFileJobDB', cls = 'JobDB')
	activity = Activity('Filtering job entries')
	selected = jobDB.getJobs(JobSelector.create(opts.job_selector, task = task))
	activity.finish()

	report = config.getCompositePlugin('transient report', opts.report, 'MultiReport', cls = 'Report',
		pargs = (jobDB, task, selected, opts.string))
	report.display(jobDB)

if __name__ == '__main__':
	sys.exit(main(options.opts, options.args))
