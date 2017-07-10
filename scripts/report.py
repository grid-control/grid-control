#!/usr/bin/env python
# | Copyright 2009-2017 Karlsruhe Institute of Technology
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
from gc_scripts import ScriptOptions, display_plugin_list_for, get_script_object


def _main():
	parser = ScriptOptions(usage='%s [OPTIONS] <config file>')
	parser.add_bool(None, 'L', 'report-list', default=False,
		help='List available report classes')
	parser.add_bool(None, 'T', 'use-task', default=False,
		help='Forward task information to report')
	parser.add_text(None, 'R', 'report', default='modern')
	parser.add_text(None, 'J', 'job-selector', default=None)
	options = parser.script_parse()

	if options.opts.report_list:
		display_plugin_list_for('Report', title='Available report classes')

	if len(options.args) != 1:
		parser.exit_with_usage()

	script_obj = get_script_object(config_file=options.args[0],
		job_selector_str=options.opts.job_selector, require_task=options.opts.use_task)
	report = script_obj.new_config.get_composited_plugin('transient report',
		options.opts.report, 'MultiReport', cls='Report',
		pargs=(script_obj.job_db, script_obj.task))
	report.show_report(script_obj.job_db, script_obj.job_db.get_job_list())


if __name__ == '__main__':
	sys.exit(_main())
