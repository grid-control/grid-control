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

import sys, logging
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.report import Report
from grid_control.utils.activity import Activity
from grid_control.utils.parsing import strTimeShort
from hpfwk import AbstractError

class GUI(ConfigurablePlugin):
	def __init__(self, config, workflow):
		ConfigurablePlugin.__init__(self, config)
		self._workflow = workflow
		self._reportOpts = config.get('report options', '', onChange = None)
		self._report = config.getCompositePlugin('report', 'BasicReport', 'MultiReport',
			cls = Report, onChange = None, pargs = (workflow.jobManager.jobDB,
			workflow.task), pkwargs = {'configString': self._reportOpts})

	def displayWorkflow(self):
		raise AbstractError()


class SimpleActivityStream(object):
	def __init__(self, stream, register_callback = False):
		(self._stream, self._old_message, self._register_cb) = (stream, None, register_callback)
		if self._register_cb:
			Activity.callbacks.append(self.write)

	def flush(self):
		return self._stream.flush()

	def isatty(self):
		return self._stream.isatty()

	def finish(self):
		if self._register_cb:
			Activity.callbacks.remove(self.write)
			self.write('\n')
		return self._stream

	def write(self, value = ''):
		activity_message = None
		if Activity.root:
			for activity in Activity.root.get_children():
				activity_message = activity.getMessage() + '...'
				if len(activity_message) > 75:
					activity_message = activity_message[:37] + '...' + activity_message[-35:]
		if self._old_message is not None:
			self._stream.write('\r%s\r' % (' ' * len(self._old_message)))
		self._old_message = activity_message
		result = self._stream.write(value)
		if (activity_message is not None) and (value.endswith('\n') or not value):
			self._stream.write(activity_message)
			self._stream.flush()
		return result


class SimpleConsole(GUI):
	def __init__(self, config, workflow):
		GUI.__init__(self, config, workflow)
		self._user_log = logging.getLogger('user')

	def displayWorkflow(self):
		self._user_log.info('')
		self._report.display()
		self._user_log.info('')
		if self._workflow.duration < 0:
			self._user_log.info('Running in continuous mode. Press ^C to exit.')
		elif self._workflow.duration > 0:
			self._user_log.info('Running for %s', strTimeShort(self._workflow.duration))
		if not sys.stdout.isatty():
			return self._workflow.jobCycle()
		sys.stdout = SimpleActivityStream(sys.stdout, register_callback = True)
		sys.stderr = SimpleActivityStream(sys.stderr)
		try:
			return self._workflow.jobCycle()
		finally:
			(sys.stdout, sys.stderr) = (sys.stdout.finish(), sys.stderr.finish())
