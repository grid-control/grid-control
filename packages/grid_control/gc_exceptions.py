# | Copyright 2015-2016 Karlsruhe Institute of Technology
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

import os, sys, logging
from hpfwk import NestedException, clear_current_exception

(initial_stdout, initial_stderr, initial_excepthook) = (sys.stdout, sys.stderr, sys.excepthook)

class GCLogHandler(logging.FileHandler):
	def __init__(self, fn_candidates, mode = 'a', *args, **kwargs):
		(self._fn, self._mode) = (None, mode)
		for fn_candidate in fn_candidates:
			try:
				fn_candidate = os.path.abspath(os.path.normpath(os.path.expanduser(fn_candidate)))
				logging.FileHandler.__init__(self, fn_candidate, 'a', *args, **kwargs)
				self._fn = fn_candidate
				break
			except Exception:
				clear_current_exception()
		if self._fn is None:
			raise Exception('Unable to find writeable debug log path!')

	def emit(self, record):
		fp = open(self._fn, self._mode)
		try:
			try:
				for idx, instance in enumerate(GCLogHandler.config_instances):
					fp.write('-' * 70 + '\n' + ('Config instance %d\n' % idx) + '=' * 70 + '\n')
					instance.write(fp)
			except Exception:
				fp.write('-> unable to display configuration!\n')
				clear_current_exception()
		finally:
			if GCLogHandler.config_instances:
				fp.write('\n' + '*' * 70 + '\n\n')
			fp.close()
		logging.FileHandler.emit(self, record)
		msg = '\nIn case this is caused by a bug, please send the log file:\n\t%r\nto grid-control-dev@googlegroups.com\n' % self._fn
		sys.stderr.write(msg)
GCLogHandler.config_instances = []


# Exception handler for interactive mode:
def gc_excepthook(*exc_info):
	if gc_excepthook.restore:
		(sys.stdout, sys.stderr, sys.excepthook) = (initial_stdout, initial_stderr, initial_excepthook)
	try:
		version = __import__('grid_control').__version__
	except Exception:
		version = 'unknown version'
	log = logging.getLogger('abort')
	if not log.handlers and not (log.propagate and logging.getLogger().handlers):
		log.addHandler(logging.StreamHandler(sys.stderr))
	log.handle(log.makeRecord('exception', logging.CRITICAL, __file__, None,
		'Exception occured in grid-control [%s]\n\n' % version, tuple(), exc_info))
gc_excepthook.restore = True
sys.excepthook = gc_excepthook


class GCError(NestedException):
	pass	# grid-control exception base class


class UserError(GCError):
	pass	# some error caused by the user


class InstallationError(GCError):
	pass	# some error with installed programs
