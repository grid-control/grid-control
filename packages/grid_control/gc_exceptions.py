# | Copyright 2015-2017 Karlsruhe Institute of Technology
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
from grid_control.utils.data_structures import make_enum
from hpfwk import NestedException, clear_current_exception, ignore_exception
from python_compat import imap, izip


INITIAL_EXCEPTHOOK = sys.excepthook


class GCError(NestedException):
	pass  # grid-control exception base class


class InstallationError(GCError):
	pass  # some error with installed programs


class UserError(GCError):
	pass  # some error caused by the user


def gc_excepthook(*exc_info):
	# Exception handler for interactive mode:
	if hasattr(gc_excepthook, 'restore_old_hook') and getattr(gc_excepthook, 'restore_old_hook'):
		sys.excepthook = INITIAL_EXCEPTHOOK
	version = ignore_exception(Exception, 'unknown version',
		lambda: sys.modules['grid_control'].__version__)
	log = logging.getLogger('abort')
	if not log.handlers and not (log.propagate and logging.getLogger().handlers):
		log.addHandler(logging.StreamHandler(sys.stderr))
	log.handle(log.makeRecord('exception', logging.CRITICAL, __file__, None,
		'Exception occured in grid-control [%s]\n\n' % version, tuple(), exc_info))
sys.excepthook = gc_excepthook  # <alias>


class GCLogHandler(logging.FileHandler):
	config_instances = []

	# This handler stores several pieces of debug information in a file
	def __init__(self, fn_candidates, mode='a', *args, **kwargs):
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
					fp.write('-' * 70 + '\nConfig instance %d\n' % idx + '=' * 70 + '\n')
					instance.write(fp)
			except Exception:
				fp.write('-> unable to display configuration!\n')
				clear_current_exception()
		finally:
			if GCLogHandler.config_instances:
				fp.write('\n' + '*' * 70 + '\n')
		if make_enum.enum_list:
			fp.write('\nList of enums\n')
			for enum in make_enum.enum_list:
				fp.write('\t%s\n' % str.join('|', imap(lambda name_value: '%s:%s' % name_value,
					izip(enum.enum_name_list, enum.enum_value_list))))
			fp.write('\n' + '*' * 70 + '\n')
		fp.write('\n')
		fp.close()
		logging.FileHandler.emit(self, record)
		sys.stderr.write('\nIn case this is caused by a bug, please send the log file:\n' +
			'\t%r\n' % self._fn + 'to grid-control-dev@googlegroups.com\n')
