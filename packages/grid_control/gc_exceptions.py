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
from hpfwk import NestedException

class GCLogHandler(logging.FileHandler):
	def __init__(self, fn, mode = 'a', *args, **kwargs):
		self._mode = mode
		if not fn:
			found = False
			for fn in [os.path.join(os.environ['GC_PACKAGES_PATH'], '..', 'debug.log'), '/tmp/gc.debug.%d' % os.getuid(), '~/gc.debug']:
				try:
					fn = os.path.abspath(os.path.normpath(os.path.expanduser(fn)))
					logging.FileHandler.__init__(self, fn, 'a', *args, **kwargs)
					found = True
					break
				except Exception:
					continue
			if not found:
				raise NestedException('Unable to find writeable debug log path!')
		else:
			logging.FileHandler.__init__(self, fn, 'a', *args, **kwargs)
		self._fn = os.path.abspath(fn)

	def emit(self, record):
		try:
			fp = open(self._fn, self._mode)
			for idx, instance in enumerate(GCLogHandler.config_instances):
				fp.write('-' * 70 + '\n' + ('Config instance %d\n' % idx) + '=' * 70 + '\n')
				instance.write(fp)
			fp.close()
		except Exception:
			pass
		logging.FileHandler.emit(self, record)
		sys.stderr.write('In case this is caused by a bug, please send the log file:\n')
		sys.stderr.write('\t"%s"\nto grid-control-dev@googlegroups.com\n' % self._fn)
GCLogHandler.config_instances = []


# Exception handler for interactive mode:
def gc_excepthook(*exc_info):
	sys.excepthook = sys.__excepthook__
	try:
		version = __import__('grid_control').__version__
	except Exception:
		version = 'Unknown version'
	log = logging.getLogger('exception')
	if not log.handlers:
		log.addHandler(logging.StreamHandler(sys.stderr))
	log.handle(log.makeRecord('exception', logging.CRITICAL, __file__, None,
		'Exception occured in grid-control [%s]' % version, tuple(), exc_info))
sys.excepthook = gc_excepthook

class GCError(NestedException):
	pass	# grid-control exception base class

class UserError(GCError):
	pass	# some error caused by the user

class InstallationError(GCError):
	pass	# some error with installed programs
