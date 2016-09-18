# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

import os, sys

hpf_startup_directory = os.getcwd()

def clear_current_exception():
	return impl_detail(sys, 'exc_clear', args = (), default = None)


def get_current_exception():
	return sys.exc_info()[1]


def get_current_traceback():
	return sys.exc_info()[2]


def impl_detail(module, name, args, default, fun = lambda x: x):
	# access some python implementation detail with default
	try:
		return fun(getattr(module, name)(*args))
	except Exception:
		return default


def parse_frame(frame):
	# Parse stack frame
	def _parse_frame(result, frame):
		while frame:
			result.insert(0, {
				'file': os.path.abspath(frame.f_code.co_filename),
				'line': frame.f_lineno,
				'fun': frame.f_code.co_name,
				'locals': dict(frame.f_locals)})
			frame = frame.f_back
		for idx, entry in enumerate(result):
			entry['idx'] = idx
	return _parse_helper(_parse_frame, frame)


def parse_traceback(traceback):
	# Parse traceback information
	def _parse_traceback(result, traceback):
		while traceback:
			result.append({'idx': len(result) + 1,
				'file': os.path.abspath(traceback.tb_frame.f_code.co_filename),
				'line': traceback.tb_lineno, # tb_lineno shows the location of the exception cause
				'fun': traceback.tb_frame.f_code.co_name,
				'locals': dict(traceback.tb_frame.f_locals)})
			traceback = traceback.tb_next
	return _parse_helper(_parse_traceback, traceback)


def _parse_helper(fun, *args):
	result = []
	cwd = os.getcwd()
	os.chdir(hpf_startup_directory)
	fun(result, *args)
	os.chdir(cwd)
	return result


class ExceptionCollector(object):
	# Utility class to collect multiple exceptions and throw them at a later time
	def __init__(self, log = None):
		(self._exception_list, self._log) = ([], log)

	def collect(self, *args, **kwargs):
		if self._log and args:
			self._log.log(*args, **kwargs)
		self._exception_list.append(NestedExceptionHelper(get_current_exception(), get_current_traceback()))
		clear_current_exception()

	def raise_any(self, value):
		if not self._exception_list:
			return
		if hasattr(value, 'nested'):
			value.nested.extend(self._exception_list)
		self._exception_list = []
		raise value


class NestedExceptionHelper(object):
	def __init__(self, exception_value, exception_traceback):
		self.nested = [exception_value]
		self.traceback = parse_traceback(exception_traceback)


class NestedException(Exception):
	# nested exception base class
	def __init__(self, *args, **kwargs):
		self.nested = []
		self.traceback = []
		cur_exception = get_current_exception()
		if cur_exception:
			self.nested = [cur_exception]
			self.traceback = parse_traceback(get_current_traceback())
		Exception.__init__(self, *args, **kwargs)


class APIError(NestedException):
	# some error in using the API
	pass


class AbstractError(APIError):
	# some error related to abstract functions
	def __init__(self):
		fun_name = impl_detail(sys, '_getframe', args = (2,), fun = lambda x: x.f_code.co_name, default = 'The invoked method')
		APIError.__init__(self, '%s is an abstract function!' % fun_name)
