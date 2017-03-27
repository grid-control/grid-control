# | Copyright 2007-2017 Karlsruhe Institute of Technology
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


HPF_STARTUP_DIRECTORY = os.getcwd()


class NestedException(Exception):
	# nested exception base class
	def __init__(self, *args, **kwargs):
		self.nested = []
		self.traceback = []
		cur_exception = get_current_exception()
		if cur_exception:
			self.nested = [cur_exception]
			self.traceback = _parse_traceback(_get_current_traceback())
		Exception.__init__(self, *args, **kwargs)


class APIError(NestedException):
	# some error in using the API
	pass


class AbstractError(APIError):
	# some error related to abstract functions
	def __init__(self):
		fun_name = impl_detail(sys, '_getframe', args=(2,),
			fun=lambda x: x.f_code.co_name, default='The invoked method')
		APIError.__init__(self, '%s is an abstract function!' % fun_name)


def clear_current_exception():
	# automatic cleanup in >= py-3.0
	impl_detail(sys, 'exc_clear', args=(), default=None)


def except_nested(ex_cls, ex_value):
	if isinstance(ex_value, ex_cls):
		return True
	if hasattr(ex_value, 'nested'):
		for ex_nested in ex_value.nested:
			if except_nested(ex_cls, ex_nested):
				return True


def get_current_exception():
	return sys.exc_info()[1]


def ignore_exception(exception_cls, exception_default, fun, *args, **kwargs):
	try:
		return fun(*args, **kwargs)
	except exception_cls:
		clear_current_exception()
		return exception_default


def impl_detail(module, name, args, default, fun=lambda x: x):
	# access some python implementation detail with default
	try:
		return fun(getattr(module, name)(*args))
	except Exception:
		return default


def parse_frame(frame):
	def _get_frame_dict(cur_frame):  # Parse single stack frame
		return {'idx': 0,
			'file': os.path.abspath(cur_frame.f_code.co_filename),
			'line': cur_frame.f_lineno,
			'fun': cur_frame.f_code.co_name,
			'locals': dict(cur_frame.f_locals)}
	return _parse_helper(_get_frame_dict, frame)  # use _parse_helper for correct path resolution


def rethrow(ex_value, fun, *args, **kwargs):
	try:
		return fun(*args, **kwargs)
	except Exception:
		if hasattr(ex_value, 'nested'):
			ex_value.nested.append(ExceptionWrapper(get_current_exception(), _get_current_traceback()))
		raise ex_value


class ExceptionCollector(object):
	# Utility class to collect multiple exceptions and throw them at a later time
	def __init__(self, log=None):
		(self._exception_list, self._log) = ([], log)

	def collect(self, *args, **kwargs):
		if self._log and args:
			self._log.log(*args, **kwargs)
		ex_helper = ExceptionWrapper(get_current_exception(), _get_current_traceback())
		self._exception_list.append(ex_helper)
		clear_current_exception()

	def raise_any(self, value):
		if not self._exception_list:
			return
		if hasattr(value, 'nested'):
			value.nested.extend(self._exception_list)
		self._exception_list = []
		raise value


class ExceptionWrapper(object):
	def __init__(self, exception_value, exception_traceback):
		self.nested = [exception_value]
		self.traceback = _parse_traceback(exception_traceback)


def _get_current_traceback():
	return sys.exc_info()[2]


def _parse_helper(fun, *args):
	cwd = os.getcwd()
	os.chdir(HPF_STARTUP_DIRECTORY)
	result = fun(*args)
	os.chdir(cwd)
	return result


def _parse_traceback(traceback):
	def _get_traceback_dict(traceback):  # Parse traceback information
		return {'idx': len(result) + 1,
			'file': os.path.abspath(traceback.tb_frame.f_code.co_filename),
			'line': traceback.tb_lineno,  # tb_lineno shows the location of the exception cause
			'fun': traceback.tb_frame.f_code.co_name,
			'locals': dict(traceback.tb_frame.f_locals)}
	result = []
	while traceback:
		result.append(_parse_helper(_get_traceback_dict, traceback))
		traceback = traceback.tb_next
	return result
