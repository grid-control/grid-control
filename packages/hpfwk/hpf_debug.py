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

import os, sys, threading
from hpfwk.hpf_exceptions import ExceptionWrapper, ignore_exception, impl_detail, parse_frame


def format_exception(exc_info,
		show_code_context=0, show_variables=0, show_file_stack=0, show_exception_stack=1, show_threads=1):
	msg_parts = []
	if exc_info not in [None, (None, None, None)]:
		frame_dict_list, ex_info_list = _collect_exception_infos(*exc_info)

		# Code and variable listing
		if show_code_context > 0:
			msg_parts.extend(_format_stack(frame_dict_list,
				code_context=show_code_context - 1, truncate_var_repr=show_variables))

		# Show file stack for all threads
		thread_frame_dict = impl_detail(sys, '_current_frames', args=(), default={})
		if (len(thread_frame_dict) > 1) and (show_threads > 0):
			for (thread_id, thread_frame) in thread_frame_dict.items():
				msg_parts.extend(_format_file_stack(_parse_stack(thread_frame), ' for thread %s' % thread_id))

		# File stack with line information
		if show_file_stack > 0:
			msg_parts.extend(_format_file_stack(frame_dict_list))

		# Exception message tree
		if show_exception_stack > 0:
			msg_parts.append(_format_ex_tree(ex_info_list, show_exception_stack))

	return str.join('\n', msg_parts)


def get_thread_name(thread=None):
	if thread is None:
		thread = _get_current_thread()
	try:  # new lowercase name in >= py-2.6
		return thread.name
	except Exception:
		return thread.getName()


def get_trace_fun():
	return get_trace_fun.trace_fun
get_trace_fun.trace_fun = None  # <global-state>


def set_trace_fun(trace_fun=None):
	if set_trace_fun.enabled:
		sys.settrace(trace_fun)  # set trace function in local thread
		get_trace_fun.trace_fun = trace_fun  # set global trace function for all newly started threads
set_trace_fun.enabled = True  # <global-state>


class DebugInterface(object):
	callback_list = []

	def __init__(self, cur_frame=None, interrupt_fun=None, stream=sys.stderr):
		(self._frame, self._interrupt_fun, self._stream) = (cur_frame, interrupt_fun, stream)
		self._map_thread_id2frame = {}
		self._get_thread_id2_frame_map()

	def get_console(self, env_dict):
		import code
		console = code.InteractiveConsole(env_dict)
		console.push('try:')
		console.push('  import rlcompleter, readline')
		console.push('  readline.parse_and_bind("tab: complete")')
		console.push('  readline.set_completer(rlcompleter.Completer(globals()).complete)')
		console.push('  del rlcompleter')
		console.push('  del readline')
		console.push('except Exception:')
		console.push('  pass')
		console.push('')
		return console

	def get_console_env_dict(self, env_dict):
		env_dict = env_dict or {}
		env_dict.update({'stack': self.show_stack, 'locals': self._get_locals,
			'trace': self.set_trace, 'resume': self._resume, 'threads': self._get_thread_list})
		msg = '\nDEBUG MODE ENABLED!\n  available debug commands: %s\n' % str.join(', ', env_dict)
		msg += '  list of active threads [%d]:\n' % len(list(threading.enumerate()))
		msg += '  available thread ids: %s\n' % list(self._get_thread_id2_frame_map().keys())
		self._stream.write(msg)
		self._get_thread_list()
		return env_dict

	def set_trace(self, filename=None, lineno=None, fun_name=None, event=None,
			stop_on_match=False, start_on_match=False):
		set_trace_fun(_create_trace_fun(self._stream, self._interrupt_fun, filename, lineno,
			fun_name, event, stop_on_match, start_on_match))

	def show_stack(self, stack_depth=None, thread_id=None, show_vars=-1, show_code=0):
		def _show_frame_list(frame):
			if stack_depth in (None, 'all'):
				frame_dict_list = _parse_stack(frame)
			else:
				frame_dict_list = [parse_frame(self._get_frame(frame, stack_depth))]
			iter_lines = _format_stack(frame_dict_list, show_code, show_vars, tight=True)
			self._stream.write(str.join('\n', iter_lines) + '\n')
		if str(thread_id).lower() == 'all':
			for (thread_id, frame) in self._get_thread_id2_frame_map().items():
				thread_id_str = str(thread_id)
				for thread in threading.enumerate():
					if thread_id_str in repr(thread):
						thread_id_str += ' (%s)' % get_thread_name(thread)
				self._stream.write('\nStack for thread %s\n' % thread_id_str)
				_show_frame_list(frame)
		else:
			_show_frame_list(self._get_thread_id2_frame_map().get(thread_id, self._frame))

	def start_console(self, env_dict=None):
		for (callback_start, _) in DebugInterface.callback_list:
			callback_start()
		env_dict = self.get_console_env_dict(env_dict=env_dict)
		ignore_exception(SystemExit, None, self.get_console(env_dict).interact, '')
		for (_, callback_end) in DebugInterface.callback_list:
			callback_end()
		self._stream.write('Resuming ...\n')

	def _get_frame(self, cur_frame, stack_depth=-1):
		frame_list = []
		while cur_frame:
			frame_list.insert(0, cur_frame)
			cur_frame = cur_frame.f_back
		return frame_list[stack_depth]

	def _get_locals(self, stack_depth=-1, thread_id=None):
		""" return dictionary with local variables for selected frame """
		frame = self._get_thread_id2_frame_map().get(thread_id, self._frame)
		return self._get_frame(frame, stack_depth).f_locals

	def _get_thread_id2_frame_map(self):
		self._map_thread_id2frame.update(impl_detail(sys, '_current_frames', args=(), default={}))
		return self._map_thread_id2frame

	def _get_thread_list(self):
		thread_display_list = []
		for thread in threading.enumerate():
			thread_desc = repr(thread)
			if hasattr(thread, 'desc'):
				thread_desc += '\t%s' % getattr(thread, 'desc')
			thread_display_list.append('\t- %s\t%s\n' % (get_thread_name(thread), thread_desc))
		thread_display_list.sort()
		self._stream.write(str.join('', thread_display_list))

	def _resume(self, duration=None):
		if duration is not None:
			self._interrupt_fun(duration)
		raise SystemExit(os.EX_OK)


def _collect_exception_infos(exception_type, exception_value, exception_traceback):
	# Collect full traceback and exception context
	exception_start = ExceptionWrapper(exception_value, exception_traceback)
	exception_info_list = []

	def _collect_exception_infos_impl(exception, cur_depth=-1, exception_id='T'):
		if not isinstance(exception, ExceptionWrapper):
			cur_depth += 1
			exception_info_list.append((exception, cur_depth, exception_id))
		if hasattr(exception, 'traceback') and hasattr(exception, 'nested'):  # decent
			for frame in exception.traceback[:-1]:  # yield all frames except for the final raise statement
				frame['exception_id'] = exception_id
				yield frame
			for idx, exception_nested in enumerate(exception.nested):
				for frame in _collect_exception_infos_impl(exception_nested,
						cur_depth, exception_id + '|%02d' % idx):
					yield frame
			for frame in exception.traceback[-1:]:
				frame['exception_id'] = exception_id
				yield frame

	frame_dict_list = list(_collect_exception_infos_impl(exception_start))
	return (frame_dict_list, exception_info_list)  # skipping top-level exception helper


def _create_trace_fun(stream, interrupt_fun, filename=None, lineno=None, fun_name=None, event=None,
		stop_on_match=False, start_on_match=False):
	def _trace_fun(frame, event, arg):
		if _trace_fun.started:
			_output_trace(stream, frame, event, arg)
		if (filename is None) or (filename in frame.f_code.co_filename):
			if (lineno is None) or (lineno == frame.f_lineno):
				if (fun_name is None) or (fun_name == frame.f_code.co_name):
					if not _trace_fun.started:
						_output_trace(stream, frame, event, arg)
					if start_on_match:
						_trace_fun.started = True
					if stop_on_match and not _trace_fun.stopped:
						_trace_fun.stopped = True
						set_trace_fun(None)
						if interrupt_fun:
							interrupt_fun(duration=0)
						return
		return _trace_fun
	_trace_fun.started = False
	_trace_fun.stopped = False
	return _trace_fun


def _format_ex_tree(ex_info_list, show_exception_stack=2):
	ex_msg_list = []
	for info in ex_info_list:
		(exception_value, exception_depth, _) = info
		prefix = ''
		if show_exception_stack != 1:
			prefix = '  ' * exception_depth
		exception_type_name = exception_value.__class__.__name__
		ex_msg_list.append('%s%s: %s' % (prefix, exception_type_name, exception_value))
		if (show_exception_stack <= 1) or hasattr(exception_value, 'nested'):
			continue
		if hasattr(exception_value, 'args'):
			args = exception_value.args
			already_displayed = (len(args) == 1) and str(args[0]) not in str(exception_value)
			if already_displayed or (len(args) > 1):
				def _fmt_ex_args():
					return ['%s%s  %s' % (prefix, len(exception_type_name) * ' ', exception_value.args)]
				ex_msg_list.extend(ignore_exception(Exception, [], _fmt_ex_args))
	if show_exception_stack > 1:
		return str.join('\n', ex_msg_list)
	return str.join('\n' + '-' * 10 + '\n', ex_msg_list)


def _format_file_stack(frame_dict_list, title=''):
	yield 'File stack%s:' % title
	for frame_dict in frame_dict_list:
		frame_dict['_id'] = frame_dict.get('exception_id', '') + '|%02d' % frame_dict.get('idx', 0)
		yield '%(_id)s %(file)s %(line)s (%(fun)s)' % frame_dict
	yield ''


def _format_frame(frame, code_context, truncate_var_repr):
	import linecache
	linecache.checkcache()
	# Output relevant code fragment
	exception_id = ''
	if frame.get('exception_id') is not None:
		exception_id = '%s-' % frame['exception_id']
	yield 'Stack #%s%02d [%s:%d] %s' % (exception_id,
		frame['idx'], frame['file'], frame['line'], frame['fun'])

	def _get_source_code(line_num):
		return linecache.getline(frame['file'], line_num).rstrip().replace('\t', '  ')
	delta_line_num = -code_context
	while delta_line_num <= code_context:
		if delta_line_num == 0:
			yield '\t=>| %s' % _get_source_code(frame['line'] + delta_line_num)
		else:
			yield '\t  | %s' % _get_source_code(frame['line'] + delta_line_num)
		delta_line_num += 1
	if (truncate_var_repr != -1) and frame['locals']:
		yield ''
		for line in _format_variables(frame['locals'], truncate_var_repr):
			yield line


def _format_stack(frame_list, code_context, truncate_var_repr, tight=False):
	# Function to log source code and variables from frames
	for frame in frame_list:
		for line in _format_frame(frame, code_context, truncate_var_repr):
			yield line
		if not tight:
			yield ''


def _format_variables(variable_dict, truncate_var_repr):
	class_instance = variable_dict.pop('self', None)
	if variable_dict:
		yield '\tLocal variables:'
		for line in _format_variables_list(truncate_var_repr, variable_dict):
			yield line
	if class_instance is not None:
		yield '\tClass variables (%s):' % _safe_repr(class_instance, truncate_var_repr)
		if hasattr(class_instance, '__dict__'):
			class_variable_dict = class_instance.__dict__
		elif hasattr(class_instance, '__slots__'):
			class_variable_dict = {}
			for attr in class_instance.__slots__:
				class_variable_dict[attr] = getattr(class_instance, attr)
		try:
			for line in _format_variables_list(truncate_var_repr, class_variable_dict, 'self.'):
				yield line
		except Exception:
			yield '\t\t<unable to access class>'


def _format_variables_list(truncate_var_repr, var_dict, vn_prefix=''):
	# Function to log local and class variables
	max_vn_len = 0
	for vn in var_dict:
		max_vn_len = max(max_vn_len, len(vn))
	vn_list = list(var_dict)
	vn_list.sort()
	for vn in vn_list:
		repr_str = _safe_repr(var_dict[vn], truncate_var_repr)
		if 'password' in vn:
			repr_str = '<redacted>'
		yield '\t\t%s%s = %s' % (vn_prefix, vn.ljust(max_vn_len), repr_str)


def _get_current_thread():
	try:  # new lowercase name in >= py-2.6
		return threading.current_thread()
	except Exception:
		return threading.currentThread()


def _output_trace(stream, frame, event, arg):
	arg_str = ignore_exception(Exception, '<repr failed>', repr, arg)
	thread_name = ignore_exception(Exception, '<unknown thread>', get_thread_name)
	stream.write('%s %s:%s %s %s %s\n' % (thread_name, frame.f_code.co_filename,
		frame.f_lineno, frame.f_code.co_name, event, arg_str))


def _parse_stack(frame):
	result = []
	while frame:
		result.insert(0, parse_frame(frame))
		frame = frame.f_back
	for idx, frame_dict in enumerate(result):
		frame_dict['idx'] = idx
	return result


def _safe_repr(obj, truncate_var_repr):
	repr_str = ignore_exception(Exception, 'unable to display!', repr, obj)
	if (truncate_var_repr is None) or (len(repr_str) < truncate_var_repr):
		return repr_str
	return repr_str[:truncate_var_repr] + ' ... [length:%d]' % len(repr_str)
