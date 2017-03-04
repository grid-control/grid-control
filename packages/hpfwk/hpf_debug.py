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

import sys, logging, threading
from hpfwk.hpf_exceptions import ExceptionWrapper, clear_current_exception, impl_detail, parse_frame


def create_debug_console(variables):
	import code
	console = code.InteractiveConsole(variables)
	console.push('import rlcompleter, readline')
	console.push('readline.parse_and_bind("tab: complete")')
	console.push('readline.set_completer(rlcompleter.Completer(globals()).complete)')
	return console


def format_exception(exc_info,
		show_code_context=0, show_variables=0, show_file_stack=0, show_exception_stack=1):
	msg_parts = []

	if exc_info not in [None, (None, None, None)]:
		frame_info_list, ex_info_list = _collect_exception_infos(*exc_info)

		# Code and variable listing
		if show_code_context > 0:
			msg_parts.append(str.join('\n', _format_stack(frame_info_list,
				code_context=show_code_context - 1, truncate_var_repr=show_variables)))

		# File stack with line information
		if show_file_stack > 0:
			msg_fstack = 'File stack:\n'
			for frame_info in frame_info_list:
				frame_info['_id'] = frame_info.get('exception_id', '') + '|%02d' % frame_info.get('idx', 0)
				msg_fstack += '%(_id)s %(file)s %(line)s (%(fun)s)\n' % frame_info
			msg_parts.append(msg_fstack)

		# Exception message tree
		if show_exception_stack > 0:
			msg_parts.append(_format_ex_tree(ex_info_list, show_exception_stack))

	return str.join('\n', msg_parts)


def handle_debug_interrupt(sig=None, frame=None):
	# Signal handler for debug session requests
	create_debug_console(handle_dump_interrupt(sig, frame)).interact('debug mode enabled!')


def handle_dump_interrupt(sig, frame):
	# Signal handler for state dump requests
	variables = {'_frame': frame}
	if frame:
		variables.update(frame.f_globals)
		variables.update(frame.f_locals)
	log = logging.getLogger('console.debug')
	thread_list = threading.enumerate()
	log.info('# active threads %d', len(thread_list))
	thread_display = []
	for thread in thread_list:
		thread_display.append(repr(thread))
	thread_display.sort()
	for thread_repr in thread_display:
		log.info(' - %s', thread_repr)
	frames_by_thread_id = impl_detail(sys, '_current_frames', args=(), default={})
	if not frames_by_thread_id:
		log.info('Stack of threads is not available!')
	for (thread_id, frame) in frames_by_thread_id.items():
		log.info('Stack of thread #%d:\n' % thread_id + str.join('\n',
			_format_stack(parse_frame(frame), code_context=0, truncate_var_repr=-1)))
	return variables


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

	frame_info_list = list(_collect_exception_infos_impl(exception_start))
	return (frame_info_list, exception_info_list)  # skipping top-level exception helper


def _format_ex_tree(ex_info_list, show_exception_stack=2):
	ex_msg_list = []
	if show_exception_stack == 1:
		ex_info_list = ex_info_list[-2:]
	for info in ex_info_list:
		(exception_value, exception_depth, _) = info
		prefix = ''
		if show_exception_stack != 1:
			prefix = '  ' * exception_depth
		exception_type_name = exception_value.__class__.__name__
		ex_msg_list.append('%s%s: %s' % (prefix, exception_type_name, exception_value))
		if (show_exception_stack <= 1) or hasattr(exception_value, 'nested'):
			continue
		if not hasattr(exception_value, 'args'):
			continue
		args = exception_value.args
		already_displayed = (len(args) == 1) and str(args[0]) not in str(exception_value)
		if already_displayed or (len(args) > 1):
			try:
				result = '%s%s  %s' % (prefix, len(exception_type_name) * ' ', exception_value.args)
				ex_msg_list.append(result)
			except Exception:
				clear_current_exception()
	if show_exception_stack > 1:
		return str.join('\n', ex_msg_list)
	return str.join(' - ', ex_msg_list)


def _format_stack(frame_list, code_context, truncate_var_repr):
	# Function to log source code and variables from frames
	import linecache
	linecache.checkcache()
	for frame in frame_list:
		# Output relevant code fragment
		exception_id = ''
		if frame.get('exception_id') is not None:
			exception_id = '%s-' % frame['exception_id']
		yield 'Stack #%s%02d [%s:%d] %s' % (exception_id,
			frame['idx'], frame['file'], frame['line'], frame['fun'])

		def get_source_code(line_num):
			return linecache.getline(frame['file'], line_num).rstrip().replace('\t', '  ')
		for delta_line_num in range(-code_context, code_context + 1):  # pylint:disable=bad-builtin
			if delta_line_num == 0:
				yield '\t=>| %s' % get_source_code(frame['line'] + delta_line_num)
			else:
				yield '\t  | %s' % get_source_code(frame['line'] + delta_line_num)
		yield ''
		if truncate_var_repr != -1:
			for line in _format_variables(frame['locals'], truncate_var_repr):
				yield line


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
	if variable_dict or (class_instance is not None):
		yield ''


def _safe_repr(obj, truncate_var_repr):
	try:
		repr_str = repr(obj)
	except Exception:
		return 'unable to display!'
	if (truncate_var_repr is None) or (len(repr_str) < truncate_var_repr):
		return repr_str
	return repr_str[:truncate_var_repr] + ' ... [length:%d]' % len(repr_str)
