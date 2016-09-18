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

import sys, logging, threading
from hpfwk.hpf_exceptions import NestedException, NestedExceptionHelper, clear_current_exception, impl_detail, parse_frame

def collect_exception_infos(exception_type, exception_value, exception_traceback):
	# Collect full traceback and exception context
	exception_start = NestedExceptionHelper(exception_value, exception_traceback)

	exception_info_list = []
	def collect_exception_infos_recursive(exception, cur_depth = -1, exception_id = 'T'):
		if not isinstance(exception, NestedExceptionHelper):
			cur_depth += 1
			exception_info_list.append((exception, cur_depth, exception_id))
		if hasattr(exception, 'traceback') and hasattr(exception, 'nested'): # decent
			for frame in exception.traceback[:-1]: # yield all frames except for the final raise statement
				frame['exception_id'] = exception_id
				yield frame
			for idx, exception_nested in enumerate(exception.nested):
				for frame in collect_exception_infos_recursive(exception_nested, cur_depth, exception_id + '|%d' % idx):
					yield frame
			for frame in exception.traceback[-1:]:
				frame['exception_id'] = exception_id
				yield frame

	traceback = list(collect_exception_infos_recursive(exception_start))
	return (traceback, exception_info_list) # skipping top-level exception helper


def _safe_repr(obj, truncate_len):
	try:
		repr_str = repr(obj)
	except Exception:
		return 'unable to display!'
	if (truncate_len is None) or (len(repr_str) < truncate_len):
		return repr_str
	return repr_str[:truncate_len] + ' ... [length:%d]' % len(repr_str)


def format_variables(variable_dict, truncate_len = 200):
	# Function to log local and class variables
	max_vn_len = 0
	for vn in variable_dict:
		max_vn_len = max(max_vn_len, len(vn))

	def display(vn_list, var_dict, vn_prefix = ''):
		vn_list.sort()
		for vn in vn_list:
			repr_str = _safe_repr(var_dict[vn], truncate_len)
			if 'password' in vn:
				repr_str = '<redacted>'
			yield '\t\t%s%s = %s' % (vn_prefix, vn.ljust(max_vn_len), repr_str)

	class_instance = variable_dict.pop('self', None)
	if variable_dict:
		yield '\tLocal variables:'
		for line in display(list(variable_dict.keys()), variable_dict):
			yield line
	if class_instance is not None:
		yield '\tClass variables (%s):' % _safe_repr(class_instance, truncate_len)
		if hasattr(class_instance, '__dict__'):
			class_variable_dict = class_instance.__dict__
		elif hasattr(class_instance, '__slots__'):
			class_variable_dict = {}
			for attr in class_instance.__slots__:
				class_variable_dict[attr] = getattr(class_instance, attr)
		try:
			for line in display(list(class_variable_dict.keys()), class_variable_dict, 'self.'):
				yield line
		except Exception:
			yield '\t\t<unable to access class>'
	if variable_dict or (class_instance is not None):
		yield ''


def format_stack(frame_list, code_context = 0, showVariables = True, truncate_len = 200):
	# Function to log source code and variables from frames
	import linecache
	linecache.checkcache()
	for frame in frame_list:
		# Output relevant code fragment
		exception_id = ''
		if frame.get('exception_id') is not None:
			exception_id = '%s-' % frame['exception_id']
		yield 'Stack #%s%02d [%s:%d] %s' % (exception_id, frame['idx'], frame['file'], frame['line'], frame['fun'])
		def get_source_code(line_num):
			return linecache.getline(frame['file'], line_num).rstrip().replace('\t', '  ')
		for delta_line_num in range(-code_context, code_context + 1):
			if delta_line_num == 0:
				yield '\t=>| %s' % get_source_code(frame['line'] + delta_line_num)
			else:
				yield '\t  | %s' % get_source_code(frame['line'] + delta_line_num)
		yield ''
		if showVariables:
			for line in format_variables(frame['locals'], truncate_len):
				yield line

def format_ex_tree(ex_info_list, showExStack = 2):
	ex_msg_list = []
	if showExStack == 1:
		ex_info_list = ex_info_list[-2:]
	for info in ex_info_list:
		(exception_value, exDepth, _) = info
		if showExStack == 1:
			exDepth = 0
		result = '%s%s: %s' % ('  ' * exDepth, exception_value.__class__.__name__, exception_value)
		if (showExStack > 1) and hasattr(exception_value, 'args') and not isinstance(exception_value, NestedException):
			if ((len(exception_value.args) == 1) and (str(exception_value.args[0]) not in str(exception_value))) or (len(exception_value.args) > 1):
				try:
					result += '\n%s%s  %s' % ('  ' * exDepth, len(exception_value.__class__.__name__) * ' ', exception_value.args)
				except Exception:
					clear_current_exception()
		ex_msg_list.append(result)
	if showExStack > 1:
		return str.join('\n', ex_msg_list)
	return str.join(' - ', ex_msg_list)

def format_exception(exc_info, showcode_context = 0, showVariables = 0, showFileStack = 0, showExStack = 1):
	msg_parts = []

	if exc_info not in [None, (None, None, None)]:
		traceback, ex_info_list = collect_exception_infos(*exc_info)

		# Code and variable listing
		if showcode_context > 0:
			stackInfo = format_stack(traceback, code_context = showcode_context - 1,
				showVariables = showVariables > 0, truncate_len = showVariables > 1)
			msg_parts.append(str.join('\n', stackInfo))

		# File stack with line information
		if showFileStack > 0:
			msg_fstack = 'File stack:\n'
			for tb in traceback:
				msg_fstack += '%s %s %s (%s)\n' % (tb.get('exception_id', '') + '|%02d' % tb.get('idx', 0), tb['file'], tb['line'], tb['fun'])
			msg_parts.append(msg_fstack)

		# Exception message tree
		if showExStack > 0:
			msg_parts.append(format_ex_tree(ex_info_list, showExStack))

	return str.join('\n', msg_parts)

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
	frames_by_threadID = impl_detail(sys, '_current_frames', args = (), default = {})
	if not frames_by_threadID:
		log.info('Stack of threads is not available!')
	for (threadID, frame) in frames_by_threadID.items():
		log.info('Stack of thread #%d:\n' % threadID + str.join('\n',
			format_stack(parse_frame(frame), code_context = 0, showVariables = False)))
	return variables

def create_debug_console(variables):
	import code
	console = code.InteractiveConsole(variables)
	console.push('import rlcompleter, readline')
	console.push('readline.parse_and_bind("tab: complete")')
	console.push('readline.set_completer(rlcompleter.Completer(globals()).complete)')
	return console

def handle_debug_interrupt(sig = None, frame = None):
	# Signal handler for debug session requests
	create_debug_console(handle_dump_interrupt(sig, frame)).interact('debug mode enabled!')
