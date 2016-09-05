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

# Collect full traceback and exception context
def collect_exception_infos(exType, exValue, exTraceback):
	topLevel = NestedExceptionHelper(exValue, exTraceback)

	exInfo = []
	def collectRecursive(ex, cur_depth = -1, trackingID = 'T'):
		if not isinstance(ex, NestedExceptionHelper):
			cur_depth += 1
			exInfo.append((ex, cur_depth, trackingID))
		if hasattr(ex, 'traceback') and hasattr(ex, 'nested'): # decent
			for frame in ex.traceback[:-1]: # yield all frames except for the final raise statement
				frame['trackingID'] = trackingID
				yield frame
			for idx, exNested in enumerate(ex.nested):
				for frame in collectRecursive(exNested, cur_depth, trackingID + '|%d' % idx):
					yield frame
			for frame in ex.traceback[-1:]:
				frame['trackingID'] = trackingID
				yield frame

	traceback = list(collectRecursive(topLevel))
	return (traceback, exInfo) # skipping top-level exception helper

def repr_safe(obj, verbose):
	try:
		value = repr(obj)
	except Exception:
		return 'unable to display!'
	if (len(value) < 200) or verbose:
		return value
	return value[:200] + ' ... [length:%d]' % len(value)

# Function to log local and class variables
def format_variables(variables, showLongVariables = False):
	maxlen = 0
	for var in variables:
		maxlen = max(maxlen, len(var))
	def display(keys, varDict, varPrefix = ''):
		keys.sort()
		for var in keys:
			value = repr_safe(varDict[var], showLongVariables)
			if 'password' in var:
				value = '<redacted>'
			yield '\t\t%s%s = %s' % (varPrefix, var.ljust(maxlen), value)

	classVariable = variables.pop('self', None)
	if variables:
		yield '\tLocal variables:'
		for line in display(list(variables.keys()), variables):
			yield line
	if classVariable is not None:
		yield '\tClass variables (%s):' % repr_safe(classVariable, showLongVariables)
		if hasattr(classVariable, '__dict__'):
			classVariables = classVariable.__dict__
		elif hasattr(classVariable, '__slots__'):
			classVariables = {}
			for attr in classVariable.__slots__:
				classVariables[attr] = getattr(classVariable, attr)
		try:
			for line in display(list(classVariables.keys()), classVariables, 'self.'):
				yield line
		except Exception:
			yield '\t\t<unable to access class>'
	if variables or (classVariable is not None):
		yield ''

# Function to log source code and variables from frames
def format_stack(frames, codeContext = 0, showVariables = True, showLongVariables = True):
	import linecache
	linecache.checkcache()
	for frame in frames:
		# Output relevant code fragment
		trackingDisplay = ''
		if frame.get('trackingID') is not None:
			trackingDisplay = '%s-' % frame['trackingID']
		yield 'Stack #%s%02d [%s:%d] %s' % (trackingDisplay, frame['idx'], frame['file'], frame['line'], frame['fun'])
		fmtLine = lambda line: linecache.getline(frame['file'], line).rstrip().replace('\t', '  ')
		for delta_line in range(-codeContext, codeContext + 1):
			if delta_line == 0:
				yield '\t=>| %s' % fmtLine(frame['line'] + delta_line)
			else:
				yield '\t  | %s' % fmtLine(frame['line'] + delta_line)
		yield ''
		if showVariables:
			for line in format_variables(frame['locals'], showLongVariables = showLongVariables):
				yield line

def format_ex_tree(ex_info_list, showExStack = 2):
	ex_msg_list = []
	if showExStack == 1:
		ex_info_list = ex_info_list[-2:]
	for info in ex_info_list:
		(exValue, exDepth, _) = info
		if showExStack == 1:
			exDepth = 0
		result = '%s%s: %s' % ('  ' * exDepth, exValue.__class__.__name__, exValue)
		if (showExStack > 1) and hasattr(exValue, 'args') and not isinstance(exValue, NestedException):
			if ((len(exValue.args) == 1) and (str(exValue.args[0]) not in str(exValue))) or (len(exValue.args) > 1):
				try:
					result += '\n%s%s  %s' % ('  ' * exDepth, len(exValue.__class__.__name__) * ' ', exValue.args)
				except Exception:
					clear_current_exception()
		ex_msg_list.append(result)
	if showExStack > 1:
		return str.join('\n', ex_msg_list)
	return str.join(' - ', ex_msg_list)

def format_exception(exc_info, showCodeContext = 0, showVariables = 0, showFileStack = 0, showExStack = 1):
	msg_parts = []

	if exc_info not in [None, (None, None, None)]:
		traceback, ex_info_list = collect_exception_infos(*exc_info)

		# Code and variable listing
		if showCodeContext > 0:
			stackInfo = format_stack(traceback, codeContext = showCodeContext - 1,
				showVariables = showVariables > 0, showLongVariables = showVariables > 1)
			msg_parts.append(str.join('\n', stackInfo))

		# File stack with line information
		if showFileStack > 0:
			msg_fstack = 'File stack:\n'
			for tb in traceback:
				msg_fstack += '%s %s %s (%s)\n' % (tb.get('trackingID', '') + '|%d' % tb.get('idx', 0), tb['file'], tb['line'], tb['fun'])
			msg_parts.append(msg_fstack)

		# Exception message tree
		if showExStack > 0:
			msg_parts.append(format_ex_tree(ex_info_list, showExStack))

	return str.join('\n', msg_parts)

# Signal handler for state dump requests
def handle_dump_interrupt(sig, frame):
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
			format_stack(parse_frame(frame), codeContext = 0, showVariables = False)))
	return variables

def create_debug_console(variables):
	import code
	console = code.InteractiveConsole(variables)
	console.push('import rlcompleter, readline')
	console.push('readline.parse_and_bind("tab: complete")')
	console.push('readline.set_completer(rlcompleter.Completer(globals()).complete)')
	return console

# Signal handler for debug session requests
def handle_debug_interrupt(sig = None, frame = None):
	create_debug_console(handle_dump_interrupt(sig, frame)).interact('debug mode enabled!')
