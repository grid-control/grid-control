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

import sys, logging

def clearException():
	try: # python 2 needs manual clear of exception information after the exception handler
		sys.exc_clear()
	except Exception: # python 3 removed this function
		pass

def safeRepr(obj, verbose):
	try:
		value = repr(obj)
	except Exception:
		return 'unable to display!'
	if (len(value) < 200) or verbose:
		return value
	return value[:200] + ' ... [length:%d]' % len(value)

# Function to log local and class variables
def formatVariables(variables, showLongVariables = False):
	maxlen = 0
	for var in variables:
		maxlen = max(maxlen, len(var))
	def display(keys, varDict, varPrefix = ''):
		keys.sort()
		for var in keys:
			value = safeRepr(varDict[var], showLongVariables)
			if 'password' in var:
				value = '<redacted>'
			yield '\t\t%s%s = %s' % (varPrefix, var.ljust(maxlen), value)

	classVariable = variables.pop('self', None)
	if variables:
		yield '\tLocal variables:'
		for line in display(list(variables.keys()), variables):
			yield line
	if classVariable is not None:
		yield '\tClass variables (%s):' % safeRepr(classVariable, showLongVariables)
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
			yield '\t\t<unable to acces class>'
	if variables:
		yield ''

# Function to log source code and variables from frames
def formatStack(frames, codeContext = 0, showVariables = True, showLongVariables = True):
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
			for line in formatVariables(frame['locals'], showLongVariables = showLongVariables):
				yield line

# Parse traceback information
def parseTraceback(traceback):
	result = []
	while traceback:
		result.append({'idx': len(result) + 1,
			'file': traceback.tb_frame.f_code.co_filename,
			'line': traceback.tb_lineno, # tb_lineno shows the location of the exception cause
			'fun': traceback.tb_frame.f_code.co_name,
			'locals': dict(traceback.tb_frame.f_locals)})
		traceback = traceback.tb_next
	return result

# Parse stack frame
def parseFrame(frame):
	result = []
	while frame:
		result.insert(0, {
			'file': frame.f_code.co_filename,
			'line': frame.f_lineno,
			'fun': frame.f_code.co_name,
			'locals': dict(frame.f_locals)})
		frame = frame.f_back
	for idx, entry in enumerate(result):
		entry['idx'] = idx
	return result

class NestedExceptionHelper(object):
	def __init__(self, exValue, exTraceback):
		self.nested = [exValue]
		self.traceback = parseTraceback(exTraceback)

# nested exception base class
class NestedException(Exception):
	def __init__(self, *args, **kwargs):
		self.nested = []
		self.traceback = []
		if sys.exc_info()[1]:
			self.nested = [sys.exc_info()[1]]
			self.traceback = parseTraceback(sys.exc_info()[2])
		Exception.__init__(self, *args, **kwargs)

# some error in using the API
class APIError(NestedException):
	pass

# some error related to abstract functions
class AbstractError(APIError):
	def __init__(self):
		try:
			fun_name = getattr(sys, '_getframe')(1).f_code.co_name # python implementation detail
		except Exception:
			fun_name = 'The invoked method'
		APIError.__init__(self, '%s is an abstract function!' % fun_name)

# Collect full traceback and exception context
def collectExceptionInfos(exType, exValue, exTraceback):
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

# Formatter class to display exception details
class ExceptionFormatter(logging.Formatter):
	def __init__(self, showCodeContext, showVariables, showFileStack):
		logging.Formatter.__init__(self)
		(self._showCodeContext, self._showVariables, self._showFileStack) =\
			(showCodeContext, showVariables, showFileStack)

	def __repr__(self):
		return '%s(code = %r, var = %r, file = %r)' % (self.__class__.__name__,
			self._showCodeContext, self._showVariables, self._showFileStack)

	def format(self, record):
		if record.exc_info in [None, (None, None, None)]:
			return logging.Formatter.format(self, record)
		traceback, infos = collectExceptionInfos(*record.exc_info)

		msg = '\n%s\n\n' % record.msg
		# Code and variable listing
		if self._showCodeContext > 0:
			stackInfo = formatStack(traceback, codeContext = self._showCodeContext - 1,
				showVariables = self._showVariables > 0, showLongVariables = self._showVariables > 1)
			msg += str.join('\n', stackInfo) + '\n'
		# File stack with line information
		if self._showFileStack:
			msg += 'File stack:\n'
			for tb in traceback:
				msg += '%s %s %s (%s)\n' % (tb.get('trackingID', '') + '|%d' % tb.get('idx', 0), tb['file'], tb['line'], tb['fun'])
			msg += '\n'
		# Exception message tree
		def formatInfos(info):
			(exValue, exDepth, _) = info
			result = '%s%s: %s' % ('  ' * exDepth, exValue.__class__.__name__, exValue)
			if not isinstance(exValue, NestedException) and hasattr(exValue, 'args') and (len(exValue.args) > 1):
				try:
					result += '\n%s%s  %s' % ('  ' * exDepth, len(exValue.__class__.__name__) * ' ', exValue.args)
				except Exception:
					pass
			return result
		for info in infos:
			msg += formatInfos(info) + '\n'
			if logging.getLogger().isEnabledFor(logging.INFO1):
				msg += '\n'
		return msg

# Signal handler for debug session requests
def handle_debug_interrupt(sig, frame):
	import code
	variables = {'_frame': frame}
	variables.update(frame.f_globals)
	variables.update(frame.f_locals)
	console = code.InteractiveConsole(variables)
	console.push('import rlcompleter, readline')
	console.push('readline.parse_and_bind("tab: complete")')
	console.push('readline.set_completer(rlcompleter.Completer(globals()).complete)')
	try:
		stackDict = getattr(sys, '_current_frames')() # python implementation detail
	except Exception:
		stackDict = {}
	log = logging.getLogger('debug_session')
	for threadID in stackDict:
		log.critical('Stack of thread #%d:\n' % threadID + str.join('\n',
			formatStack(parseFrame(stackDict[threadID]), codeContext = 0, showVariables = False)))
	console.interact('debug mode enabled!')

# Utility class to collect multiple exceptions and throw them at a later time
class ExceptionCollector(object):
	def __init__(self):
		self._exceptions = []

	def collect(self):
		self._exceptions.append(NestedExceptionHelper(sys.exc_info()[1], sys.exc_info()[2]))
		clearException()

	def raise_any(self, value, collapse = False):
		if not self._exceptions:
			return
		if collapse and (len(self._exceptions) == 1):
			value = self._exceptions[0]
		elif hasattr(value, 'nested'):
			value.nested.extend(self._exceptions)
		self._exceptions = []
		raise value
