#-#  Copyright 2007-2015 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import sys, logging

# Function to log local and class variables
def logVariables(log, variables):
	def safeRepr(obj):
		try:
			value = repr(obj)
		except Exception:
			raise
			return 'unable to display!'
		try:
			if log.isEnabledFor(logging.INFO1) or (len(value) < 500):
				return value
		except Exception:
			return value[:500] + ' ... [length:%d]' % len(value)
	def display(keys, varDict, varPrefix = ''):
		maxlen = max(map(len, variables.keys()) + [0])
		keys.sort()
		for var in keys:
			value = safeRepr(varDict[var])
			if 'password' in var:
				value = '<redacted>'
			log.debug('\t\t%s%s = %s', varPrefix, var.ljust(maxlen), value)

	if filter(lambda v: v != 'self', variables):
		log.debug('\tLocal variables:')
		display(filter(lambda v: v != 'self', variables), variables)
	if 'self' in variables:
		log.debug('\tClass variables (%s):' % safeRepr(variables['self']))
		if hasattr(variables['self'], '__dict__'):
			classVariables = variables['self'].__dict__
		elif hasattr(variables['self'], '__slots__'):
			classVariables = dict(map(lambda attr: (attr, getattr(variables['self'], attr)), variables['self'].__slots__))
		try:
			display(classVariables.keys(), classVariables, 'self.')
		except Exception:
			log.debug('\t\t<unable to acces>')
	if variables:
		log.debug('')

# Function to log source code and variables from frames
def logStack(log, frames, codeContext = 0):
	import linecache
	for frame in frames:
		# Output relevant code fragment
		linecache.checkcache(frame['file'])
		ctx = ''
		if frame['context']:
			ctx = '%s-' % frame['context']
		log.info('Stack #%s%02d [%s:%d] %s', ctx, frame['idx'], frame['file'], frame['line'], frame['fun'])
		fmtLine = lambda line: linecache.getline(frame['file'], line).rstrip().replace('\t', '  ')
		for delta_line in range(-codeContext, codeContext + 1):
			if delta_line == 0:
				log.info('\t=>| %s', fmtLine(frame['line'] + delta_line))
			else:
				log.info('\t  | %s', fmtLine(frame['line'] + delta_line))
		log.info('')
		if log.isEnabledFor(logging.DEBUG):
			logVariables(log, frame['locals'])

# Parse traceback information
def parseTraceback(contextName, exClass, exValue, exStack):
	result = []
	while exStack:
		result.append({'context': contextName, 'idx': len(result) + 1,
			'file': exStack.tb_frame.f_code.co_filename,
			'line': exStack.tb_lineno, # tb_lineno shows the location of the exception cause
			'fun': exStack.tb_frame.f_code.co_name,
			'locals': dict(exStack.tb_frame.f_locals)})
		exStack = exStack.tb_next
	return (result[:-1], result[-1:])

# Parse stack frame
def parseFrame(contextName, frame):
	result = []
	while frame:
		result.insert(0, {'context': contextName,
			'file': frame.f_code.co_filename,
			'line': frame.f_lineno,
			'fun': frame.f_code.co_name,
			'locals': dict(frame.f_locals)})
		frame = frame.f_back
	for idx, entry in enumerate(result):
		entry['idx'] = idx
	return result

# Collect full traceback and exception context
def collectExceptionInfos(exType, exValue, exStack):
	top_context = None
	if hasattr(exValue, 'context'):
		top_context = exValue.context
	traceback, traceback_tail = parseTraceback(top_context, exType, exValue, exStack)
	context = {}
	traceback_stack = traceback_tail
	while hasattr(exValue, 'context') and hasattr(exValue, 'traceback') and hasattr(exValue, 'nested'): # contains infos about nested exceptions
		context[exValue.context] = exValue
		# append head and put tail onto the traceback stack
		cur_traceback, cur_traceback_tail = exValue.traceback
		traceback.extend(cur_traceback)
		traceback_stack.extend(cur_traceback_tail)
		exValue = exValue.nested
	if exValue: # inner exception is not a GCError
		context[0] = exValue
	while traceback_stack:
		traceback.append(traceback_stack.pop())
	return (traceback, context)

# Exception handler which outputs a stack trace to the logging facility
def logException(exType, exValue, exStack):
	traceback, context = collectExceptionInfos(exType, exValue, exStack)
	import time
	log = logging.getLogger('exception')
	log.critical('Exception occured: %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
	try:
		import grid_control.utils
		log.critical('grid-control: %s' % grid_control.utils.getVersion())
	except Exception:
		log.critical('grid-control: Unknown version')
	log.critical('')
	logStack(log, traceback, codeContext = 1)
	context_keys = context.keys()
	context_keys.sort()
	while context_keys:
		exValue = context[context_keys.pop()]
		msg = '%s: %s' % (exValue.__class__.__name__, exValue)
		if not isinstance(exValue, GCError):
			msg += '\n%s  %s' % (len(exValue.__class__.__name__) * ' ', exValue.args)
		log.critical(msg)
		sys.stderr.write(msg + '\n')

# Exception handler for interactive mode:
def gc_excepthook(exType, exValue, exStack):
	logException(exType, exValue, exStack)
	sys.stderr.write('\n')
	for handler in logging.getLogger('exception').handlers:
		if isinstance(handler, logging.FileHandler):
			sys.stderr.write('In case this is caused by a bug, please send the log file:\n')
			sys.stderr.write('\t"%s"\nto grid-control-dev@googlegroups.com\n' % handler.baseFilename)
			break
sys.excepthook = gc_excepthook

# grid-control exception base class
class GCError(Exception):
	def __init__(self, *args, **kwargs):
		self.context = 1
		self.nested = sys.exc_info()[1]
		if hasattr(self.nested, 'context'):
			self.context = self.nested.context + 1
		self.traceback = parseTraceback(self.context - 1, *sys.exc_info())
		Exception.__init__(self, *args, **kwargs)

class ConfigError(GCError):
	pass	# some error with the configuration

class InstallationError(GCError):
	pass	# some error with installed programs

class RuntimeError(GCError):
	pass	# some error with the runtime

class UserError(GCError):
	pass	# some error caused by the user

class APIError(GCError):
	pass	# some error in using the API

class GridError(GCError):
	pass	# some error with the Backend

class DatasetError(GCError):
	pass	# some error with the dataset

class TimeoutError(GCError):
	pass	# timeout while waiting for an operation to finish

class InputValidationError(GCError):
	pass	# error during input validation for publication in DBS3

# some error related to abstract functions
class AbstractError(APIError):
	def __init__(self):
		APIError.__init__(self, '%s is an abstract function!' % sys._getframe(1).f_code.co_name)

class RethrowError(GCError):
	pass
