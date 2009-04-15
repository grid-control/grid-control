from __future__ import generators
import sys, os, bisect, popen2, StringIO
from grid_control import InstallationError, ConfigError

try:
	enumerate = enumerate
except:
	# stupid python 2.2 doesn't have a builtin enumerator
	def enumerate(iterable):
		i = 0
		for item in iterable:
			yield (i, item)
			i += 1

# Python 2.2 has no tempfile.mkstemp
def mkstemp(ending):
	while True:
		fn = tempfile.mktemp(ending)
		try:
			fd = os.open(jdl, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
		except OSError:
			continue
		break
	return (fd, fn)


def deprecated(text):
	print open(atRoot('share', 'fail.txt'), 'r').read()
	print("[DEPRECATED] %s" % text)
	userinput = raw_input('Do you want to continue? [no]: ')
	if userinput != 'yes':
		sys.exit(0)


class VirtualFileObject(StringIO.StringIO):
	def __init__(self, name, lines):
		StringIO.StringIO.__init__(self, str.join('', lines))
		self.name = name
		self.size = len(self.getvalue())


class DictFormat(object):
	def __init__(self, delimeter, escapeString = False, types = True):
		self.delimeter = delimeter
		self.types = types
		self.escapeString = escapeString

	def parseType(self, x):
		try:
			if '.' in x:
				return float(x)
			else:
				return int(x)
		except ValueError:
			return x

	# Parse dictionary lists
	def parse(self, lines, lowerCaseKey = True):
		data = {}
		currentline = ''
		doAdd = False
		for line in lines:
			if self.escapeString:
				# Accumulate lines until closing " found
				if (line.count('"') - line.count('\\"')) % 2:
					doAdd = not doAdd
				currentline += line
				if doAdd:
					continue
			else:
				currentline = line
			try:
				# split at first occurence of delimeter and strip spaces around
				key, value = map(lambda x: x.strip(), currentline.split(self.delimeter, 1))
				if self.escapeString:
					value = value.strip('"').replace('\\"', '"').replace('\\$', '$')
				if self.types:
					value = self.parseType(value)
				if lowerCaseKey:
					key = key.lower()
				data[key] = value
			except:
				# in case no delimeter was found
				pass
			currentline = ''
		if doAdd:
			raise ConfigError('Invalid dict format in %s' % fp.name)
		return data

	# Format dictionary list
	def format(self, dict, printNone = False, fkt = lambda (x,y,z): (x,y,z), format = '%s%s%s\n'):
		result = []
		for key in dict.keys():
			value = dict[key]
			if value == None and not printNone:
				continue
			if self.escapeString and isinstance(value, str):
				value = '"%s"' % str(value).replace('"', '\\"').replace('$', '\\$')
				lines = value.split('\n')
				result.append(format % fkt((key, self.delimeter, lines[0])))
				result.extend(map(lambda x: x + '\n', lines[1:]))
			else:
				result.append(format % fkt((key, self.delimeter, value)))
		return result


def atRoot(*args):
	# relies on _root to be set in go.py
	return os.path.join(sys.modules['__main__']._root, *args)


def searchPathFind(program):
	try:
		path = os.environ['PATH'].split(':')
	except:
		# Hmm, something really wrong
		path = ['/bin', '/usr/bin', '/usr/local/bin']

	for dir in path:
		fname = os.path.join(dir, program)
		if os.path.exists(fname):
			return fname
	raise InstallationError("%s not found" % program)


def shellEscape(value):
	repl = { '\\': r'\\', '\"': r'\"', '$': r'\$' }
	def replace(char):
		try:
			return repl[char]
		except:
			return char

	return '"' + str.join('', map(replace, value)) + '"'


def parseTime(usertime):
	if usertime == None or usertime == '':
		return -1
	tmp = map(int, usertime.split(":"))
	if len(tmp) > 3:
		raise ConfigError('Invalid time format: %s' % usertime)
	while len(tmp) < 3:
		tmp.append(0)
	if tmp[2] > 59 or tmp[1] > 59:
		raise ConfigError('Invalid time format: %s' % usertime)
	return reduce(lambda x, y: x * 60 + y, tmp)


def genTarball(outFile, dir, inFiles):
	tarExec = searchPathFind('tar')

	cmd = "%s -C %s -f %s -cz %s" % \
	      (tarExec, shellEscape(dir), shellEscape(outFile),
	       str.join(' ', map(shellEscape, inFiles)))

	activity = ActivityLog('generating tarball')

	proc = popen2.Popen3(cmd, True)
	msg = str.join('', proc.fromchild.readlines())
	retCode = proc.wait()

	del activity

	if retCode != 0:
		raise InstallationError("Error creating tar file: %s" % msg)


def parseShellDict(fp):
	data = {}
	lineIter = iter(fp)

	def getString(value, lineIter):
		inString = False
		result = []
		pos = 0
		while True:
			if inString:
				back = value.find('\\', pos)
				quote = value.find('"', pos)
				if back >= 0 and quote >= 0:
					if back < quote:
						quote = -1
					else:
						back = -1
				if back >= 0:
					if len(value) < back + 2:
						raise ConfigError('Invalid dict format in %s' % fp.name)
					if back > pos:
						result.append(value[pos:back])
					result.append(value[back + 1])
					pos = back + 2
				elif quote >= 0:
					if quote > pos:
						result.append(value[pos:quote])
					pos = quote + 1
					inString = False
				else:
					if len(value) > pos:
						result.append(value[pos:])
					pos = -1

				if pos < 0 or pos >= len(value):
					if not inString:
						break

					try:
						value = lineIter.next()
						pos = 0
					except StopIteration:
						raise ('Invalid job format')
			else:
				value = value[pos:].lstrip()
				if not len(value):
					break
				elif value[0] != '"':
					raise RuntimeError('Invalid job file')
				pos = 1
				inString = True

		return str.join('', result)

	while True:
		try:
			line = lineIter.next()
		except StopIteration:
			break
		key, value = line.split('=', 1)
		key = key.strip()
		value = value.lstrip()
		if value[0] == '"':
			value = getString(value, lineIter)
		elif value.find('.') >= 0:
			value = float(value)
		else:
			value = int(value)
		data[key] = value

	return data


class AbstractObject:
	def __init__(self):
		raise Exception('AbstractObject cannot be instantiated.')

	def open(cls, name, *args, **kwargs):
		try:
			newcls = getattr(sys.modules['grid_control'], name)
			if not issubclass(newcls, cls):
				raise Exception
		except:
			raise ConfigError("%s '%s' does not exist!" % (cls.__name__, name))

		return newcls(*args, **kwargs)
	open = classmethod(open)


class SortedList(list):
	def __init__(self, arg = []):
		for item in arg:
			self.add(item)

	def add(self, item):
		bisect.insort(self, item)

	def has(self, item):
		pos = bisect.bisect_left(self, item)
		return pos < len(self) and self[pos] == item

	def remove(self, item):
		pos = bisect.bisect_left(self, item)
		if pos < len(self) and self[pos] == item:
			del self[pos]

class ActivityLog:
	class Activity:
		def __init__(self, stream, message):
			self.stream = stream
			self.message = message
			self.status = False

		def run(self):
			if not self.status:
				self.stream.write('%s...' % self.message)
				self.stream.flush()
				self.status = True

		def clear(self):
			if self.status:
				self.stream.write('\r%s\r' % \
					''.ljust(len(self.message) + 3))
				self.stream.flush()
				self.status = False

	class WrappedStream:
		def __init__(self, stream, activity):
			self.__stream = stream
			self.__activity = activity
			self.__activity.run()

		def __del__(self):
			self.__activity.clear()

		def write(self, data):
			self.__activity.clear()
			retVal = self.__stream.write(data)
			if data.endswith('\n'):
				self.__activity.run()
			return retVal

		def __getattr__(self, name):
			return self.__stream.__getattr__(name)

	def __init__(self, message):
		self.saved = (sys.stdout, sys.stderr)
		self.activity = self.Activity(sys.stdout, message)

		sys.stdout = self.WrappedStream(sys.stdout, self.activity)
		sys.stderr = self.WrappedStream(sys.stderr, self.activity)

	def __del__(self):
		sys.stdout, sys.stderr = self.saved


def activityLog(message, fn, *args, **kwargs):
	activity = ActivityLog(message)
	try:
		return fn(*args, **kwargs)
	finally:
		del activity
