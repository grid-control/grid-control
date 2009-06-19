from __future__ import generators
import sys, os, bisect, popen2, StringIO, tarfile, time
from fnmatch import fnmatch
from grid_control import InstallationError, ConfigError

def verbosity():
	return sys.modules['__main__']._verbosity


def vprint(text, level = 0, printTime = False):
	if verbosity() > level:
		if printTime:
			print "%s - " % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
		print text


def boolUserInput(text, default):
	while True:
		userinput = raw_input('%s %s: ' % (text, ('[no]', '[yes]')[default]))
		if userinput == '':
			return default
		if userinput.lower() in ('yes', 'y', 'true', 'ok'):
			return True
		if userinput.lower() in ('no', 'n', 'false'):
			return False
		if userinput != 'yes' and userinput != '':
			return 0
		print 'Invalid input! Answer with "yes" or "no"'
	

def deprecated(text):
	print open(atRoot('share', 'fail.txt'), 'r').read()
	print("[DEPRECATED] %s" % text)
	if not boolUserInput('Do you want to continue?', False):
		sys.exit(0)


def se_copy(source, target, force = True):
	# kill the runtime on se
	if force:
		tool = 'se_copy_force.sh'
	else:
		tool = 'se_copy.sh'

	proc = popen2.Popen4('%s %s %s' % (atRoot('share', tool), source, target), True)
	result = proc.wait()
	if sys.modules['__main__']._verbosity or (result != 0):
		sys.stderr.write(proc.fromchild.read())
	return result == 0


class VirtualFile(StringIO.StringIO):
	def __init__(self, name, lines):
		StringIO.StringIO.__init__(self, str.join('', lines))
		self.name = name
		self.size = len(self.getvalue())


	def getTarInfo(self):
		info = tarfile.TarInfo(self.name)
		info.size = self.size
		return (info, self)


class DictFormat(object):
	# escapeString = escape '"', '$'
	# types = preserve type information
	def __init__(self, delimeter = '=', escapeString = False, types = True):
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
	def parse(self, lines, lowerCaseKey = True, keyRemap = {}):
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
				key, value = map(str.strip, currentline.split(self.delimeter, 1))
				if self.escapeString:
					value = value.strip('"').replace('\\"', '"').replace('\\$', '$')
				if lowerCaseKey:
					key = key.lower()
				if self.types:
					value = self.parseType(value)
					key = self.parseType(key)
				# do .encode('utf-8') ?
				data[keyRemap.get(key, key)] = value
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


def strTime(secs):
	return "%dh %0.2dmin %0.2dsec" % (secs / 60 / 60, (secs / 60) % 60, secs % 60)


def genTarball(outFile, dir, pattern):
	def walk(tar, root, pattern, dir):
		if len(dir) > 50:
			msg = dir[:15] + '...' + dir[len(dir)-32:]
		else:
			msg = dir
		activity = ActivityLog('Generating tarball: %s' % msg)
		for file in os.listdir(os.path.join(root, dir)):
			if len(dir):
				name = os.path.join(dir, file)
			else:
				name = file
			for match in pattern:
				neg = match[0] == '-'
				if neg: match = match[1:]
				if fnmatch(name, match):
					break
			else:
				if os.path.isdir(os.path.join(root, name)):
					walk(tar, root, pattern, name)
				continue
			if not neg:
				tar.add(os.path.join(root, name), name)
		del activity

	tar = tarfile.open(outFile, 'w:gz')
	walk(tar, dir, pattern, '')
	tar.close()


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
			try:
				return self.__stream.__getattr__(name)
			except:
				return self.__stream.__getattribute__(name)

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


def DiffLists(oldList, newList, cmpFkt, changedFkt):
	listAdded = []
	listMissing = []
	listChanged = []
	oldIter = iter(sorted(oldList, cmpFkt))
	newIter = iter(sorted(newList, cmpFkt))
	try:
		new = newIter.next()
	except:
		new = None
	try:
		old = oldIter.next()
	except:
		old = None
	while True:
		try:
			result = cmpFkt(new, old)
			if result < 0:
				listAdded.append(new)
				try:
					new = newIter.next()
				except:
					new = None
					raise
			elif result > 0:
				listMissing.append(old)
				try:
					old = oldIter.next()
				except:
					old = None
					raise
			else:
				changedFkt(listAdded, listMissing, listChanged, old, new)
				try:
					try:
						new = newIter.next()
					except:
						new = None
						raise
					old = oldIter.next()
				except:
					old = None
					raise
		except: #StopIteration:
			break
	if new:
		listAdded.append(new)
	for new in newIter:
		listAdded.append(new)
	if old:
		listMissing.append(old)
	for old in oldIter:
		listMissing.append(old)

	return (listAdded, listMissing, listChanged)
