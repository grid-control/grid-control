import sys, os, StringIO, tarfile, time, fnmatch, re, popen2
from grid_control import InstallationError, ConfigError, RuntimeError

try:
	import hashlib
	md5 = hashlib.md5
except:
	import md5
	md5 = md5.md5

def sorted(list, comp = None):
	tmp = list[:]
	if comp != None:
		tmp.sort(comp)
	else:
		tmp.sort()
	return tmp


class PersistentDict(dict):
	def __init__(self, filename, delimeter = "="):
		dict.__init__(self)
		(self.format, self.filename) = (delimeter, filename)
		try:
			self.update(DictFormat(self.format).parse(open(filename)))
		except:
			pass

	def write(self, newdict = {}, update = True):
		if not update:
			self.clear()
		self.update(newdict)
		try:
			open(self.filename, 'w').writelines(DictFormat(self.format).format(self))
		except:
			raise RuntimeError('Could not write to file %s' % self.filename)


def atRoot(*args):
	return os.path.join(atRoot.root, *args)


def verbosity():
	return verbosity.setting


def vprint(text, level = 0, printTime = False, newline = True, once = False):
	if once:
		if text in vprint.log:
			return
		else:
			vprint.log.append(text)
	if verbosity() > level:
		if printTime:
			print "%s -" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
		if newline:
			print text
		else:
			print text,
vprint.log = []


def boolUserInput(text, default):
	while True:
		try:
			userinput = raw_input('%s %s: ' % (text, ('[no]', '[yes]')[default]))
		except:
			sys.exit(0)
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


def se_copy(src, dst, force = True):
	src = src.replace('dir://', 'file://')
	dst = dst.replace('dir://', 'file://')
	lib = atRoot(os.path.join('share', 'run.lib'))
	cmd = 'print_and_%seval "url_copy_single%s" "%s" "%s"' % (('', 'q')[verbosity() == 0], ('', '_force')[force], src, dst)
	proc = popen2.Popen4('source %s || exit 1; %s' % (lib, cmd), True)
	se_copy.lastlog = proc.fromchild.read()
	return proc.wait() == 0


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
		try:
			lines = lines.splitlines()
		except:
			pass
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
	def format(self, dict, printNone = False, fkt = lambda (x, y, z): (x, y, z), format = '%s%s%s\n'):
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
	if secs < 0:
		return ""
	return "%dh %0.2dmin %0.2dsec" % (secs / 60 / 60, (secs / 60) % 60, secs % 60)


def parseTuples(string):
	"""Parse a string for keywords and tuples of keywords."""
	def to_tuple_or_str((t, s)):
		if len(s) > 0:
			return s
		elif len(t.strip()) == 0:
			return tuple()
		return tuple(map(str.strip, t.split(',')))

	return map(to_tuple_or_str, re.findall('\(([\w\s,]*)\)|(\w+)', string))


def genTarball(outFile, dir, pattern):
	def walk(tar, root, dir):
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
				if fnmatch.fnmatch(name, match):
					break
			else:
				if os.path.isdir(os.path.join(root, name)):
					walk(tar, root, name)
				continue
			if not neg:
				tar.add(os.path.join(root, name), name)
		del activity

	tar = tarfile.open(outFile, 'w:gz')
	walk(tar, dir, '')
	tar.close()


class AbstractObject:
	def __init__(self):
		raise Exception('AbstractObject cannot be instantiated.')

	def open(cls, name, *args, **kwargs):
		packages = name.split('.')[:-1]
		for package in range(len(packages)):
			__import__(str.join(".", packages[:(package + 1)]))
		try:
			if len(name.split('.')) > 1:
				newcls = getattr(sys.modules[str.join(".", packages)], name.split('.')[-1])
			else:
				newcls = getattr(sys.modules['grid_control'], name)
			if not issubclass(newcls, cls):
				raise Exception
		except:
			raise ConfigError("%s '%s' does not exist!" % (cls.__name__, name))

		return newcls(*args, **kwargs)
	open = classmethod(open)


class ActivityLog:
	class Activity:
		def __init__(self, stream, message):
			self.stream = stream
			self.message = '%s...' % message
			self.status = False

		def run(self):
			if not self.status:
				self.stream.write(self.message)
				self.stream.flush()
				self.status = True

		def clear(self):
			if self.status:
				self.stream.write('\r%s\r' % (' ' * len(self.message)))
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
			return self.__stream.__getattribute__(name)

	def __init__(self, message):
		self.saved = (sys.stdout, sys.stderr)
		self.activity = self.Activity(sys.stdout, message)

		sys.stdout = self.WrappedStream(sys.stdout, self.activity)
		sys.stderr = self.WrappedStream(sys.stderr, self.activity)

	def __del__(self):
		sys.stdout, sys.stderr = self.saved


def accumulate(status, marker):
	(cleared, buffer) = (True, '')
	for line in status:
		if line != marker:
			buffer += line
			cleared = False
		else:
			yield buffer
			(cleared, buffer) = (True, '')
	if not cleared:
		yield buffer


class LoggedProcess(object):
	def __init__(self, cmd, args = ''):
		self.cmd = (cmd, args)
		self.proc = popen2.Popen3("%s %s" % (cmd, args), True)
		self.stdout = []
		self.stderr = []

	def getOutput(self):
		self.stdout.extend(self.proc.fromchild.readlines())
		return str.join("", self.stdout)

	def getError(self):
		self.stderr.extend(self.proc.childerr.readlines())
		return str.join("", self.stderr)

	def iter(self, opts):
		while True:
			try:
				line = self.proc.fromchild.readline()
			except:
				opts.abort = True
				break
			if not line:
				break
			self.stdout.append(line)
			yield line

	def wait(self):
		return self.proc.wait()

	def getAll(self):
		self.stdout.extend(self.proc.fromchild.readline())
		self.stderr.extend(self.proc.childerr.readlines())
		return (self.wait(), self.stdout, self.stderr)


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


def lenSplit(list, maxlen):
	clen = 0
	tmp = []
	for item in list:
		if clen + len(item) < maxlen:
			tmp.append(item)
			clen += len(item)
		else:
			tmp.append('')
			yield tmp
			tmp = [item]
			clen = len(item)
	yield tmp


def printTabular(head, entries, fmtString = ''):
	justFunDict = { 'l': str.ljust, 'r': str.rjust, 'c': str.center }
	justFun = dict(map(lambda (idx, x): (idx[0], justFunDict[x]), zip(head, fmtString)))

	maxlen = dict(map(lambda (id, name): (id, len(name)), head))
	head = [ x for x in head ]
	entries = [ x for x in entries ]

	for entry in filter(lambda x: x, entries):
		for id, name in head:
			maxlen[id] = max(maxlen.get(id, len(name)), len(str(entry.get(id, ''))))

	headentry = dict(map(lambda (id, name): (id, name.center(maxlen[id])), head))
	for entry in [headentry, None] + entries:
		format = lambda id, x: justFun.get(id, str.rjust)(str(x), maxlen[id])
		applyFmt = lambda fun: map(lambda (id, name): format(id, fun(id)), head)
		if entry == None:
			print("=%s=" % str.join("=+=", applyFmt(lambda id: '=' * maxlen[id])))
		else:
			print(" %s " % str.join(" | ", applyFmt(lambda id: entry.get(id, ''))))
