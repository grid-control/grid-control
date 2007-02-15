from __future__ import generators
import sys, os, bisect, popen2
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


def getRoot():
	# relies on _root to be set in go.py
	return sys.modules['__main__']._root


def atRoot(*args):
	return os.path.join(getRoot(), *args)


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
	repl = { '\\': r'\\', '\"': r'\"' }
	def replace(char):
		try:
			return repl[char]
		except:
			return char
	return '"' + str.join('', map(replace, value)) + '"'


def genTarball(outFile, dir, inFiles):
	tarExec = searchPathFind('tar')

	cmd = "%s -C %s -f %s -cz %s" % \
	      (tarExec, shellEscape(dir), shellEscape(outFile),
	       str.join(' ', map(shellEscape, inFiles)))
	proc = popen2.Popen3(cmd, True)
	msg = str.join('', proc.fromchild.readlines())
	retCode = proc.wait()
	if retCode != 0:
		raise InstallationError("Error creating tar file: %s" % msg)


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
