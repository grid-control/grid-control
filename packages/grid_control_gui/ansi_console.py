from grid_control import GUI, JobClass, utils, Report
from grid_control.job_selector import ClassSelector
import re, sys, signal, termios, array, fcntl

class Console:
	attr = {'COLOR_BLACK': '30', 'COLOR_RED': '31', 'COLOR_GREEN': '32',
		'COLOR_YELLOW': '33', 'COLOR_BLUE': '34', 'COLOR_MAGENTA': '35',
		'COLOR_CYAN': '36', 'COLOR_WHITE': '37', 'BOLD': '1', 'RESET': '0'}
	cmd = {'savePos': '7', 'loadPos': '8', 'eraseDown': '[J', 'erase': '[2J'}
	for (name, esc) in attr.items():
		locals()[name] = esc

	def fmt(cls, data, attr = []):
		if sys.stdout.isatty():
			return '\033[%sm%s\033[0m' % (str.join(';', [Console.RESET] + attr), data)
		return data
	fmt = classmethod(fmt)

	def __init__(self):
		(self.stdout, self.stdin) = (sys.stdout, sys.stdin)
		def callFactory(x):
			return lambda: self._esc(x)
		for (proc, esc) in Console.cmd.items():
			setattr(self, proc, callFactory(esc))

	def _esc(self, data):
		self.stdout.write('\033' + data)
		self.stdout.flush()

	def getmaxyx(self):
		size = array.array('B', [0, 0, 0, 0])
		fcntl.ioctl(0, termios.TIOCGWINSZ, size, True)
		return (size[0], size[2])

	def move(self, row, col):
		self._esc('[%d;%dH' % (row, col))

	def setscrreg(self, top = 0, bottom = 0):
		self._esc('[%d;%dr' % (top, bottom))

	def addstr(self, data, attr = []):
		self.stdout.write(Console.fmt(data, attr))
		self.stdout.flush()


class GUIStream:
	def __init__(self, stream, screen):
		(self.stream, self.screen, self.logged) = (stream, screen, True)

		# This is a list of (regular expression, GUI attributes).  The
		# attributes are applied to matches of the regular expression in
		# the output written into this stream.  Lookahead expressions
		# should not overlap with other regular expressions.
		self.attrs = [
			('DONE(?!:)', [Console.COLOR_BLUE, Console.BOLD]),
			('FAILED(?!:)', [Console.COLOR_RED, Console.BOLD]),
			('SUCCESS(?!:)', [Console.COLOR_GREEN, Console.BOLD]),
			('(?<=DONE:)\s+[1-9]\d*', [Console.COLOR_BLUE, Console.BOLD]),
			('(?<=Failing jobs:)\s+[1-9]\d*', [Console.COLOR_RED, Console.BOLD]),
			('(?<=FAILED:)\s+[1-9]\d*', [Console.COLOR_RED, Console.BOLD]),
			('(?<=Successful jobs:)\s+[1-9]\d*', [Console.COLOR_GREEN, Console.BOLD]),
			('(?<=SUCCESS:)\s+[1-9]\d*', [Console.COLOR_GREEN, Console.BOLD]),
		]
		self.regex = re.compile('(%s)' % '|'.join(map(lambda (a, b): a, self.attrs)))

	def _attributes(self, string, pos):
		""" Retrieve the attributes for a match in string at position pos. """
		for (expr, attr) in self.attrs:
			match = re.search(expr, string)
			if match and match.start() == pos:
				return attr
		return 0

	def write(self, data):
		if self.logged:
			GUIStream.backlog.pop(0)
			GUIStream.backlog.append(data)
		idx = 0
		match = self.regex.search(data[idx:])
		while match:
			self.screen.addstr(data[idx:idx + match.start()])
			self.screen.addstr(match.group(0), self._attributes(data[idx:], match.start()))
			idx += match.end()
			match = self.regex.search(data[idx:])
		self.screen.addstr(data[idx:])
		return True

	def __getattr__(self, name):
		return self.stream.__getattribute__(name)

	def dump(cls):
		for data in filter(lambda x: x, GUIStream.backlog):
			sys.stdout.write(data)
		sys.stdout.write('\n')
	dump = classmethod(dump)
GUIStream.backlog = [None] * 100


class BasicProgressBar:
	def __init__(self, minValue = 0, maxValue = 100, totalWidth = 16):
		(self._min, self._max, self._width) = (minValue, maxValue, totalWidth)
		self.update(0)

	def update(self, newProgress = 0):
		# Compute variables
		complete = self._width - 2
		progress = max(self._min, min(self._max, newProgress))
		done = int(round(((progress - self._min) / max(1.0, float(self._max - self._min))) * 100.0))
		blocks = int(round((done / 100.0) * complete))

		# Build progress bar
		if blocks == 0:
			self._bar = '[>%s]' % (' '*(complete-1))
		elif blocks == complete:
			self._bar = '[%s]' % ('='*complete)
		else:
			self._bar = '[%s>%s]' % ('='*(blocks-1), ' '*(complete-blocks))

		# Print percentage
		text = str(done) + '%'
		textPos = (self._width - len(text) + 1) / 2
		self._bar = self._bar[0:textPos] + text + self._bar[textPos+len(text):]

	def __str__(self):
		return str(self._bar)


class ANSIConsole(GUI):
	def __init__(self, config, workflow):
		GUI.__init__(self, config, workflow)
		self._report = self._reportClass.getInstance(self._workflow.jobManager.jobDB, self._workflow.task)

	def displayWorkflow(self):
		report = self._report
		workflow = self._workflow
		def wrapper(screen):
			# Event handling for resizing
			def onResize(sig, frame):
				screen.savePos()
				(sizey, sizex) = screen.getmaxyx()
				screen.setscrreg(min(17, sizey), sizey)
				utils.printTabular.wraplen = sizex - 5
				screen.loadPos()
			screen.erase()
			onResize(None, None)
			bar = BasicProgressBar(0, len(workflow.jobManager.jobDB), 65)

			def guiWait(timeout):
				onResize(None, None)
				oldHandler = signal.signal(signal.SIGWINCH, onResize)
				result = utils.wait(timeout)
				signal.signal(signal.SIGWINCH, oldHandler)
				return result

			# Wrapping ActivityLog functionality
			class GUILog:
				def __init__(self, message):
					self.message = '%s...' % message
					self.show(self.message.center(65))

				def __del__(self):
					if hasattr(sys.stdout, 'logged'):
						self.show(' ' * len(self.message))

				def show(self, message):
					screen.savePos()
					screen.move(0, 0)
					sys.stdout.logged = False
					report.display()
					bar.update(len(workflow.jobManager.jobDB.getJobs(ClassSelector(JobClass.SUCCESS))))
					sys.stdout.write('%s\n%s\n' % (bar, message))
					sys.stdout.logged = True
					screen.loadPos()

			# Main cycle - GUI mode
			saved = (sys.stdout, sys.stderr, utils.ActivityLog)
			try:
				utils.ActivityLog = GUILog
				sys.stdout = GUIStream(saved[0], screen)
				sys.stderr = GUIStream(saved[1], screen)
				workflow.jobCycle(guiWait)
			finally:
				if sys.modules['__main__'].log: del sys.modules['__main__'].log
				sys.stdout, sys.stderr, utils.ActivityLog = saved
				screen.setscrreg()
				screen.move(1, 0)
				screen.eraseDown()
				report.display()
				sys.stdout.write('\n')
		try:
			wrapper(Console())
		finally:
			GUIStream.dump()
