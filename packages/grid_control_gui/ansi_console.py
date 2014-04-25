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
			return lambda: self.esc(x)
		for (proc, esc) in Console.cmd.items():
			setattr(self, proc, callFactory(esc))

	def esc(self, data):
		self.stdout.write('\033' + data)
		self.stdout.flush()

	def getmaxyx(self):
		size = array.array('B', [0, 0, 0, 0])
		fcntl.ioctl(0, termios.TIOCGWINSZ, size, True)
		return (size[0], size[2])

	def move(self, row, col):
		self.esc('[%d;%dH' % (row, col))

	def setscrreg(self, top = 0, bottom = 0):
		self.esc('[%d;%dr' % (top, bottom))

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

	def attributes(self, string, pos):
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
			self.screen.addstr(match.group(0), self.attributes(data[idx:], match.start()))
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


class ANSIProgressBar:
	def __init__(self, minValue = 0, maxValue = 100, totalWidth = 16):
		(self.min, self.max, self.width) = (minValue, maxValue, totalWidth)
		self.update(0)

	def update(self, newProgress = 0):
		# Compute variables
		complete = self.width - 2
		progress = max(self.min, min(self.max, newProgress))
		done = int(round(((progress - self.min) / max(1.0, float(self.max - self.min))) * 100.0))
		blocks = int(round((done / 100.0) * complete))

		# Build progress bar
		if blocks == 0:
			self.bar = '[>%s]' % (' '*(complete-1))
		elif blocks == complete:
			self.bar = '[%s]' % ('='*complete)
		else:
			self.bar = '[%s>%s]' % ('='*(blocks-1), ' '*(complete-blocks))

		# Print percentage
		text = str(done) + '%'
		textPos = (self.width - len(text) + 1) / 2
		self.bar = self.bar[0:textPos] + text + self.bar[textPos+len(text):]

	def __str__(self):
		return str(self.bar)


class ANSIConsole(GUI):
	def __init__(self, config, workflow):
		GUI.__init__(self, config, workflow)
		self._report = self._reportClass.getInstance(self._workflow.jobManager.jobDB, configString = self.getHeader())

	def displayWorkflow(self):
		if self._workflow.runContinuous:
			utils.vprint(level = -1)

	def displayWorkflow(self):
		jobMgr = self._workflow.jobManager
		header = self.getHeader()
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
			bar = ANSIProgressBar(0, len(jobMgr.jobDB), 65)

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
					bar.update(len(jobMgr.jobDB.getJobs(ClassSelector(JobClass.SUCCESS))))
					self._report.display(message = '%s\n%s' % (bar, message))
					report.Report(jobMgr.jobDB, header = header).summary()
					sys.stdout.logged = True
					screen.loadPos()

			# Main cycle - GUI mode
			saved = (sys.stdout, sys.stderr, utils.ActivityLog)
			try:
				utils.ActivityLog = GUILog
				sys.stdout = GUIStream(saved[0], screen)
				sys.stderr = GUIStream(saved[1], screen)
				self._workflow.jobCycle(guiWait)
			finally:
				if sys.modules['__main__'].log: del sys.modules['__main__'].log
				sys.stdout, sys.stderr, utils.ActivityLog = saved
				screen.setscrreg()
				screen.move(1, 0)
				screen.eraseDown()
				self._report.display()
		try:
			wrapper(Console())
		finally:
			GUIStream.dump()

	def getHeader(self):
		tmp = self._workflow.task.taskConfigName + ' / ' + self._workflow.task.taskID
		if len(tmp) < 45:
			return tmp
		tmp = self._workflow.task.taskConfigName
		if len(tmp) < 45:
			return tmp
		return self._workflow.task.taskID
