import re, sys, signal, curses, utils, report

class CursesStream:
	def __init__(self, *args):
		(self.stream, self.screen) = args
		self.logged = True

		# This is a list of (regular expression, curses attributes).  The
		# attributes are applied to matches of the regular expression in
		# the output written into this stream.  Lookahead expressions
		# should not overlap with other regular expressions.
		self.attrs = [
				('DONE(?!:)', curses.color_pair(3) | curses.A_BOLD),
				('FAILED(?!:)', curses.color_pair(1) | curses.A_BOLD),
				('SUCCESS(?!:)', curses.color_pair(2) | curses.A_BOLD),
				('(?<=DONE:)\s+[1-9]\d*', curses.color_pair(3) | curses.A_BOLD),
				('(?<=Failing jobs:)\s+[1-9]\d*', curses.color_pair(1) | curses.A_BOLD),
				('(?<=FAILED:)\s+[1-9]\d*', curses.color_pair(1) | curses.A_BOLD),
				('(?<=Successful jobs:)\s+[1-9]\d*', curses.color_pair(2) | curses.A_BOLD),
				('(?<=SUCCESS:)\s+[1-9]\d*', curses.color_pair(2) | curses.A_BOLD),
		]
		self.regex = re.compile('(%s)' % '|'.join(map(lambda (a, b): a,
			self.attrs)))

	def attributes(self, string, pos):
		"""Retrieve the attributes for a match in string at position
		pos.  This is a helper routine for write().
		"""
		for (expr, attr) in self.attrs:
			match = re.search(expr, string)
			if match and match.start() == pos:
				return attr
		return 0

	def write(self, data):
		if self.logged:
			CursesStream.backlog.pop(0)
			CursesStream.backlog.append(data)

		if curses.has_colors():
			idx = 0
			match = self.regex.search(data[idx:])
			while match:
				self.screen.addstr(data[idx:idx + match.start()])
				self.screen.addstr(match.group(0),
						self.attributes(data[idx:], match.start()))
				self.screen.refresh()
				idx += match.end()
				match = self.regex.search(data[idx:])
			self.screen.addstr(data[idx:])
		else:
			self.screen.addstr(data)
		self.screen.refresh()

		return True

	def __getattr__(self, name):
		return self.stream.__getattribute__(name)

	def dump(cls):
		for data in filter(lambda x: x, CursesStream.backlog):
			sys.stdout.write(data)
		sys.stdout.write('\n')	
	dump = classmethod(dump)
CursesStream.backlog = [None for i in range(100)]


class ProgressBar:
	def __init__(self, minValue = 0, maxValue = 100, totalWidth=16):
		(self.min, self.max) = (minValue, maxValue)
		self.width = totalWidth
		self.update(0)

	def update(self, newProgress = 0):
		# Compute variables
		complete = self.width - 2
		progress = max(self.min, min(self.max, newProgress))
		done = int(round(((progress - self.min) / float(self.max - self.min)) * 100.0))
		blocks = int(round((done / 100.0) * complete))

		# Build progress bar
		if blocks == 0:
			self.bar = "[>%s]" % (' '*(complete-1))
		elif blocks == complete:
			self.bar = "[%s]" % ('='*complete)
		else:
			self.bar = "[%s>%s]" % ('='*(blocks-1), ' '*(complete-blocks))

		# Print percentage
		text = str(done) + "%"
		textPos = (self.width - len(text) + 1) / 2
		self.bar = self.bar[0:textPos] + text + self.bar[textPos+len(text):]

	def __str__(self):
		return str(self.bar)


def CursesGUI(jobs, jobCycle):
	def cursesWrapper(screen):
		screen.scrollok(True)

		try:
			curses.use_default_colors()
			curses.init_pair(1, curses.COLOR_RED, -1)
			curses.init_pair(2, curses.COLOR_GREEN, -1)
			curses.init_pair(3, curses.COLOR_CYAN, -1)
		except:
			screen.attron(curses.A_BOLD)
			curses.init_pair(1, curses.COLOR_RED, 0)
			curses.init_pair(2, curses.COLOR_GREEN, 0)
			curses.init_pair(3, curses.COLOR_CYAN, 0)

		# Event handling for resizing
		def onResize(sig, frame):
			oldy = screen.getyx()[0]
			curses.endwin()
			screen.refresh()
			(sizey, sizex) = screen.getmaxyx()
			screen.setscrreg(min(16, sizey - 2), sizey - 1)
			screen.move(min(sizey - 1, max(16, oldy)), 0)
		onResize(None, None)
		signal.signal(signal.SIGWINCH, onResize)
		bar = ProgressBar(0, jobs.nJobs, 65)

		# Wrapping ActivityLog functionality
		class CursesLog:
			def __init__(self, message):
				self.message = "%s..." % message
				self.show(self.message.center(65))

			def __del__(self):
				self.show(' ' * len(self.message))

			def show(self, message):
				oldpos = screen.getyx()
				screen.move(0, 0)
				sys.stdout.logged = False
				bar.update(len(jobs.ok))
				report.Report(jobs, jobs).summary("%s\n%s" % (bar, message))
				sys.stdout.logged = True
				screen.move(*oldpos)
				screen.refresh()

		# Main cycle - GUI mode
		saved = (sys.stdout, sys.stderr, utils.ActivityLog)
		try:
			utils.ActivityLog = CursesLog
			sys.stdout = CursesStream(saved[0], screen)
			sys.stderr = CursesStream(saved[1], screen)
			jobCycle()
		finally:
			if sys.modules['__main__'].log: del sys.modules['__main__'].log
			sys.stdout, sys.stderr, utils.ActivityLog = saved
	try:
		curses.wrapper(cursesWrapper)
	finally:
		CursesStream.dump()
