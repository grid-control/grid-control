import re, sys, signal, curses, utils, report

class CursesStream:
	def __init__(self, *args):
		(self.stream, self.screen) = args
		self.logged = True

		curses.init_pair(1, curses.COLOR_RED, -1)
		curses.init_pair(2, curses.COLOR_GREEN, -1)

		self.attrs = {
				'FAILED': curses.color_pair(1) | curses.A_BOLD,
				'SUCCESS': curses.color_pair(2) | curses.A_BOLD
		}

	def write(self, data):
		if self.logged:
			CursesStream.backlog.pop(0)
			CursesStream.backlog.append(data)

		if curses.has_colors():
			regex = '(%s)' % '|'.join(self.attrs.keys())
			start = 0
			match = re.search(regex, data[start:])
			while match:
				self.screen.addstr(data[start:match.start()])
				self.screen.addstr(match.group(0), self.attrs[match.group(0)])
				start = match.end()
				match = re.search(regex, data[start:])
			self.screen.addstr(data[start:])
			self.screen.refresh()
		else:
			self.screen.addstr(data)

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
		except:
			screen.attron(curses.A_BOLD)

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
