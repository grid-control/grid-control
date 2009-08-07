import sys, signal, curses, utils, report

class CursesStream:
	def __init__(self, *args):
		(self.stream, self.screen) = args
		self.logged = True

	def write(self, data):
		if self.logged:
			CursesStream.backlog.pop(0)
			CursesStream.backlog.append(data)
		self.screen.addstr(data)
		return True

	def __getattr__(self, name):
		return self.stream.__getattribute__(name)

	def dump(cls):
		for data in filter(lambda x: x, CursesStream.backlog):
			sys.stdout.write(data)
		sys.stdout.write('\n')	
	dump = classmethod(dump)
CursesStream.backlog = [None for i in xrange(100)]


def CursesGUI(jobs, jobCycle):
	def cursesWrapper(screen):
		screen.scrollok(True)
		curses.use_default_colors()

		# Event handling for resizing
		def onResize(sig, frame):
			oldy = screen.getyx()[0]
			curses.endwin()
			screen.refresh()
			(sizey, sizex) = screen.getmaxyx()
			screen.setscrreg(min(15, sizey - 2), sizey - 1)
			screen.move(min(sizey - 1, max(15, oldy)), 0)
		onResize(None, None)
		signal.signal(signal.SIGWINCH, onResize)

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
				report.Report(jobs, jobs).summary(message)
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
