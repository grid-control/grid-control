#!/usr/bin/env python
import sys, os, signal, optparse, curses

# add python subdirectory from where go.py was started to search path
root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.insert(0, os.path.join(root, 'python'))

# and include grid_control python module
from grid_control import *
from time import sleep
utils.atRoot.root = root

def print_help(*args):
	sys.stderr.write("Syntax: %s [OPTIONS] <config file>\n\n" % sys.argv[0])
	sys.stderr.write(open(utils.atRoot('share', 'help.txt'), 'r').read())
	sys.exit(0)

_verbosity = 0

def main(args):
	global opts, log, handler
	log = None

	# display the 'grid-control' logo and version
	print open(utils.atRoot('share', 'logo.txt'), 'r').read()
	try:
		ver = popen2.popen3('svnversion %s' % _root)[0].read().strip()
		if ver != '':
			print 'Revision: %s' % ver
	except:
		pass
	pyver = reduce(lambda x,y: x+y/10., sys.version_info[:2])
	if pyver < 2.3:
		utils.deprecated("This python version (%.1f) is not supported anymore!" % pyver)

	parser = optparse.OptionParser(add_help_option=False)
	parser.add_option("-h", "--help",          action="callback", callback=print_help),
	parser.add_option(""  , "--help-vars",     dest="help_vars",  default=False, action="store_true")
	parser.add_option(""  , "--help-conf",     dest="help_cfg",   default=False, action="store_true")
	parser.add_option(""  , "--help-confmin",  dest="help_scfg",  default=False, action="store_true")
	parser.add_option("-s", "--no-submission", dest="submission", default=True,  action="store_false")
	parser.add_option("-q", "--requery",       dest="resync",     default=False, action="store_true")
	parser.add_option("-i", "--init",          dest="init",       default=False, action="store_true")
	parser.add_option("-c", "--continuous",    dest="continuous", default=False, action="store_true")
	parser.add_option("-G", "--gui",           dest="gui",        default=False, action="store_true")
	parser.add_option("-r", '--report',        dest="report",     default=False, action="store_true")
	parser.add_option("-v", "--verbose",       dest="verbosity",  default=0,     action="count")
	parser.add_option("-R", '--site-report',   dest="reportSite", default=0,     action="count")
	parser.add_option("-T", '--time-report',   dest="reportTime", default=0,     action="count")
	parser.add_option("-M", '--module-report', dest="reportMod",  default=0,     action="count")
	parser.add_option("-m", '--max-retry',     dest="maxRetry",   default=None,  type="int")
	parser.add_option("-d", '--delete',        dest="delete",     default=None)
	parser.add_option("-S", '--seed',          dest="seed",       default=None)
	(opts, args) = parser.parse_args()
	utils.verbosity.setting = opts.verbosity
	opts.abort = False

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		sys.stderr.write("Config file not specified!\n")
		sys.stderr.write("Syntax: %s [OPTIONS] <config file>\n" % sys.argv[0])
		sys.stderr.write("Use --help to get a list of options!\n")
		sys.exit(0)

	# set up signal handler for interrupts
	def interrupt(sig, frame):
		global opts, log, handler
		opts.abort = True
		log = utils.ActivityLog('Quitting grid-control! (This can take a few seconds...)')
		signal.signal(signal.SIGINT, handler)
	handler = signal.signal(signal.SIGINT, interrupt)

	# big try... except block to catch exceptions and print error message
	try:
		# try to open config file
		try:
			open(args[0], 'r')
			config = Config(args[0])
			config.opts = opts
		except IOError, e:
			raise ConfigError("Error while reading configuration file '%s'!" % args[0])

		# Check work dir validity (default work directory is the config file name)
		config.workDir = config.getPath('global', 'workdir', config.workDirDefault)
		if not os.path.exists(config.workDir):
			if utils.boolUserInput("Do you want to create the working directory %s?" % config.workDir, True):
				os.mkdir(config.workDir)
		try:
			os.chdir(config.workDir)
		except:
			raise UserError("Could not access specified working directory '%s'!" % config.workDir)

		backend = config.get('global', 'backend', 'grid')

		# Initialise proxy
		defaultproxy = { 'grid': 'VomsProxy', 'local': 'TrivialProxy' }
		proxy = Proxy.open(config.get(backend, 'proxy', defaultproxy[backend]))

		# Initialise application module
		module = config.get('global', 'module')
		module = Module.open(module, config, proxy)

		# Give help about variables
		if opts.help_vars:
			Help().listVars(module)
			return 0

		# Initialise workload management interface
		defaultwms = { 'grid': 'GliteWMS', 'local': 'LocalWMS' }
		if backend == 'grid':
			wms = WMS.open(config.get(backend, 'wms', 'GliteWMS'), config, module)
		elif backend == 'local':
			wms = WMS.open(defaultwms[backend], config, module)
		else:
			raise UserError("Invalid backend specified!" % config.workDir)

		# Initialise job database
		jobs = JobDB(config, module)

		# Give config help
		if opts.help_cfg or opts.help_scfg:
			Help().getConfig(config, opts.help_cfg)
			return 0

		# If invoked in report mode, just show report and exit
		if Report(jobs, jobs).show(opts, module):
			return 0

		# Check if jobs have to be deleted and exit
		if opts.delete != None:
			jobs.delete(wms, opts.delete)
			return 0

		if opts.continuous and not opts.gui:
			print
			Report(jobs, jobs).summary()
			print "Running in continuous mode. Press ^C to exit."

		if not proxy.canSubmit(module.wallTime, opts.submission):
			opts.submission = False

		# Job submission loop
		def wait(timeout):
			shortStep = map(lambda x: (x, 1), xrange(max(timeout - 5, 0), timeout))
			for x, w in map(lambda x: (x, 5), xrange(0, timeout - 5, 5)) + shortStep:
				if opts.abort:
					return False
				log = utils.ActivityLog('waiting for %d seconds' % (timeout - x))
				sleep(w)
				del log
			return True

		def jobCycle():
			didWait = False
			# Check free disk space
			if int(os.popen("df -P -m %s" % config.workDir).readlines()[-1].split()[3]) < 10:
				raise RuntimeError("Not enough space left in working directory")

			# check for jobs
			if not opts.abort and jobs.check(wms):
				didWait = wait(wms.getTimings()[1])
			# retrieve finished jobs
			if not opts.abort and jobs.retrieve(wms):
				didWait = wait(wms.getTimings()[1])
			# try submission
			if not opts.abort and jobs.submit(wms):
				didWait = wait(wms.getTimings()[1])

			# quit if abort flag is set or not in continuous mode
			if opts.abort or not opts.continuous:
				return False
			# idle timeout
			wait(wms.getTimings()[0])
			# Check proxy lifetime
			if not proxy.canSubmit(module.wallTime, opts.submission):
				opts.submission = False
			return True

		if opts.gui:
			def cursesWrapper(screen):
				screen.scrollok(True)
				screen.attron(curses.A_BOLD)

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
						Report(jobs, jobs).summary(message)
						sys.stdout.logged = True
						screen.move(*oldpos)
						screen.refresh()

				# Main cycle - GUI mode
				try:
					utils.ActivityLog = CursesLog
					saved = (sys.stdout, sys.stderr)
					sys.stdout = utils.CursesStream(saved[0], screen)
					sys.stderr = utils.CursesStream(saved[1], screen)
					while jobCycle(): pass
				finally:
					global log
					if log: del log
					sys.stdout, sys.stderr = saved
			try:
				curses.wrapper(cursesWrapper)
			finally:
				utils.CursesStream.dump()
		else:
			# Main cycle - non GUI mode
			while jobCycle(): pass

	except GridError, e:
		e.showMessage()
		return 1

	return 0

# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
