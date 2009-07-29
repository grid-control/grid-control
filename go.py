#!/usr/bin/env python
import sys, os, signal, optparse

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.insert(0, os.path.join(_root, 'python'))

# and include grid_control python module
from grid_control import *
from time import sleep

def print_help(*args):
	sys.stderr.write("Syntax: %s [OPTIONS] <config file>\n\n" % sys.argv[0])
	sys.stderr.write(open(utils.atRoot('share', 'help.txt'), 'r').read())
	sys.exit(0)

_verbosity = 0

def main(args):
	global opts, log, handler

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
	parser.add_option("-r", '--report',        dest="report",     default=False, action="store_true")
	parser.add_option("-v", "--verbose",       dest="verbosity",  default=0,     action="count")
	parser.add_option("-R", '--site-report',   dest="reportSite", default=0,     action="count")
	parser.add_option("-T", '--time-report',   dest="reportTime", default=0,     action="count")
	parser.add_option("-M", '--module-report', dest="reportMod",  default=0,     action="count")
	parser.add_option("-m", '--max-retry',     dest="maxRetry",   default=None,  type="int")
	parser.add_option("-d", '--delete',        dest="delete",     default=None)
	parser.add_option("-S", '--seed',          dest="seed",       default=None)
	(opts, args) = parser.parse_args()
	sys.modules['__main__']._verbosity = opts.verbosity
	opts.abort = False

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		sys.stderr.write("Config file not specified!\n")
		sys.stderr.write("Syntax: %s [OPTIONS] <config file>\n" % sys.argv[0])
		sys.stderr.write("Use --help to get a list of options!\n")
		sys.exit(0)
	opts.confName = str.join("", os.path.basename(args[0]).split(".")[:-1])

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
		except IOError, e:
			raise ConfigError("Error while reading configuration file '%s'!" % args[0])

		# Check work dir validity (default work directory is the config file name)
		workDirDefault = os.path.join(config.baseDir, 'work.%s' % opts.confName)
		opts.workDir = config.getPath('global', 'workdir', workDirDefault)
		if not os.path.exists(opts.workDir):
			if utils.boolUserInput("Do you want to create the working directory %s?" % opts.workDir, True):
				os.mkdir(opts.workDir)
		try:
			os.chdir(opts.workDir)
		except:
			raise UserError("Could not access specified working directory '%s'!" % opts.workDir)

		backend = config.get('global', 'backend', 'grid')

		# Initialise proxy
		defaultproxy = { 'grid': 'VomsProxy', 'local': 'TrivialProxy' }
		proxy = Proxy.open(config.get(backend, 'proxy', defaultproxy[backend]))

		# Initialise application module
		module = config.get('global', 'module')
		module = Module.open(module, config, opts, proxy)
		utils.vprint('Current task ID %s' % module.taskID, -1)

		# Give help about variables
		if opts.help_vars:
			Help().listVars(module)
			return 0

		# Initialise workload management interface
		defaultwms = { 'grid': 'GliteWMS', 'local': 'LocalWMS' }
		if backend == 'grid':
			wms = WMS.open(config.get(backend, 'wms', 'GliteWMS'), config, opts, module)
		elif backend == 'local':
			wms = WMS.open(defaultwms[backend], config, opts, module)
		else:
			raise UserError("Invalid backend specified!" % opts.workDir)

		# Initialise job database
		jobs = JobDB(config, opts, module)

		# Give config help
		if opts.help_cfg or opts.help_scfg:
			Help().getConfig(config, opts.help_cfg)
			return 0

		# If invoked in report mode, just show report and exit
		if Report(jobs, jobs).show(opts, module):
			return 0

		# Check if jobs have to be deleted and exit
		if opts.delete != None:
			jobs.delete(wms, opts)
			return 0

		if not proxy.canSubmit(module.wallTime, opts.submission):
			opts.submission = False

		# Check if running in continuous mode
		if opts.continuous:
			print "Running in continuous mode with job submission %s. Press ^C to exit." % ("disabled", "enabled")[opts.submission]

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

		while True:
			didWait = False
			# Check free disk space
			if int(os.popen("df -P -m %s" % opts.workDir).readlines()[-1].split()[3]) < 10:
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
				break
			# idle timeout
			wait(wms.getTimings()[0])
			# Check proxy lifetime
			if not proxy.canSubmit(module.wallTime, opts.submission):
				opts.submission = False

	except GridError, e:
		e.showMessage()
		return 1

	return 0

# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
