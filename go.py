#!/usr/bin/env python
import sys, os, signal, time, getopt

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.append(os.path.join(_root, 'python'))
# and include grid_control python module
from grid_control import *


def syntax(out):
	out.write("Syntax: %s [OPTIONS] <config file>\n\n"
	          "    Options:\n"
	          "\t-h, --help               Show this helpful message\n"
	          "\t-i, --init               Initialise working directory\n"
	          "\t-c, --continuous         Run in continuous mode\n"
	          "\t-s, --no-submission      Disable job submission\n"
	          "\n" % sys.argv[0])


def main(args):
	global continuous

	# display the 'grid-control' logo
	logoFile = os.path.join(_root,'share','logo.txt')
	try:
		f = open(logoFile, 'r')
		print f.read()
		f.close()
	except:
		print ("WARNING: The logofile '%s' could not be read!" % logoFile)

	# set up signal handler for interrupts
	def interrupt(sig, frame):
		global continuous
		continuous = False
	signal.signal(signal.SIGINT, interrupt)

	longOptions = ['help', 'init', 'continuous', 'no-submission']
	shortOptions = 'hics'

	# global variables
	continuous = False
	init = False
	jobSubmission = True

	# let getopt dig through the options
	try:
		opts, args = getopt.getopt(args, shortOptions, longOptions)
	except getopt.GetoptError:
		# fail if an invalid option or missing argument was found
		syntax(sys.stderr)
		return 1

	# process options
	for opt, arg in opts:
		if opt in ('-h', '--help'):
			syntax(sys.stdout)
			return 0
		elif opt in ('-i', '--init'):
			init = True
		elif opt in ('-c', '--continuous'):
			continuous = True
		elif opt in ('-s', '--no-submission'):
			jobSubmission = False

	# we need exactly one config file argument
	if len(args) != 1:
		syntax(sys.stderr)
		return 1
	configFile = args[0]

	# big try... except block to catch exceptions and print error message
	try:

		# try to open config file
		try:
			f = open(configFile, 'r')
		except IOError, e:
			raise ConfigError("Configuration file '%s' not found!" % configFile)

		config = Config(f)
		f.close()

		# Check work dir validity
		workdir = config.getPath('global', 'workdir')
		if os.path.exists(workdir):
			print "Using working directory: %s" % workdir
		else:
			raise UserError("The specified working directory '%s' does not exist!" % workdir) 

		# Open grid proxy
		proxy = config.get('grid', 'proxy')
		proxy = Proxy.open(proxy)
		if proxy.critical():
			raise UserError('Your proxy only has %d seconds left!' % proxy.timeleft())

		# Test grid proxy lifetime
		wallTime = config.getInt('jobs', 'wall time')
		if not proxy.check(wallTime * 60 * 60):
			print >> sys.stderr, "Proxy lifetime (%d seconds) does not meet the walltime requirements of %d hours (%d seconds)!\n" \
			                     "INFO: Disabling job submission." \
			                     % (proxy.timeleft(), wallTime, wallTime * 60 * 60)
			jobSubmission = False

		# Load the application module
		module = config.get('global', 'module')
		module = Module.open(module, config)

		# Initialise workload management interface
		wms = config.get('grid', 'wms')
		wms = WMS.open(wms, config, module)

		# Initialise job database
		jobs = JobDB(workdir, init)

		nJobs = config.get('jobs', 'jobs')
		inFlight = config.get('jobs', 'in flight')
		while True:
			print "Iterating..."

			wms.makeJDL(sys.stdout, 0)

			if not continuous:
				break
			time.sleep(5)
			if not continuous:
				break

			# Retest grid proxy lifetime
			if jobSubmission and not proxy.check(wallTime * 60 * 60):
				print >> sys.stderr, "Proxy lifetime (%d seconds) does not meet the walltime requirements of %d hours (%d seconds)!\n" \
				                     "INFO: Disabling job submission." \
			                     % (proxy.timeleft(), wallTime, wallTime * 60 * 60)
			jobSubmission = False

	except GridError, e:
		e.showMessage()
		return 1

	# everything seems to be in order
	return 0


# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
