#!/usr/bin/env python
import sys, os, signal, getopt

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.insert(0, os.path.join(_root, 'python'))

# and include grid_control python module
from grid_control import *
import time

def syntax(out):
	out.write("Syntax: %s [OPTIONS] <config file>\n\n"
			"    Options:\n"
			"\t-h, --help               Show this helpful message\n"
			"\t-i, --init               Initialise working directory\n"
			"\t-c, --continuous         Run in continuous mode\n"
			"\t-s, --no-submission      Disable job submission\n"
			"\t-m, --max-retry <args>   Set maximum number of job resubmission attempts\n"
			"\t                         Default is to resubmit indefinitely\n"
			"\t                            -m 0 (Disable job REsubmission)\n"
			"\t                            -m 5 (Resubmit jobs up to 5 times)\n"
			"\t-r, --report             Show status report of jobs\n"
			"\t-S, --seed <args>        Override seed specified in the config file e.g:\n"
			"\t                            -S 1234,423,7856\n"
			"\t                            -S (= generate 10 random seeds)\n"
			"\t-d, --delete <args>      Delete given jobs, e.g:\n"
			"\t                            -d 1,5,9,...  (JobNumbers)\n"
			"\t                            -d QUEUED,... (JobStates)\n"
			"\t                            -d TODO (= SUBMITTED,WAITING,READY,QUEUED)\n"
			"\t                            -d ALL\n"
			"\n" % sys.argv[0])


def main(args):
	global continuous

	# display the 'grid-control' logo
	print open(utils.atRoot('share', 'logo.txt'), 'r').read()
	print ('$Revision$'.strip('$'))

	# set up signal handler for interrupts
	def interrupt(sig, frame):
		global continuous
		continuous = False
	signal.signal(signal.SIGINT, interrupt)

	longOptions = ['help', 'init', 'continuous', 'no-submission', 'max-retry', 'report', 'delete', 'seed']
	shortOptions = 'hicsrm:d:S:'

	# global variables
	continuous = False
	init = False
	jobSubmission = True
	maxRetry = None
	report = False
	delete = None
	seed = False
	seedarg = None

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
		elif opt in ('-m', '--max-retry'):
			maxRetry = int(arg)
		elif opt in ('-r', '--report'):
			report = True
		elif opt in ('-d', '--delete'):
			delete = arg
		elif opt in ('-S', '--seed'):
			seed = True
			seedarg = arg

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

		# change to workdir
		try:
			os.chdir(workdir)
		except:
			raise UserError("The specified working directory '%s' is inaccessible!" % workdir)

		# Initialise application module
		module = config.get('global', 'module')
		module = Module.open(module, config, init)
		if seed:
			module.setSeed(seedarg)

		# Initialise workload management interface
		backend = config.get('global', 'backend', 'grid')
		wms = config.get(backend, 'wms')
		wms = WMS.open(wms, config, module, init)

		# Initialise proxy
		proxy = config.get(backend, 'proxy', 'TrivialProxy')
		proxy = Proxy.open(proxy)
		if proxy.critical():
			raise UserError('Your proxy only has %d seconds left!' % proxy.timeleft())

		# Test grid proxy lifetime
		wallTime = config.getInt('jobs', 'wall time')
		if not proxy.check(wallTime * 60 * 60):
			proxy.warn(wallTime)
			jobSubmission = False

		# Initialise job database
		try:
			nJobs = config.getInt('jobs', 'jobs')
		except:
			nJobs = module.getMaxJobs()
			if nJobs == None:
				raise

		maxInFlight = config.getInt('jobs', 'in flight')
		jobs = JobDB(workdir, nJobs, module, init)
		# If invoked in report mode, scan job database and exit
		if report:
			report = Report(jobs, jobs)
			report.details()
			report.summary()
			return 0

		# Check if jobs have to be deleted and exit
		if delete != None:
			jobs.delete(wms, delete)
			return 0

		# Check if running in continuous mode
		if continuous:
			Report(jobs, jobs).summary()
			print "Running in continuous mode. Press ^C to exit."

		# Job submission loop
		while True:
			# idle timeout is one minute
			timeout = 60

			# retrieve finished jobs
			if jobs.retrieve(wms):
				timeout = 10

			# check for jobs
			if jobs.check(wms):
				timeout = 10

			# try submission
			if jobSubmission:
				jobList = jobs.getSubmissionJobs(config.getInt('jobs', 'in flight'), maxRetry)
				if len(jobList):
					jobs.submit(wms, jobList)
				del jobList

			if not continuous:
				break
			time.sleep(timeout)
			if not continuous:
				break

			# Retest grid proxy lifetime
			if jobSubmission and not proxy.check(wallTime * 60 * 60):
				proxy.warn(wallTime)

	except GridError, e:
		e.showMessage()
		return 1

	# everything seems to be in order
	return 0


# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
