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
		  "\t-r, --report	      Show status report of jobs\n"
	          "\n" % sys.argv[0])


def main(args):
	global continuous

	# display the 'grid-control' logo
	print open(utils.atRoot('share', 'logo.txt'), 'r').read()

	# set up signal handler for interrupts
	def interrupt(sig, frame):
		global continuous
		continuous = False
	signal.signal(signal.SIGINT, interrupt)

	longOptions = ['help', 'init', 'continuous', 'no-submission', 'report']
	shortOptions = 'hicsr'

	# global variables
	continuous = False
	init = False
	jobSubmission = True
	report = False

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
		elif opt in ('-r', '--report'):
			report = True

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
		module = Module.open(module, config, init)

		# Initialise workload management interface
		wms = config.get('grid', 'wms')
		wms = WMS.open(wms, config, module, init)

		# Initialise job database
		nJobs = config.getInt('jobs', 'jobs')
		maxInFlight = config.getInt('jobs', 'in flight')
		jobs = JobDB(workdir, nJobs, init)

		# If invoked in report mode, scan job database and exit
		if report:
			reports = []
			maxWidth = [0, 0, 0]

			# Get the maximum width of each column
			for id in jobs.all:
				job = jobs.get(id)
				thisreport = job.report()
				reports.append(thisreport)
				if maxWidth[0] < len(thisreport[0]):
					maxWidth[0] = len(thisreport[0])
				if maxWidth[1] < len(thisreport[1]):
					maxWidth[1] = len(thisreport[1])
				if maxWidth[2] < len(thisreport[2]):
					maxWidth[2] = len(thisreport[2])
			
			# Print table header
			print "\n%4s %*s %s" % ("Job", maxWidth[0], "Status", "Destination / Job ID")

			# Calculate width of horizontal lines
			if maxWidth[1] > maxWidth[2]:
				lineWidth = maxWidth[0]+maxWidth[1]+8
			else:
				lineWidth = maxWidth[0]+maxWidth[2]+6
			line = "-"*lineWidth
			print line

			# Calculate spacer width for second row
			spacer = " "*(maxWidth[0]+5)

			# Setup dict for the summary report
			summary = {}
			for category in job.states:
				summary[category] = 0.

			# Get information of each job
			for id in jobs.all:
				print "%4d %*s %s /" % (id, maxWidth[0], reports[id][0], reports[id][1])
				print "%s %-*s" % (spacer, maxWidth[0]+maxWidth[2]+6, reports[id][2])
				for category in job.states:
					if category == reports[id][0]:
						summary[category] = summary[category] + 1 
				
			print line + "\n"

			# Print report summary
			print "-----------------------------------------------------------------"
			print "REPORT SUMMARY:"
			print "---------------"
			
			print "Total number of jobs:      %4d   Number of successful jobs: %4d" % (id+1, summary["OK"])
			print "Number of unfinished jobs: %4d   Number of failed jobs:     %4d\n" % (summary["INIT"]+summary["READY"]+summary["WAITING"]
								   			             +summary["QUEUED"]+summary["SUBMITTED"], summary["ABORTED"]
												     +summary["CANCELLED"]+summary["FAILED"])
			print "Detailed Information:"
			for category in job.states:
				if summary[category] != 0:
					ratio = (summary[category] / (id + 1))*100
				else:
					ratio = 0
				print "Jobs   %9s: %4d     %3d%%" % (category, summary[category], round(ratio))
			
			print "-----------------------------------------------------------------\n"
		
			return 0

		# Check if running in continuous mode
		if continuous:
			print "Running in continuous mode. Press ^C to exit."

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
				curInFlight = len(jobs.running)
				submit = maxInFlight - curInFlight
				if submit < 0:
					submit = 0
				for job in jobs.ready[:submit]:
					jobs.submit(wms, job)

			if not continuous:
				break
			time.sleep(timeout)
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
