#!/usr/bin/env python
import sys, os, optparse, popen2, gcSupport
from grid_control import *

def md5sum(filename):
	m = md5.new()
	# use 4M blocksize:
	blocksize = 4096 * 1024
	f = open(filename, 'r')
	while True:
		s = f.read(blocksize)
		m.update(s)
		if len(s) != blocksize:
			break
	return m.hexdigest()


def se_rm(target):
	target = target.replace('dir://', 'file://')
	runLib = utils.atRoot(os.path.join('share', 'run.lib'))
	proc = popen2.Popen4('source %s || exit 1; url_rm %s' % (runLib, target), True)
	result = proc.wait()
	if utils.verbosity() or (result != 0):
		sys.stderr.write(proc.fromchild.read())
	return result == 0


def main(args):
	parser = optparse.OptionParser(usage = "%prog [options] <config file>")
	parser.add_option("-m", "--no-md5",        dest="verify",       default=True,  action="store_false",
		help = "disable MD5 verification of SE files")
	parser.add_option("-d", "--no-mark-dl",    dest="markDownload", default=True,  action="store_false",
		help = "do not mark sucessfully downloaded jobs as such")
	parser.add_option("-f", "--no-mark-fail",  dest="markFailed",   default=True,  action="store_false",
		help = "do not mark jobs failing verification as such")

	parser.add_option("", "--keep-se-fail",    dest="rmSEFail",     default=True,  action="store_false",
		help = "keep files of failed jobs on the SE")
	parser.add_option("", "--keep-local-fail", dest="rmLocalFail",  default=True,  action="store_false",
		help = "keep files of failed jobs in local directory")
	parser.add_option("-k", "--keep-se-ok",    dest="rmSEOK",       default=True,  action="store_false",
		help = "keep files of successful jobs on SE")
	parser.add_option("", "--rm-local-ok",     dest="rmLocalOK",    default=False, action="store_true",
		help = "remove files of successful jobs from local directory")

	(opts, args) = parser.parse_args()

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		sys.stderr.write("Config file not specified!\n")
		sys.stderr.write("Syntax: %s [OPTIONS] <config file>\n" % sys.argv[0])
		sys.stderr.write("Use --help to get a list of options!\n")
		sys.exit(0)

	(workDir, pathSE, jobList) = gcSupport.getWorkSEJobs(args)

	# Create SE output dir
	seOutputDir = os.path.abspath(os.path.join(workDir, 'se_output'))
	if not os.path.exists(seOutputDir):
		os.mkdir(seOutputDir)

	for jobNum in utils.sorted(jobList):
		print "Job %d:" % jobNum,

		# Only run over finished and not yet downloaded jobs
		try:
			jobFile = os.path.join(workDir, 'jobs', 'job_%d.txt' % jobNum)
			job = Job.load(jobFile)
		except:
			print "Could not load job status file %s!" % jobFile
			continue
		print job.state
		if job.state != Job.SUCCESS:
			print "Job has not yet finished successfully!"
			continue
		if job.get('download') == 'True':
			print "Files already downloaded!"
			continue
		print

		# Read specified jobinfo.txt files
		jobInfo = gcSupport.getJobInfo(workDir, jobNum, lambda retCode: retCode == 0)
		if not jobInfo:
			continue

		# Just get the file hash entries from jobinfo.txt
		files = filter(lambda x: x[0].startswith('file'), jobInfo.items())
		files = map(lambda (x,y): tuple(y.strip('"').split('  ')), files)

		failJob = False
		for (hash, name_local, name_dest) in files:
			print "\t", name_dest,

			# Copy files to local folder
			outFilePath = os.path.join(seOutputDir, name_dest)
			utils.se_copy(os.path.join(pathSE, name_dest), "file://%s" % outFilePath)

			# Verify => compute md5hash
			if opts.verify:
				hashLocal = md5sum(outFilePath)
				print "=>", ('\33[0;91mFAIL\33[0m', '\33[0;94mMATCH\33[0m')[hash == hashLocal]
				print "\t\tRemote site:", hash
				print "\t\t Local site:", hashLocal
				if hash != hashLocal:
					failJob = True
			else:
				print
				print "\t\tRemote site:", hash
		print

		for (hash, name_local, name_dest) in files:
			# Remove downloaded files in case of failure
			if (failJob and opts.rmLocalFail) or (not failJob and opts.rmLocalOK):
				se_rm("file://%s" % os.path.join(seOutputDir, name_dest))
			# Remove SE files in case of failure
			if (failJob and opts.rmSEFail)    or (not failJob and opts.rmSEOK):
				se_rm("file://%s" % os.path.join(pathSE, name_dest))

		if failJob:
			if opts.markFailed:
				# Mark job as failed to trigger resubmission
				job.state = Job.FAILED
		else:
			if opts.markDownload:
				# Mark as downloaded
				job.set('download', 'True')

		# Save new job status infos
		job.save(jobFile)
	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
