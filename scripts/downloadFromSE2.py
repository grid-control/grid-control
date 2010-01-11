#!/usr/bin/env python
import gcSupport, sys, os, optparse, popen2
from grid_control import *

def md5sum(filename):
	m = utils.md5()
	# use 4M blocksize:
	blocksize = 4096 * 1024
	f = open(filename, 'r')
	while True:
		s = f.read(blocksize)
		m.update(s)
		if len(s) != blocksize:
			break
	return m.hexdigest()


# Use url_* functions from run.lib (just like the job did...)
def se_rm(target, quiet = False):
	target = target.replace('dir://', 'file://')
	runLib = utils.atRoot(os.path.join('share', 'run.lib'))
	cmd = 'print_and_qeval "url_rm" "%s"' % target
	proc = popen2.Popen4('source %s || exit 1; %s' % (runLib, cmd), True)
	se_rm.lastlog = proc.fromchild.read()
	return proc.wait() == 0


def main(args):
	help = \
"""
DEFAULT: The default is to check the files with MD5 hashes. The default
         output directory is named "se_output" and located in the work
         directory of the job
  * For jobs with verified output files, the files are moved to the
    local SE output directory, and the job itself is marked as downloaded.
  * Jobs failing verification are marked as FAILED and their files are
    deleted from the SE and local SE output directory."""
	parser = optparse.OptionParser(usage = "%prog [options] <config file>\n" + help)
	parser.add_option("-m", "--no-md5",        dest="verify",       default=True,  action="store_false",
		help = "disable MD5 verification of SE files (all jobs are ok)")
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
	parser.add_option("-r", "--rm-local-ok",   dest="rmLocalOK",    default=False, action="store_true",
		help = "remove files of successful jobs from local directory")
	parser.add_option("-o", '--output',        dest="output",       default=None,
		help = "specify the local output directory")
	parser.add_option("-u", '--update',        dest="update",       default=False, action="store_true",
		help = "update, i.e. download only files that do not already exist")

	justDownloadOpts = "-d -f -k --keep-se-fail --keep-local-fail"
	parser.add_option("-j", '--just-download', dest="justDownload", default=False, action="store_true",
		help = "Just download files - shorthand for %s" % justDownloadOpts)

	(opts, args) = parser.parse_args()
	if opts.justDownload:
		parser.parse_args(args = justDownloadOpts.split() + sys.argv[1:], values = opts)
	realmain(opts, args)

def realmain(opts, args):
	# we need exactly one positional argument (config file)
	if len(args) != 1:
		sys.stderr.write("usage: %s [options] <config file>\n\n" % os.path.basename(sys.argv[0]))
		sys.stderr.write("Config file not specified!\n")
		sys.stderr.write("Use --help to get a list of options!\n")
		sys.exit(0)

	(workDir, jobList) = gcSupport.getWorkJobs(args)

	# Create SE output dir
	if not opts.output:
		opts.output = os.path.join(workDir, 'se_output')
	opts.output = os.path.abspath(opts.output)
	if not os.path.exists(opts.output):
		os.mkdir(opts.output)

	infos = {}
	def incInfo(x):
		infos[x] = infos.get(x, 0) + 1

	for jobNum in utils.sorted(jobList):
		print "Job %d:" % jobNum,

		# Only run over finished and not yet downloaded jobs
		try:
			jobFile = os.path.join(workDir, 'jobs', 'job_%d.txt' % jobNum)
			job = Job.load(jobFile)
		except:
			print "Could not load job status file %s!" % jobFile
			continue
		if job.state != Job.SUCCESS:
			print "Job has not yet finished successfully!"
			incInfo("Processing")
			continue
		if job.get('download') == 'True':
			print "All files already downloaded!"
			incInfo("Downloaded")
			continue

		# Read the file hash entries from job info file
		files = gcSupport.getFileInfo(workDir, jobNum, lambda retCode: retCode == 0)
		if not files:
			incInfo("No files")
			continue
		print "The job wrote %d file%s to the SE" % (len(files), ('s', '')[len(files) == 1])

		failJob = False
		for (hash, name_local, name_dest, pathSE) in files:
			print "\t", name_dest,

			# Copy files to local folder
			outFilePath = os.path.join(opts.output, name_dest)
			if opts.update and os.path.exists(outFilePath): 
				print "skip file as it already exists!"
				continue
				
			if not utils.se_copy(os.path.join(pathSE, name_dest), "file:///%s" % outFilePath):
				print "\n\t\tUnable to copy file from SE!"
				sys.stderr.write(utils.se_copy.lastlog)
				failJob = True
				break

			# Verify => compute md5hash
			if opts.verify:
				try:
					hashLocal = md5sum(outFilePath)
				except:
					print ""
					hashLocal = None
				print "=>", ('\33[0;91mFAIL\33[0m', '\33[0;92mMATCH\33[0m')[hash == hashLocal]
				print "\t\tRemote site:", hash
				print "\t\t Local site:", hashLocal
				if hash != hashLocal:
					failJob = True
			else:
				print
				print "\t\tRemote site:", hash

		for (hash, name_local, name_dest, pathSE) in files:
			# Remove downloaded files in case of failure
			if (failJob and opts.rmLocalFail) or (not failJob and opts.rmLocalOK):
				localPath = os.path.join(opts.output, name_dest)
				if os.path.exists(localPath):
					if not se_rm("file://%s" % localPath):
						print "\t\tUnable to remove local file!"
						sys.stderr.write(se_rm.lastlog)
			# Remove SE files in case of failure
			if (failJob and opts.rmSEFail)    or (not failJob and opts.rmSEOK):
				if not se_rm(os.path.join(pathSE, name_dest)):
					print "\t\tUnable to remove SE file!"
					sys.stderr.write(se_rm.lastlog)

		if failJob:
			incInfo("Failed downloads")
			if opts.markFailed:
				# Mark job as failed to trigger resubmission
				job.state = Job.FAILED
		else:
			incInfo("Sucessful download")
			if opts.markDownload:
				# Mark as downloaded
				job.set('download', 'True')

		# Save new job status infos
		job.save(jobFile)
		print

	# Print overview
	print
	print "Status overview:"
	for (state, num) in infos.items():
		if num > 0:
			print "%20s: [%d/%d]" % (state, num, len(jobList))
	print
	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
