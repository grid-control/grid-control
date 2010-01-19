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
THIS IS NOT UP-TO-DATE!
DEFAULT: The default is to check the files with MD5 hashes. The default
         output directory is named "se_output" and located in the work
         directory of the job
  * For jobs with verified output files, the files are moved to the
    local SE output directory, and the job itself is marked as downloaded.
  * Jobs failing verification are marked as FAILED and their files are
    deleted from the SE and local SE output directory."""
	parser = optparse.OptionParser(usage = "%prog [options] <config file>\n" + help)

	def addBoolOpt(optList, optPostfix, dest, default, help, optShort=("", ""), optPrefix=("no", ""), helpPrefix=("do not ", "")):
		def buildLongOpt(prefix, postfix):
			if prefix and postfix:
				return "--%s-%s" % (prefix, postfix)
			elif prefix and not postfix:
				return "--" + prefix
			else:
				return "--" + postfix
		optList.add_option(optShort[True], buildLongOpt(optPrefix[True], optPostfix), dest=dest,
			default=default, action="store_true", help=helpPrefix[True] + help + ("", " [Default]")[default])
		optList.add_option(optShort[False], buildLongOpt(optPrefix[False], optPostfix), dest=dest,
			default=default, action="store_false", help=helpPrefix[False] + help + (" [Default]", "")[default])

	addBoolOpt(parser, "verify-md5", dest="verify",       default=True, optShort=("", "-v"),
		help="MD5 verification of SE files", helpPrefix=("disable ", "enable "))
	addBoolOpt(parser, "",           dest="skipExisting", default=False, optPrefix=("overwrite", "skip-existing"),
		help="files which are already on local disk", helpPrefix=("overwrite ", "skip "))

	ogFlags = optparse.OptionGroup(parser, "Job state / flag handling", "")
	addBoolOpt(ogFlags, "mark-dl",   dest="markDL",       default=True,
		help="mark sucessfully downloaded jobs as such")
	addBoolOpt(ogFlags, "mark-dl",   dest="markIgnoreDL", default=False, optPrefix=("use","ignore"),
		help="mark about sucessfully downloaded jobs", helpPrefix=("use ", "ignore "))
	addBoolOpt(ogFlags, "mark-fail", dest="markFailed",   default=True,
		help="mark jobs failing verification as such")
	parser.add_option_group(ogFlags)

	ogFiles = optparse.OptionGroup(parser, "Local / SE file handling", "")
	for (optPostfix, dest, help, default) in [
			('local-ok',   'rmLocalOK',   'files of successful jobs in local directory', False),
			('local-fail', 'rmLocalFail', 'files of failed jobs in local directory', False),
			('se-ok',      'rmSEOK',      'files of successful jobs on SE', False),
			('se-fail',    'rmSEFail',    'files of failed jobs on the SE', False),
		]:
		addBoolOpt(ogFiles, optPostfix, dest=dest, default=default, optPrefix=("keep","rm"),
			help=help, helpPrefix=("keep ", "remove "))
	parser.add_option_group(ogFiles)

	parser.add_option("-o", "--output", dest="output", default=None,
		help="specify the local output directory")

	# Shortcut options
	def withoutDefaults(opts):
		def isDefault(opt):
			return (parser.get_option(opt).default and parser.get_option(opt).action == "store_true") or \
				(not parser.get_option(opt).default and parser.get_option(opt).action == "store_false")
		return str.join(" ", filter(lambda x: not isDefault(x), opts.split()))

	ogShort = optparse.OptionGroup(parser, "Shortcuts", "")
	optMove = "--verify-md5 --overwrite --mark-dl --use-mark-dl --mark-fail --rm-se-fail --rm-local-fail --rm-se-ok --keep-local-ok"
	ogShort.add_option("-m", "--move", dest="shMove", default=None, action="store_const", const=optMove,
		help = "Move files from SE - shorthand for:".ljust(100) + withoutDefaults(optMove))

	optCopy = "--verify-md5 --overwrite --mark-dl --use-mark-dl --mark-fail --rm-se-fail --rm-local-fail --keep-se-ok --keep-local-ok"
	ogShort.add_option("-c", "--copy", dest="shCopy", default=None, action="store_const", const=optCopy,
		help = "Copy files from SE - shorthand for:".ljust(100) + withoutDefaults(optCopy))

	optJCopy = "--verify-md5 --skip-existing --no-mark-dl --ignore-mark-dl --no-mark-fail --keep-se-fail --keep-local-fail --keep-se-ok --keep-local-ok"
	ogShort.add_option("-j", "--just-copy", dest="shJCopy", default=None, action="store_const", const=optJCopy,
		help = "Just copy files from SE - shorthand for:".ljust(100) + withoutDefaults(optJCopy))
	parser.add_option_group(ogShort)

	(opts, args) = parser.parse_args()
	def processShorthand(optSet):
		if optSet:
			parser.parse_args(args = optSet.split() + sys.argv[1:], values = opts)
	processShorthand(opts.shMove)
	processShorthand(opts.shCopy)
	processShorthand(opts.shJCopy)
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
		if job.get('download') == 'True' and not opts.markIgnoreDL:
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
			if opts.skipExisting and os.path.exists(outFilePath): 
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
			if opts.markDL:
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
