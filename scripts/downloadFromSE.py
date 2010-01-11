#!/usr/bin/env python
import sys, optparse
from downloadFromSE2 import realmain

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
	parser.add_option("-u", '--update',        dest="skipExisting", default=False, action="store_true",
		help = "update, i.e. download only files that do not already exist")

	justDownloadOpts = "-d -f -k --keep-se-fail --keep-local-fail"
	parser.add_option("-j", '--just-download', dest="justDownload", default=False, action="store_true",
		help = "Just download files - shorthand for %s" % justDownloadOpts)

	(opts, args) = parser.parse_args()
	if opts.justDownload:
		parser.parse_args(args = justDownloadOpts.split() + sys.argv[1:], values = opts)
	realmain(opts, args)

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
