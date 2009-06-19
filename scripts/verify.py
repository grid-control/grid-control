#!/usr/bin/env python

import os, sys, os.path, re, md5

_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path = [os.path.join(_root, 'python')] + sys.path
from grid_control import *

output_pattern = ''


def md5sum(filename):
	m = md5.new()
	#use 4M blocksize:
	blocksize = 4096 * 1024
	f = open(filename, 'r')
	while True:
		s = f.read(blocksize)
		m.update(s)
		if(len(s)!=blocksize):
			break
	return m.hexdigest()

def find_file(filename, dirname):
	"""Returns the absolute filename if the filename part is filename and
	the directory dirname. 
	Reimplement this method for a more sophisticated file lookup.
	(dirname is just the string given as command line argument, so the meaning 
	can be something else in your implementation).

	For example, reimplementing it can implement a filter: Just return an empty 
	string to skip a certain file without reporting it as missing (to report it as missing,
	return a non-empty string of a non-existent file).

	Can also be used to implement searching for the file in multiple directories
	or some other more complex file-lookup scheme.

	Default implementation would be something like
	return os.path.join(dirname, filename)"""
#	if not filename.endswith(".root"):
#		return ""
	return os.path.join(dirname, filename)

def _verify_job(jobid, workdir, dirname, verbose=False):
	"""Verifies the files for the given job id. 
	Returns a dictionary that contains all the filenames as keys as described in 
	verify(). If an error occurs while parsing job output or job output is not 
	found, returns a string describing the error.

	If verbose is True, prints the jobid, the filename (as on the ce) and the test result
	for each file to stdout.
	In case of errors while looking for the job output or
	the md5sums in the job output, it prints the jobid and the string 'SKIP'."""
	#find md5sum-section in job-output:
	stdout_filename = os.path.join(workdir, 'output' + os.path.sep+ ('job_%d' % jobid) + os.path.sep + 'stdout.txt')
	if not os.path.isfile(stdout_filename):
		if verbose:
			print "%d SKIP" % jobid
		return "Job output file (%s) not found" % stdout_filename
	f = open(stdout_filename, 'r')
	in_md5block = False
	result = {}
	md5_regex = re.compile(r"^([0-9a-f]{32})\s+(.+)$")
	global output_pattern
	for line in f:
#		if not in_md5block and line.startswith("CMSSW working directory after cmsRun cont"):
		if not in_md5block and line.lower().find('md5-sums') > -1:
			in_md5block = True
			continue
		if in_md5block:
#			parts = line.split()
#			if len(parts)<8:
#				if len(parts) == 2 and parts[0] == 'total':
#					continue
#				break
#			filename_on_ce = parts[8]
#			if not filename_on_ce.endswith(".root"):
#				continue
#			size_on_ce = int(parts[4])
#			filename_local = output_pattern.replace('__MY_JOB__', str(jobid))
#			filename_local = filename_local.replace('__X__', filename_on_ce)
#			filename_local_abs = find_file(filename_local, dirname)
#			if not os.path.isfile(filename_local_abs):
#				result[filename_on_ce] = 'MISS'
#				continue
#			filename_local = filename_local_abs
#			size_local = os.stat(filename_local_abs).st_size
#			if size_local == size_on_ce:
#				result[filename_on_ce] = 'MATCH'
#			else:
#				result[filename_on_ce] = 'NOMATCH'
#			continue

			#we are now in the section where the md5-sums are listed. So match out the md5sum and filename
			# from the subsequent lines as long as possible. Filenames with whitespace at
			# the beginning are not treated correctly, but that should not be a major issue ...
			match = md5_regex.match(line)
			if match==None:#line could not be matched, we are out of the md5-sums block ...
				break
			md5sum_on_ce = match.group(1)
			filename_on_ce = match.group(2)
			filename_local = output_pattern.replace('__MY_JOB__', str(jobid))
			filename_local = filename_local.replace('__X__', filename_on_ce)
			filename_local_abs = find_file(filename_local, dirname)
			if filename_local_abs=='':
				continue
			if not os.path.isfile(filename_local_abs):
				result[filename_on_ce] = 'MISS'
				if verbose:
					print "%d %s MISS"  % (jobid, filename_on_ce)
				continue
			filename_local = filename_local_abs
			md5sum_local = md5sum(filename_local)
			if md5sum_local == md5sum_on_ce:
				result[filename_on_ce] = 'MATCH'
				if verbose:
					print "%d %s MATCH"  % (jobid, filename_on_ce)
			else:
				result[filename_on_ce] = 'NOMATCH'
				if verbose:
					print "%d %s NOMATCH"  % (jobid, filename_on_ce)
	else:#executed if no break was hit:
		f.close()
		if verbose:
			print "%d SKIP" % jobid
		return "No md5-sums in output file (%s) found" % stdout_filename
	f.close()
	return result


def job_fail(jobfile):
	"""Alters job status from success to failed."""
	fh = open(jobfile, 'r')
	file_contents = fh.read()
	fh.close()
	file_contents = file_contents.replace('SUCCESS', 'FAILED', 1)
	fh = open(jobfile, 'w')
	fh.truncate()
	fh.write(file_contents)
	fh.close()


def verify(config, dirname, jobs='success', quiet=False, set_to_failed=False):
	"""Parameters:
	- config is a grid_control Config Object or a filename of a valid configuration file.
	- dirname is the directory containing the output of the grid job.
	- jobs is either a list of job ids to check or a string
	  in which case all jobs with status "SUCCESS" are checked (independently of the string contents).
	- quiet=True turns off reporting to stdout, else a report constisting of one line per job is 
	  printed to stdout containing the jobid, the number of matching, missing and failed files.
	- set_to_failed=True sets all jobs with missing or non-matching files to "FAILED" causing
	  grid-control on the next run to re-submit (do not use this option while gc
	  is running and possibly overwriting those files again).

	Parses the output of the jobs (as found in working directory). Considered
	are all files listed in the MD5-Sum section of stdout.txt. The configured 
	"se output pattern" is applied and the resulting filename is appended to the 
	directory name given by dirname. Each of the filenames thus constructed is 
	checked for existence and matching md5sum.

	Returns: a dictionary with job number as index. Each entry is
	in turn a dictionary ("job report") with filenames as keys and a description of
	what went wrong as values. The start of each string can be interpreted
	programatically if necessary. 
	- "MISS file is missing" if a file is listed in the md5sum list but was not found
	   in the given directory.
	- "NOMATCH md5sums do not match"
	- "MATCH md5sums match"
	
	There is a special key "_summary" in the job report
	which points to a dictionary containing:
	- "n_miss": number of missing files
	- "n_nomatch": number of NOMATCH-files
	- "n_match": number of matching files.
	- "error": containing an error message (in case the job output could not be parsed etc.). This
		variable should ALWAYS be checked. In case of no error, it is the empty string.
	"""

	if type(config)==type(""):
		try:
			config = Config(config)
		except IOError, e:
			raise ConfigError("Configuration file '%s' not found!" % configFile)

	workdir = config.getPath("global", "workdir")
	#construct job_list containing all job numbers to be verified.
	if type(jobs)==type(""):
		jobid_list = get_jobids_success(workdir)
	else:
		jobid_list = jobs
	global output_pattern 
	output_pattern = config.get('storage', 'se output pattern')
	output_pattern = output_pattern.strip()
	result = {}
	for jobid in jobid_list:
		result[jobid] = _verify_job(jobid, workdir, dirname)
		if type(result[jobid]) == type(""):
			result[jobid] = dict(_summary=dict(error=result[jobid]))
			continue
		sumdict = {}
		result[jobid]['_summary'] = sumdict
		sumdict['n_miss'] = 0
		sumdict['n_nomatch'] = 0
		sumdict['n_match'] = 0
		sumdict['error'] = ''
		for filename in result[jobid]:
			if filename.startswith('_'):
				continue
			if result[jobid][filename].startswith('MISS'):
				sumdict['n_miss'] += 1
			elif result[jobid][filename].startswith('NOMATCH'):
				sumdict['n_nomatch'] += 1
			elif result[jobid][filename].startswith('MATCH'):
				sumdict['n_match'] += 1
			else:
				raise RuntimeError("Interface error: unknown file test result.")
	if quiet:
		return result
	#print the results:
	print "Jobnumer: Matching Missing Non-Matching"
	error = 0
	nomatch_or_miss = 0
	total = 0
	for jobid in result:
		report = result[jobid]
		total += 1
		if report['_summary']['error'] != '':
			print "%d: Error: %s" % (jobid, report['_summary']['error'])
			error += 1
		else:
			fail = ''
			if report['_summary']['n_nomatch']!=0 or report['_summary']['n_miss']!=0:
				if set_to_failed:
					job_fail(os.path.join(workdir, 'jobs' + os.path.sep + ('job_%d.txt' % jobid)))
				nomatch_or_miss +=1
				fail = "FAIL "
			print "%s%d: %d %d %d" % (fail, jobid, report['_summary']['n_match'], report['_summary']['n_miss'], report['_summary']['n_nomatch'])
	print "Total: %d, Errors: %d, with non-matching or missing files: %d" % (total, error, nomatch_or_miss)
	return result

				
def get_jobids_success(workdir):
	"""Retuns a list of job ids having the state 'SUCCESS' in the given work directory."""
	result = []
	jobdb = JobDB(workdir, 0, None)
	#reverse keys and values in Job.states:
	job_statename_to_statenumber = dict([(v,k) for (k,v) in enumerate(Job.states)])
	for jobid in jobdb.list(types = [job_statename_to_statenumber['SUCCESS']]):
		result.append(jobid)
	return result
		
if __name__=='__main__':
	if len(sys.argv) != 3:
		print "Usage: %s <grid-control-conffile> <pathname>" % sys.argv[0]
		print " where <pathname> is the (locally accessible) path to the downloaded files from the grid"
		print " note that it can take quiet a while because for each file, md5sums have to be calculated."
		sys.exit(1)
	filename = sys.argv[1]
	#print md5sum(filename)
	dirname = sys.argv[2]
	verify(filename, dirname)

