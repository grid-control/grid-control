#!/usr/bin/env python
import gcSupport, sys, optparse, os, tarfile, xml.dom.minidom
from python_compat import *
from grid_control import utils
from grid_control.CMSSW import formatLumi, mergeLumi, parseLumiFilter

def fail(msg):
	print msg
	sys.exit(1)

parser = optparse.OptionParser()

usage_le = "Usage: %s <lumi filter expression>" % sys.argv[0]
ogManip = optparse.OptionGroup(parser, "Manipulate lumi filter expressions", usage_le)
ogManip.add_option("-G", "--gc", dest="save_exprgc", default=False, action="store_true",
	help="Output grid-control compatible lumi expression")
ogManip.add_option("-J", "--expr-json", dest="save_exprjson", default=False, action="store_true",
	help="Output JSON file with lumi expression")
parser.add_option_group(ogManip)

usage_calc = "Usage: %s <config file>" % sys.argv[0]
ogCalc = optparse.OptionGroup(parser, "Options which allow luminosity related calculations", usage_calc)
ogCalc.add_option("-g", "--job-gc", dest="save_jobgc", default=False, action="store_true",
	help="Output grid-control compatible lumi expression for processed lumi sections")
ogCalc.add_option("-j", "--job-json", dest="save_jobjson", default=False, action="store_true",
	help="Output JSON file with processed lumi sections")
ogCalc.add_option("-e", "--job-events", dest="get_events", default=False, action="store_true",
	help="Get number of events processed")
parser.add_option_group(ogCalc)

(opts, args) = parser.parse_args()

def outputGC(lumis, stream = sys.stdout):
	# Wrap after 60 characters
	for line in map(lambda x: str.join(", ", x), gcSupport.utils.lenSplit(formatLumi(lumis), 65)):
		stream.write("%s\n" % line)

def outputJSON(lumis, stream = sys.stdout):
	tmp = {}
	for rlrange in lumis:
		start, end = rlrange
		if start[0] != end[0]:
			raise
		if start[0] not in tmp:
			tmp[start[0]] = []
		tmp[start[0]].append([start[1], end[1]])
	stream.write("{\n")
	stream.write("%s\n" % str.join(',\n', map(lambda run: '\t"%d": %s' % (run, tmp[run]), sorted(tmp))))
	stream.write("}\n")

# Lumi filter manuipulation
if opts.save_exprgc or opts.save_exprjson:
	if len(args) == 0:
		fail()
	try:
		lumis = parseLumiFilter(str.join(" ", args))
	except:
		fail("Could not parse: %s" % str.join(" ", args))
	try:
		if opts.save_exprgc:
			outputGC(lumis)
		if opts.save_exprjson:
			outputJSON(lumis)
	except:
		fail("Could format lumi sections!" % args)
	sys.exit(0)

# Lumi filter calculations
if opts.save_jobjson or opts.get_events:
	(workDir, jobList) = gcSupport.getWorkJobs(args)

	log = None
	incomplete = False
	lumis = []
	nEvents_read = 0
	nEvents_write = {}
	jobList = sorted(jobList)
	for jobNum in jobList:
		outputDir = os.path.join(workDir, 'output', 'job_' + str(jobNum))
		jobInfo = gcSupport.getJobInfo(workDir, jobNum, lambda retCode: retCode == 0)
		del log
		log = utils.ActivityLog(' * Reading job logs - [%d / %d]' % (jobNum, jobList[-1]))

		if not jobInfo and not incomplete:
			print "Not all jobs have finished! Results will not be complete!"
			incomplete = True

		# Read framework report files to get number of events
		tarFile = tarfile.open(os.path.join(outputDir, "cmssw.dbs.tar.gz"), "r:gz")
		fwkReports = filter(lambda x: os.path.basename(x.name) == 'report.xml', tarFile.getmembers())
		try:
			for fwkReport in map(lambda fn: tarFile.extractfile(fn), fwkReports):
				fwkXML = xml.dom.minidom.parse(fwkReport)
				for outFile in fwkXML.getElementsByTagName("File"):
					pfn = outFile.getElementsByTagName("PFN")[0].childNodes[0].data
					nEvents_write[pfn] += int(outFile.getElementsByTagName("TotalEvents")[0].childNodes[0].data)
				for inFile in fwkXML.getElementsByTagName("InputFile"):
					nEvents_read += int(inFile.getElementsByTagName("EventsRead")[0].childNodes[0].data)
				for run in fwkXML.getElementsByTagName("Run"):
					for lumi in run.getElementsByTagName("LumiSection"):
						run_id = int(run.getAttribute("ID"))
						lumi_id = int(lumi.getAttribute("ID"))
						lumis.append(([run_id, lumi_id], [run_id, lumi_id]))
		except:
			raise
			print "Error while parsing framework output!"
			continue

	log = utils.ActivityLog(' * Simplifying lumi sections...')
	lumis = mergeLumi(lumis)
	del log
	if opts.save_jobgc:
		outputGC(lumis)
	if opts.save_jobjson:
		outputJSON(lumis, open(os.path.join(workDir, 'processed.json'), 'w'))
	print
	print "Number of events processed: %12d" % nEvents_read
	print "  Number of events written: %12d" % sum(nEvents_write.values())
	for pfn in nEvents_write:
		print " * %10d - %s" % (nEvents_write[pfn], pfn)
	print 
