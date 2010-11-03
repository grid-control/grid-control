#!/usr/bin/env python
import gcSupport, sys, optparse, os, tarfile, xml.dom.minidom
from python_compat import *
from grid_control import utils, datasets
from grid_control.datasets import DataSplitter
from grid_control.CMSSW import formatLumi, mergeLumi, parseLumiFilter

def fail(msg):
	print msg
	sys.exit(1)

parser = optparse.OptionParser()

usage_le = "Usage: %s <lumi filter expression>" % sys.argv[0]
ogManip = optparse.OptionGroup(parser, "Manipulate lumi filter expressions", usage_le)
ogManip.add_option("-d", "--diff", dest="diff",
	help="Calculate difference between json file and lumi filter expression")
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
	stream.write("%s\n" % gcSupport.utils.wrapList(formatLumi(lumis), 60, ',\n'))

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
	entries = map(lambda run: '\t"%d": %s' % (run, tmp[run]), sorted(tmp.keys()))
	stream.write("%s\n" % str.join(',\n', entries))
	stream.write("}\n")

###########################
# Lumi filter calculations
###########################
if opts.save_jobjson or opts.save_jobgc or opts.get_events:
	(workDir, jobList) = gcSupport.getWorkJobs(args)
	(log, incomplete, splitter, splitInfo) = (None, False, None, {})
	(lumiDict, readDict, writeDict) = ({}, {}, {})
	try:
		splitter = DataSplitter.loadState(os.path.join(workDir, 'datamap.tar'))
	except:
		pass
	jobList = sorted(jobList)

	for jobNum in jobList:
		del log
		log = utils.ActivityLog('Reading job logs - [%d / %d]' % (jobNum, jobList[-1]))
		jobInfo = gcSupport.getJobInfo(workDir, jobNum, lambda retCode: retCode == 0)
		if not jobInfo:
			if not incomplete:
				print "WARNING: Not all jobs have finished - results will be incomplete!"
				incomplete = True
			continue

		if splitter:
			splitInfo = splitter.getSplitInfo(jobNum)
		outputName = splitInfo.get(DataSplitter.Nickname, splitInfo.get(DataSplitter.DatasetID, 0))

		# Read framework report files to get number of events
		try:
			outputDir = os.path.join(workDir, 'output', 'job_' + str(jobNum))
			for fwkXML in gcSupport.getCMSSWInfo(os.path.join(outputDir, "cmssw.dbs.tar.gz")):
				for run in fwkXML.getElementsByTagName("Run"):
					for lumi in run.getElementsByTagName("LumiSection"):
						run_id = int(run.getAttribute("ID"))
						lumi_id = int(lumi.getAttribute("ID"))
						lumiDict.setdefault(outputName, {}).setdefault(run_id, set()).add(lumi_id)
				for outFile in fwkXML.getElementsByTagName("File"):
					pfn = outFile.getElementsByTagName("PFN")[0].childNodes[0].data
					if pfn not in writeDict.setdefault(outputName, {}):
						writeDict[outputName][pfn] = 0
					writeDict[outputName][pfn] += int(outFile.getElementsByTagName("TotalEvents")[0].childNodes[0].data)
				for inFile in fwkXML.getElementsByTagName("InputFile"):
					if outputName not in readDict:
						readDict[outputName] = 0
					readDict[outputName] += int(inFile.getElementsByTagName("EventsRead")[0].childNodes[0].data)
		except KeyboardInterrupt:
			sys.exit(0)
		except:
			raise
			print "Error while parsing framework output of job %s!" % jobNum
			continue

	del log
	log = utils.ActivityLog('Simplifying lumi sections...')
	lumis = {}
	for sample in lumiDict:
		for run in lumiDict[sample]:
			for lumi in lumiDict[sample][run]:
				lumis.setdefault(sample, []).append(([run, lumi], [run, lumi]))
	for sample in lumiDict:
		lumis[sample] = mergeLumi(lumis[sample])
	del log

	for sample, lumis in lumis.items():
		print "Sample:", sample
		print "========================================="
		print "Number of events processed: %12d" % readDict[sample]
		print "  Number of events written: %12d" % sum(writeDict.get(sample, {}).values())
		if writeDict.get(sample, None):
			print
			head = [(0, "          Output filename"), (1, "Events")]
			utils.printTabular(head, map(lambda pfn: {0: pfn, 1: writeDict[sample][pfn]}, writeDict[sample]))
		if opts.save_jobjson:
			outputJSON(lumis, open(os.path.join(workDir, 'processed_%s.json' % sample), 'w'))
			print "Saved processed lumi sections in", os.path.join(workDir, 'processed_%s.json' % sample)
		if opts.save_jobgc:
			print
			print "List of processed lumisections:"
			print "-----------------------------------------"
			outputGC(lumis)
		print


###########################
# Lumi filter manuipulation
###########################
if opts.save_exprgc or opts.save_exprjson:
	if len(args) == 0:
		fail()
	try:
		lumis = parseLumiFilter(str.join(" ", args))
	except:
		fail("Could not parse: %s" % str.join(" ", args))

	if opts.save_exprgc:
		outputGC(lumis)
	if opts.save_exprjson:
		outputJSON(lumis)

	if opts.diff:
		def updateLumiSets(lumis_a, lumis_b, lumis_uc_old):
			lumis_uc = set.intersection(lumis_a, lumis_b)
			lumis_uc.update(lumis_uc_old)
			lumis_uc = set(mergeLumi(list(lumis_uc)))
			lumis_a.difference_update(lumis_uc)
			lumis_b.difference_update(lumis_uc)
			return (lumis_a, lumis_b, lumis_uc)

		def findLumiEdges(x):
			def addToLumiEdges(r, l):
				if r not in lumiEdges:
					lumiEdges[r] = set()
				lumiEdges[r].add(l)
			for (s, e) in x:
				addToLumiEdges(s[0], s[1])
				addToLumiEdges(e[0], e[1])

		# Split continous lumi sections along lumi edges:
		def splitLumiRanges(lumis, off = 0, singlemode = False):
			# Split into single runs
			(todel, toadd) = (set(), set())
			for (s, e) in lumis:
				if s[0] and e[0] and s[0] != e[0]:
					todel.add((s, e))
					toadd.add((s, (s[0], None)))
					toadd.add(((e[0], 1), e))
					for x in range(s[0] + 1, e[0]):
						toadd.add(((x, 1), (x, None)))
			lumis.difference_update(todel)
			lumis.update(toadd)

			# Split along lumi edges
			(todel, toadd) = (set(), set())
			for (s, e) in lumis:
				edges = sorted(filter(lambda x: x > s[1] and x < e[1], lumiEdges[s[0]]))
				if edges:
					todel.add((s, e))
					edg = map(lambda x: x + off, edges)
					toadd.update(zip([s] + map(lambda x: (s[0], x + 1), edg), map(lambda x: (s[0], x), edg) + [e]))
				elif s[1] != e[1] and singlemode:
					todel.add((s, e))
					toadd.update([
						((s[0], s[1]), (s[0], s[1])),
						((s[0], s[1] + 1), (e[0], e[1] - 1)),
						((e[0], e[1]), (e[0], e[1]))])
			lumis.difference_update(todel)
			lumis.update(toadd)

		def setify(x):
			return set(map(lambda x: ((x[0][0], x[0][1]), (x[1][0], x[1][1])), x))

		lumis_a = setify(lumis)
		lumis_b = setify(parseLumiFilter(opts.diff))
		lumis_uc = set()
		for offset in [0, 1, -1, None]:
			for singlemode in [True, False]:
				(lumis_a, lumis_b, lumis_uc) = updateLumiSets(lumis_a, lumis_b, lumis_uc)
				lumis_a = set(mergeLumi(list(lumis_a)))
				lumis_b = set(mergeLumi(list(lumis_b)))
				if offset != None:
					lumiEdges = {}
					findLumiEdges(lumis_a)
					findLumiEdges(lumis_b)
					splitLumiRanges(lumis_a, offset, singlemode)
					splitLumiRanges(lumis_b, offset, singlemode)

		print "Unchanged:\n", 30 * "="
		outputGC(mergeLumi(list(lumis_uc)))
		print "\nOnly in reference file:\n", 30 * "="
		outputGC(mergeLumi(list(lumis_b)))
		print "\nNot in reference file:\n", 30 * "="
		outputGC(mergeLumi(list(lumis_a)))
