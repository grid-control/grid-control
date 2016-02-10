#!/usr/bin/env python
#-#  Copyright 2010-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, sys, optparse
from gcSupport import getCMSSWInfo, getJobInfo, getWorkJobs, parseOptions, utils
from grid_control.datasets import DataSplitter
from grid_control_cms.lumi_tools import formatLumi, mergeLumi, parseLumiFilter
from python_compat import imap, irange, lmap, set, sorted

parser = optparse.OptionParser()

usage_le = 'Usage: %s <lumi filter expression>' % sys.argv[0]
ogManip = optparse.OptionGroup(parser, 'Manipulate lumi filter expressions', usage_le)
ogManip.add_option('-G', '--gc', dest='save_exprgc', default=False, action='store_true',
	help='Output grid-control compatible lumi expression')
ogManip.add_option('-J', '--expr-json', dest='save_exprjson', default=False, action='store_true',
	help='Output JSON file with lumi expression')
ogManip.add_option('-F', '--full', dest='save_exprfull', default=False, action='store_true',
	help='Output JSON file with full expression')
parser.add_option_group(ogManip)

usage_calc = 'Usage: %s <config file>' % sys.argv[0]
ogCalc = optparse.OptionGroup(parser, 'Options which allow luminosity related calculations', usage_calc)
ogCalc.add_option('-g', '--job-gc', dest='save_jobgc', default=False, action='store_true',
	help='Output grid-control compatible lumi expression for processed lumi sections')
ogCalc.add_option('-j', '--job-json', dest='save_jobjson', default=False, action='store_true',
	help='Output JSON file with processed lumi sections')
ogCalc.add_option('-e', '--job-events', dest='get_events', default=False, action='store_true',
	help='Get number of events processed')
ogCalc.add_option("-p", "--parameterized", dest="parameterized", default=False, action="store_true",
	help="Use output file name to categorize output (useful for parameterized tasks)")
parser.add_option_group(ogCalc)

(opts, args) = parseOptions(parser)

def outputGC(lumis, stream = sys.stdout):
	stream.write('%s\n' % utils.wrapList(formatLumi(lumis), 60, ',\n'))

def outputJSON(lumis, stream = sys.stdout):
	tmp = {}
	for rlrange in lumis:
		start, end = rlrange
		if start[0] != end[0]:
			raise Exception("Can't transform lumirange %s into JSON format" % repr(rlrange))
		if start[0] not in tmp:
			tmp[start[0]] = []
		tmp[start[0]].append([start[1], end[1]])
	stream.write('{\n')
	entries = imap(lambda run: '\t"%d": %s' % (run, tmp[run]), sorted(tmp.keys()))
	stream.write('%s\n' % str.join(',\n', entries))
	stream.write('}\n')

###########################
# Lumi filter calculations
###########################
def main():
	if opts.save_jobjson or opts.save_jobgc or opts.get_events:
		(workDir, nJobs, jobList) = getWorkJobs(args)
		(log, incomplete, splitter, splitInfo) = (None, False, None, {})
		(lumiDict, readDict, writeDict) = ({}, {}, {})
		try:
			splitter = DataSplitter.loadState(os.path.join(workDir, 'datamap.tar'))
		except Exception:
			pass
		jobList = sorted(jobList)

		for jobNum in jobList:
			del log
			log = utils.ActivityLog('Reading job logs - [%d / %d]' % (jobNum, jobList[-1]))
			jobInfo = getJobInfo(workDir, jobNum, lambda retCode: retCode == 0)
			if not jobInfo:
				if not incomplete:
					print 'WARNING: Not all jobs have finished - results will be incomplete!'
					incomplete = True
				continue

			if not parameterized:
				if splitter:
					splitInfo = splitter.getSplitInfo(jobNum)
				outputName = splitInfo.get(DataSplitter.Nickname, splitInfo.get(DataSplitter.DatasetID, 0))
			else:
				outputName = jobInfo['file'].split()[2].replace("_%d_" % jobNum, '_').replace('/', '_').replace('__', '_')

			# Read framework report files to get number of events
			try:
				outputDir = os.path.join(workDir, 'output', 'job_' + str(jobNum))
				for fwkXML in getCMSSWInfo(os.path.join(outputDir, 'cmssw.dbs.tar.gz')):
					for run in fwkXML.getElementsByTagName('Run'):
						for lumi in run.getElementsByTagName('LumiSection'):
							run_id = int(run.getAttribute('ID'))
							lumi_id = int(lumi.getAttribute('ID'))
							lumiDict.setdefault(outputName, {}).setdefault(run_id, set()).add(lumi_id)
					for outFile in fwkXML.getElementsByTagName('File'):
						pfn = outFile.getElementsByTagName('PFN')[0].childNodes[0].data
						if pfn not in writeDict.setdefault(outputName, {}):
							writeDict[outputName][pfn] = 0
						writeDict[outputName][pfn] += int(outFile.getElementsByTagName('TotalEvents')[0].childNodes[0].data)
					for inFile in fwkXML.getElementsByTagName('InputFile'):
						if outputName not in readDict:
							readDict[outputName] = 0
						readDict[outputName] += int(inFile.getElementsByTagName('EventsRead')[0].childNodes[0].data)
			except KeyboardInterrupt:
				sys.exit(os.EX_OK)
			except Exception:
				raise
				print 'Error while parsing framework output of job %s!' % jobNum
				continue

		del log
		log = utils.ActivityLog('Simplifying lumi sections')
		lumis = {}
		for sample in lumiDict:
			for run in lumiDict[sample]:
				for lumi in lumiDict[sample][run]:
					lumis.setdefault(sample, []).append(([run, lumi], [run, lumi]))
		for sample in lumiDict:
			lumis[sample] = mergeLumi(lumis[sample])
		del log

		for sample, lumis in lumis.items():
			print 'Sample:', sample
			print '========================================='
			print 'Number of events processed: %12d' % readDict[sample]
			print '  Number of events written: %12d' % sum(writeDict.get(sample, {}).values())
			if writeDict.get(sample, None):
				print
				head = [(0, '          Output filename'), (1, 'Events')]
				utils.printTabular(head, lmap(lambda pfn: {0: pfn, 1: writeDict[sample][pfn]}, writeDict[sample]))
			if opts.save_jobjson:
				outputJSON(lumis, open(os.path.join(workDir, 'processed_%s.json' % sample), 'w'))
				print 'Saved processed lumi sections in', os.path.join(workDir, 'processed_%s.json' % sample)
			if opts.save_jobgc:
				print
				print 'List of processed lumisections:'
				print '-----------------------------------------'
				outputGC(lumis)
			print


	###########################
	# Lumi filter manuipulation
	###########################
	if opts.save_exprgc or opts.save_exprjson or opts.save_exprfull:
		if len(args) == 0:
			raise Exception('No arguments given!')
		try:
			lumis = parseLumiFilter(str.join(' ', args))
		except Exception:
			raise Exception('Could not parse: %s' % str.join(' ', args))

		if opts.save_exprgc:
			outputGC(lumis)
		if opts.save_exprjson:
			outputJSON(lumis)
		if opts.save_exprfull:
			result = {}
			for rlrange in lumis:
				start, end = rlrange
				assert(start[0] == end[0])
				llist = result.setdefault(start[0], []).extend(irange(start[1], end[1] + 1))
			print result

sys.exit(main())
