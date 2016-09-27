#!/usr/bin/env python
# | Copyright 2010-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, sys
from gcSupport import Activity, ClassSelector, FileInfoProcessor, JobClass, Options, getCMSSWInfo, initGC, scriptOptions, utils
from grid_control.datasets import DataSplitter
from grid_control_cms.lumi_tools import formatLumi, mergeLumi, parseLumiFilter
from hpfwk import clear_current_exception
from python_compat import imap, irange, lmap, set, sorted


parser = Options()
parser.section('expr', 'Manipulate lumi filter expressions', '%s <lumi filter expression>')
parser.add_bool('expr', 'G', 'gc',            default = False, help = 'Output grid-control compatible lumi expression')
parser.add_bool('expr', 'J', 'json',          default = False, help = 'Output JSON file with lumi expression')
parser.add_bool('expr', 'F', 'full',          default = False, help = 'Output JSON file with full expression')

parser.section('calc', 'Options which allow luminosity related calculations', '%s <config file>')
parser.add_text('calc', 'O', 'output-dir',    default = None,  help = 'Set output directory (default: work directory)')
parser.add_bool('calc', 'g', 'job-gc',        default = False, help = 'Output grid-control compatible lumi expression for processed lumi sections')
parser.add_bool('calc', 'j', 'job-json',      default = False, help = 'Output JSON file with processed lumi sections')
parser.add_bool('calc', 'e', 'job-events',    default = False, help = 'Get number of events processed')
parser.add_bool('calc', 'p', 'parameterized', default = False, help = 'Use output file name to categorize output (useful for parameterized tasks)')
parser.add_bool('calc', ' ', 'replace',   default = 'job_%d_', help = 'Pattern to replace for parameterized jobs (default: job_%%d_')
options = scriptOptions(parser)

def outputGC(lumis, stream = sys.stdout):
	stream.write('%s\n' % utils.wrap_list(formatLumi(lumis), 60, ',\n'))

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

# Lumi filter manuipulation
def lumi_expr(opts, args):
	if len(args) == 0:
		raise Exception('No arguments given!')
	try:
		lumis = parseLumiFilter(str.join(' ', args))
	except Exception:
		raise Exception('Could not parse: %s' % str.join(' ', args))

	if opts.gc:
		outputGC(lumis)
	if opts.json:
		outputJSON(lumis)
	if opts.full:
		result = {}
		for rlrange in lumis:
			start, end = rlrange
			if start[0] != end[0]:
				raise Exception('Lumi filter term contains different runs: %s' % repr(rlrange))
			result.setdefault(start[0], []).extend(irange(start[1], end[1] + 1))
		print(result)

def iter_jobs(opts, workDir, jobList, splitter):
	(splitInfo, fip) = ({}, FileInfoProcessor())
	activity = Activity('Reading job logs')
	for jobNum in jobList:
		activity.update('Reading job logs - [%d / %d]' % (jobNum, jobList[-1]))

		if opts.parameterized:
			fi = fip.process(os.path.join(workDir, 'output', 'job_%d' % jobNum))
			outputName = fi[0][FileInfoProcessor.NameDest].split('.')[0]
			outputName = outputName.replace(opts.replace % jobNum, '_').replace('/', '_').replace('__', '_').strip('_')
		else:
			if splitter:
				splitInfo = splitter.get_partition(jobNum)
			outputName = splitInfo.get(DataSplitter.Nickname, splitInfo.get(DataSplitter.Dataset, '').replace('/', '_'))
		yield (jobNum, outputName)
	activity.finish()

def process_fwjr(outputName, fwkXML, lumiDict, readDict, writeDict):
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

def process_jobs(opts, workDir, jobList, splitter):
	(lumiDict, readDict, writeDict) = ({}, {}, {})
	for (jobNum, outputName) in iter_jobs(opts, workDir, jobList, splitter):
		# Read framework report files to get number of events
		try:
			outputDir = os.path.join(workDir, 'output', 'job_' + str(jobNum))
			for fwkXML in getCMSSWInfo(os.path.join(outputDir, 'cmssw.dbs.tar.gz')):
				process_fwjr(outputName, fwkXML, lumiDict, readDict, writeDict)
		except KeyboardInterrupt:
			sys.exit(os.EX_OK)
		except Exception:
			print('Error while parsing framework output of job %s!' % jobNum)
			continue
	return (lumiDict, readDict, writeDict)

# Lumi filter calculations
def lumi_calc(opts, workDir, jobList, splitter):
	(lumiDict, readDict, writeDict) = process_jobs(opts, workDir, jobList, splitter)

	activity = Activity('Simplifying lumi sections')
	lumis = {}
	for sample in lumiDict:
		for run in lumiDict[sample]:
			for lumi in lumiDict[sample][run]:
				lumis.setdefault(sample, []).append(([run, lumi], [run, lumi]))
	for sample in lumiDict:
		lumis[sample] = mergeLumi(lumis[sample])
	activity.finish()

	for sample, lumi_list in lumis.items():
		print('Sample: %s' % sample)
		if opts.job_events:
			print('=========================================')
			print('Number of events processed: %12s' % readDict.get(sample))
			print('  Number of events written: %12d' % sum(writeDict.get(sample, {}).values()))
			if writeDict.get(sample, None):
				sys.stdout.write('\n')
				head = [(0, '          Output filename'), (1, 'Events')]
				utils.display_table(head, lmap(lambda pfn: {0: pfn, 1: writeDict[sample][pfn]}, writeDict[sample]))
		if opts.job_json:
			json_fn = os.path.join(opts.output_dir or workDir, 'processed_%s.json' % sample)
			outputJSON(lumi_list, open(json_fn, 'w'))
			print('Saved processed lumi sections in ' + json_fn)
		if opts.job_gc:
			sys.stdout.write('\n')
			print('List of processed lumisections:')
			print('-----------------------------------------')
			outputGC(lumi_list)
		sys.stdout.write('\n')

def main(opts, args):
	if opts.gc or opts.json or opts.full:
		return lumi_expr(opts, args)

	if opts.job_json or opts.job_gc or opts.job_events:
		(config, jobDB) = initGC(args)
		workDir = config.get_work_path()
		splitter = None
		try:
			splitter = DataSplitter.load_partitions_for_script(os.path.join(workDir, 'datamap.tar'))
		except Exception:
			clear_current_exception()
		return lumi_calc(opts, workDir, sorted(jobDB.getJobs(ClassSelector(JobClass.SUCCESS))), splitter)

if __name__ == '__main__':
	sys.exit(main(options.opts, options.args))
