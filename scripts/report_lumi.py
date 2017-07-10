#!/usr/bin/env python
# | Copyright 2017 Karlsruhe Institute of Technology
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

import os, sys, signal, logging
from gc_scripts import Activity, ClassSelector, ConsoleTable, FileInfo, FileInfoProcessor, JobClass, ScriptOptions, get_cmssw_info, get_script_object_cmdline, handle_abort_interrupt, iter_jobnum_output_dn  # pylint:disable=line-too-long
from grid_control.datasets import DataSplitter
from grid_control.utils import wrap_list
from grid_control.utils.file_tools import SafeFile, with_file
from grid_control_cms.lumi_tools import format_lumi, merge_lumi_list, parse_lumi_filter
from hpfwk import NestedException, clear_current_exception, rethrow
from python_compat import imap, irange, lmap, partial, set, sorted


LOG = logging.getLogger('script')


def convert_lumi_expr(opts, args):
	# Lumi filter manuipulation
	run_lumi_range_list = rethrow(NestedException('Could not parse: %s' % str.join(' ', args)),
		parse_lumi_filter, str.join(' ', args))

	if opts.gc:
		write_lumi_gc(run_lumi_range_list)
	if opts.json:
		write_lumi_json(run_lumi_range_list)
	if opts.full:
		write_lumi_ext(run_lumi_range_list)


def iter_jobs(opts, work_dn, jobnum_list, reader):
	fip = FileInfoProcessor()
	for (jobnum, output_dn) in iter_jobnum_output_dn(os.path.join(work_dn, 'output'), jobnum_list):
		if opts.parameterized:
			fi = fip.process(output_dn)
			sample = fi[0][FileInfo.NameDest].split('.')[0]
			sample = sample.replace(opts.replace % jobnum, '_')
		elif reader is not None:
			partition = reader.get_partition_checked(jobnum)
			sample = partition.get(DataSplitter.Nickname, partition.get(DataSplitter.Dataset, ''))
		else:
			sample = 'sample'
		yield (jobnum, sample.replace('/', '_').replace('__', '_').strip('_'))


def lumi_calc(opts, work_dn, jobnum_list, reader):
	# Lumi filter calculations
	(map_sample2run_info_dict, map_sample2input_events, map_sample2output_events) = process_jobs(opts,
		work_dn, jobnum_list, reader)

	activity = Activity('Simplifying lumi sections')
	map_sample2run_lumi_range = {}
	for sample in map_sample2run_info_dict:
		for run in map_sample2run_info_dict[sample]:
			for lumi in map_sample2run_info_dict[sample][run]:
				map_sample2run_lumi_range.setdefault(sample, []).append(([run, lumi], [run, lumi]))
	for sample in map_sample2run_info_dict:
		map_sample2run_lumi_range[sample] = merge_lumi_list(map_sample2run_lumi_range[sample])
	activity.finish()

	for sample, lumi_list in map_sample2run_lumi_range.items():
		if opts.job_events:
			if map_sample2output_events.get(sample):
				LOG.info('')
			display_dict_list = lmap(lambda pfn: {0: pfn, 1: map_sample2output_events[sample][pfn]},
				map_sample2output_events.get(sample, {}))
			if display_dict_list:
				display_dict_list.append('=')
			display_dict_list += [{0: 'Processed in total', 1: map_sample2input_events.get(sample)}]
			ConsoleTable.create([(0, ''), (1, '#Events')], display_dict_list,
				title='Sample: %s' % sample)
		if opts.job_json:
			json_fn = os.path.join(opts.output_dir or work_dn, 'processed_%s.json' % sample)
			with_file(SafeFile(json_fn, 'w'), partial(write_lumi_json, lumi_list))
			LOG.info('Saved processed lumi sections in %s', json_fn)
		if opts.job_gc:
			LOG.info('\nList of processed lumisections\n' + '-' * 30)
			write_lumi_gc(lumi_list)
		LOG.info('')


def process_fwjr(sample, fwjr_xml_dom,
		map_sample2run_info_dict, map_sample2input_events, map_sample2output_events):
	def _get_element_data(node, name):
		return node.getElementsByTagName(name)[0].childNodes[0].data

	# Collect run lumi information
	for run_node in fwjr_xml_dom.getElementsByTagName('Run'):
		for lumi_node in run_node.getElementsByTagName('LumiSection'):
			run = int(run_node.getAttribute('ID'))
			lumi = int(lumi_node.getAttribute('ID'))
			map_sample2run_info_dict.setdefault(sample, {}).setdefault(run, set()).add(lumi)
	# Collect output file information
	for output_file_node in fwjr_xml_dom.getElementsByTagName('File'):
		pfn = _get_element_data(output_file_node, 'PFN')
		if pfn not in map_sample2output_events.setdefault(sample, {}):
			map_sample2output_events[sample][pfn] = 0
		map_sample2output_events[sample][pfn] += int(_get_element_data(output_file_node, 'TotalEvents'))
	# Collect input file information
	for input_file_node in fwjr_xml_dom.getElementsByTagName('InputFile'):
		if sample not in map_sample2input_events:
			map_sample2input_events[sample] = 0
		map_sample2input_events[sample] += int(_get_element_data(input_file_node, 'EventsRead'))


def process_jobs(opts, work_dn, jobnum_list, reader):
	(map_sample2run_info_dict, map_sample2input_events, map_sample2output_events) = ({}, {}, {})
	for (jobnum, sample) in iter_jobs(opts, work_dn, jobnum_list, reader):
		# Read framework report files to get number of events
		try:
			output_dn = os.path.join(work_dn, 'output', 'job_' + str(jobnum))
			for fwjr_xml_dom in get_cmssw_info(os.path.join(output_dn, 'cmssw.dbs.tar.gz')):
				process_fwjr(sample, fwjr_xml_dom, map_sample2run_info_dict,
					map_sample2input_events, map_sample2output_events)
		except Exception:
			LOG.exception('Error while parsing framework output of job %s!', jobnum)
			clear_current_exception()
			continue
	return (map_sample2run_info_dict, map_sample2input_events, map_sample2output_events)


def write_any(value, stream):
	if stream is None:
		LOG.info(value.rstrip())
	else:
		stream.write(value.rstrip() + '\n')


def write_lumi_ext(run_lumi_range_list, stream=None):
	map_run2lumi_list = {}
	write_iter = _iter_run_dict(map_run2lumi_list, run_lumi_range_list, 'extended lumi format')
	for (lumi_list, lumi_start, lumi_end) in write_iter:
		lumi_list.extend(irange(lumi_start, lumi_end + 1))
	_write_run_dict(map_run2lumi_list, stream)


def write_lumi_gc(run_lumi_range_list, stream=None):
	write_any('%s\n' % wrap_list(format_lumi(run_lumi_range_list), 60, ',\n'), stream)


def write_lumi_json(run_lumi_range_list, stream=None):
	map_run2lumi_range_list = {}
	write_iter = _iter_run_dict(map_run2lumi_range_list, run_lumi_range_list, 'JSON format')
	for (lumi_range_list, lumi_start, lumi_end) in write_iter:
		lumi_range_list.append([lumi_start, lumi_end])
	_write_run_dict(map_run2lumi_range_list, stream)


def _iter_run_dict(run_dict, run_lumi_range_list, fmt_name):
	for run_lumi_range in run_lumi_range_list:
		(run_lumi_start, run_lumi_end) = run_lumi_range
		if run_lumi_start[0] != run_lumi_end[0]:  # range must stay within one run
			raise Exception('Can\'t transform run-lumi range %s into %s' % (repr(run_lumi_range), fmt_name))
		yield (run_dict.setdefault(run_lumi_start[0], []), run_lumi_start[1], run_lumi_end[1])


def _main():
	signal.signal(signal.SIGINT, handle_abort_interrupt)

	parser = ScriptOptions()
	parser.section('expr', 'Manipulate lumi filter expressions', '%s <lumi filter expression>')
	parser.add_bool('expr', 'G', 'gc', default=False,
		help='Output grid-control compatible lumi expression')
	parser.add_bool('expr', 'J', 'json', default=False,
		help='Output JSON file with lumi expression')
	parser.add_bool('expr', 'F', 'full', default=False,
		help='Output JSON file with full expression')

	parser.section('calc', 'Options which allow luminosity related calculations',
		'%s <config file> [<job selector>]')
	parser.add_text('calc', 'O', 'output-dir', default=None,
		help='Set output directory (default: work directory)')
	parser.add_bool('calc', 'g', 'job-gc', default=False,
		help='Output grid-control compatible lumi expression for processed lumi sections')
	parser.add_bool('calc', 'j', 'job-json', default=False,
		help='Output JSON file with processed lumi sections')
	parser.add_bool('calc', 'e', 'job-events', default=False,
		help='Get number of events processed')
	parser.add_bool('calc', 'p', 'parameterized', default=False,
		help='Use output file name to categorize output (useful for parameterized tasks)')
	parser.add_bool('calc', ' ', 'replace', default='job_%d_',
		help='Pattern to replace for parameterized jobs (default: job_%%d_')
	options = parser.script_parse()

	if options.opts.gc or options.opts.json or options.opts.full:
		if not options.args:
			options.parser.exit_with_usage(options.parser.usage('expr'))
		return convert_lumi_expr(options.opts, options.args)

	if options.opts.job_json or options.opts.job_gc or options.opts.job_events:
		if not options.args:
			options.parser.exit_with_usage(options.parser.usage('calc'))
		script_obj = get_script_object_cmdline(options.args, only_success=True)
		work_dn = script_obj.config.get_work_path()
		reader = None
		try:
			reader = DataSplitter.load_partitions(os.path.join(work_dn, 'datamap.tar'))
		except Exception:
			clear_current_exception()
		jobnum_list = sorted(script_obj.job_db.get_job_list(ClassSelector(JobClass.SUCCESS)))
		return lumi_calc(options.opts, work_dn, jobnum_list, reader)


def _write_run_dict(run_dict, stream):
	run_entry_iter = imap(lambda run: '\t"%d": %s' % (run, run_dict[run]), sorted(run_dict.keys()))
	write_any('{\n%s\n}' % str.join(',\n', run_entry_iter), stream)


if __name__ == '__main__':
	sys.exit(_main())
