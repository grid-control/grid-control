# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

# produces plots showing the Jobs performance
#
# run like this to plot all successful jobs:
#
# scripts/report.py <config file> --report PlotReport --job-selector state:SUCCESS
#
# add the option --use-task if you want the plotting script to load data like event count
# per job from the configuration

import os, re


try:
	import numpy
except ImportError:
	numpy = None
try:
	import matplotlib
	import matplotlib.pyplot
except ImportError:
	matplotlib = None
from grid_control.output_processor import JobInfoProcessor, JobResult
from grid_control.report import ImageReport
from grid_control.utils.data_structures import make_enum
from hpfwk import clear_current_exception
from python_compat import BytesBuffer, irange, izip


JobMetrics = make_enum([  # pylint:disable=invalid-name
	'EVENT_COUNT',
	'FILESIZE_IN_TOTAL',
	'FILESIZE_OUT_TOTAL',
	'TS_CMSSW_CMSRUN1_DONE',
	'TS_CMSSW_CMSRUN1_START',
	'TS_DEPLOYMENT_DONE',
	'TS_DEPLOYMENT_START',
	'TS_EXECUTION_DONE',
	'TS_EXECUTION_START',
	'TS_SE_IN_DONE',
	'TS_SE_IN_START',
	'TS_SE_OUT_DONE',
	'TS_SE_OUT_START',
	'TS_WRAPPER_DONE',
	'TS_WRAPPER_START',
])


class PlotReport(ImageReport):
	alias_list = ['plot']

	def __init__(self, config, name, job_db, task=None):
		ImageReport.__init__(self, config, name, job_db, task)
		self._task = task
		self._output_dn = config.get_work_path('output')
		self._job_result_list = []
		if not numpy:
			raise Exception('Unable to find numpy')
		if not matplotlib:
			raise Exception('Unable to find matplotlib')
		# larger default fonts
		matplotlib.rcParams.update({'font.size': 16})

	def show_report(self, job_db, jobnum_list):
		self._job_result_list = []
		self._log.info(str(len(jobnum_list)) + ' job(s) selected for plots')

		time_span_se_out = (None, None)
		time_span_se_in = (None, None)
		time_span_wrapper = (None, None)
		time_span_cmssw = (None, None)

		for jobnum in jobnum_list:
			try:
				job_info = JobInfoProcessor().process(os.path.join(self._output_dn, 'job_%d' % jobnum))
			except Exception:
				clear_current_exception()
				self._log.info('Ignoring job')
				continue
			if job_info.get(JobResult.EXITCODE) != 0:
				continue

			job_result = _extract_job_metrics(job_info, self._task)
			self._job_result_list.append(job_result)

			time_span_se_in = self._bound_time(job_result, time_span_se_in,
				JobMetrics.TS_SE_IN_START, JobMetrics.TS_SE_IN_DONE)
			time_span_se_out = self._bound_time(job_result, time_span_se_out,
				JobMetrics.TS_SE_OUT_START, JobMetrics.TS_SE_OUT_DONE)
			time_span_wrapper = self._bound_time(job_result, time_span_wrapper,
				JobMetrics.TS_WRAPPER_START, JobMetrics.TS_WRAPPER_DONE)
			time_span_cmssw = self._bound_time(job_result, time_span_cmssw,
				JobMetrics.TS_CMSSW_CMSRUN1_START, JobMetrics.TS_CMSSW_CMSRUN1_DONE)

		self._make_hist('payload_runtime', 'Payload Runtime (min)', 'Count', _get_runtime_payload_in_min)
		self._make_hist('event_per_job', 'Event per Job', 'Count', _get_events)
		self._make_hist('event_rate', 'Event Rate (Events/min)', 'Count', _get_events_per_min)

		self._make_hist('se_in_runtime', 'SE In Runtime (s)', 'Count', _get_runtime_se_in)
		self._make_hist('se_in_size', 'SE IN Size (MB)', 'Count', _get_mb_se_in)

		self._make_hist('se_out_runtime', 'SE OUT Runtime (s)', 'Count', _get_runtime_se_out)
		self._make_hist('se_out_bandwidth', 'SE OUT Bandwidth (MB/s)', 'Count', _get_mb_per_sec_se_out)
		self._make_hist('se_out_size', 'SE OUT Size (MB)', 'Count', _get_mb_se_out)
		self._make_hist('se_out_runtime', 'SE Out Runtime (s)', 'Count', _get_runtime_se_out)

		# job active & complete
		self._make_overview('job_active_total', 'Time (s)', 'Jobs Active',
			time_span_wrapper, _get_job_count_timeslice)
		self._make_overview('job_complete_total', 'Time (s)', 'Jobs Complete',
			time_span_wrapper, _get_job_rate_timeslice)
		# stage out active & bandwidth
		self._make_overview('se_out_bandwidth_total', 'Time (s)', 'Total SE OUT Bandwidth (MB/s)',
			time_span_se_out, _get_se_out_bandwidth_timeslice, show_average=True)
		self._make_overview('se_out_active_total', 'Time (s)', 'Active Stageouts',
			time_span_se_out, _get_se_out_count_timeslice)
		# stage in active & bandwidth
		self._make_overview('se_in_bandwidth_total', 'Time (s)', 'Total SE IN Bandwidth (MB/s)',
			time_span_se_in, _get_se_in_bandwidth_timeslice, show_average=True)
		self._make_overview('se_in_active_total', 'Time (s)', 'Active Stageins',
			time_span_se_in, _get_se_in_count_timeslice)
		# total stageout size
		self._make_overview('se_out_cum_size', 'Time (s)', 'Stageout Cumulated Size (MB)',
			time_span_se_out, _get_mb_se_out_timeslice, cumulate=True)
		# total stagein size
		self._make_overview('se_in_cum_size', 'Time (s)', 'Stagein Cumulated Size (MB)',
			time_span_se_in, _get_mb_se_in_timeslice, cumulate=True)
		# event rate
		if time_span_cmssw != (None, None):
			self._make_overview('event_rate_total', 'Time (s)', 'Event Rate (Events/min)',
				time_span_cmssw, _get_events_per_min_timeslice)
		else:
			self._log.info('Skipping event_rate_total')

	def _bound_metric(self, time_step_list, metric_list, lim_low, lim_high):
		time_step_list_truncated = []
		metric_list_truncated = []
		for time_step, metric in izip(time_step_list, metric_list):
			if lim_low < time_step < lim_high:
				metric_list_truncated.append(metric)
				time_step_list_truncated.append(time_step)
		return (time_step_list_truncated, metric_list_truncated)

	def _bound_time(self, job_result, time_span, stamp_start, stamp_end):
		(min_time, max_time) = time_span
		if ((min_time is None) or (max_time is None)) and (stamp_start in job_result):
			min_time = job_result[stamp_start]
			max_time = job_result[stamp_end]
		elif stamp_start in job_result:
			min_time = min(min_time, job_result[stamp_start])
			max_time = max(max_time, job_result[stamp_end])
		return (min_time, max_time)

	def _collect_metric(self, min_time, max_time, step, cumulate, extractor, job_infos):
		time_step_list = []
		metric_list = []
		for idx in irange(min_time, max_time + 1, step):
			metric = 0
			time_step_list.append(idx - min_time)
			for job_info in job_infos:
				if cumulate:
					value = extractor(job_info, min_time, idx + 1)
				else:
					value = extractor(job_info, idx, idx + step)
				if value is not None:
					metric += value
			metric_list.append(metric)
		return (time_step_list, metric_list)

	def _draw_avg(self, time_step_list, metric_list, unit, trunc_frac_low, trunc_frac_high, time_diff):
		avg_value = numpy.polyfit(time_step_list, metric_list, 0)[0]
		matplotlib.pyplot.axhline(y=avg_value, xmin=trunc_frac_low, xmax=1.0 - trunc_frac_high,
			color='black', lw=2)
		matplotlib.pyplot.annotate('%.2f' % avg_value + ' ' + unit, xy=(time_diff * 0.7, avg_value),
			xytext=(time_diff * 0.75, avg_value * 0.85), backgroundcolor='gray')

	def _finish_hist(self, name_fig_axis, use_legend=False):
		if use_legend:
			matplotlib.pyplot.legend(loc='upper right', numpoints=1, frameon=False, ncol=2)
		image_type_list = ['png', 'pdf']
		for image_type in image_type_list:
			buffer = BytesBuffer()
			matplotlib.pyplot.savefig(buffer, format=image_type)
			self._show_image(name_fig_axis[0] + '.' + image_type, buffer)
			buffer.close()

	def _init_hist(self, name, xlabel, ylabel):
		fig = matplotlib.pyplot.figure()
		axis = fig.add_subplot(111)
		axis.set_xlabel(xlabel)
		axis.set_ylabel(ylabel, va='top', y=0.75, labelpad=20.0)
		return (name, fig, axis)

	def _make_hist(self, name, xlabel, ylabel, extractor):
		name_fig_axis = self._init_hist(name, xlabel, ylabel)
		self._plot_hist(name_fig_axis, self._job_result_list, extractor)
		self._finish_hist(name_fig_axis)

	def _make_overview(self, name, xlabel, ylabel, timespan, extractor,
			show_average=False, unit='MB/s', cumulate=False):
		if (timespan[0] == timespan[1]) or (None in timespan):
			self._log.info('Skipping plot %s because no timespan is available', name)
			return
		name_fig_axis = self._init_hist(name, xlabel, ylabel)
		self._plot_overview(name_fig_axis, self._job_result_list, timespan,
			extractor, show_average, unit, cumulate)
		self._finish_hist(name_fig_axis)

	def _plot_hist(self, name_fig_axis, job_result, extractor):
		self._log.info('Plotting %s ...', name_fig_axis[0])
		runtime = []
		for res in job_result:
			value = extractor(res)
			if value is not None:
				runtime.append(value)

		if not runtime:
			self._log.info('Skipping %s, no input metric', name_fig_axis[0])
			return None

		hist = matplotlib.pyplot.hist(runtime, 40)
		self._log.info('done')
		return hist

	def _plot_overview(self, name_fig_axis, job_infos, timespan, extractor, show_average=False,
			unit='MB/s', cumulate=False, trunc_frac_low=0.05, trunc_frac_high=0.3):
		self._log.info('Plotting %s ...', name_fig_axis[0])

		time_diff = max(timespan) - min(timespan)
		# compute the amount of slices, for a small timespan, use every step
		# for large timespans, use 1000 slices at most
		step = int(time_diff / min(1000.0, time_diff))

		(time_step_list, metric_list) = \
			self._collect_metric(min(timespan), max(timespan), step, cumulate, extractor, job_infos)

		# make sure the axis are not exactly the same
		name_fig_axis[2].set_ylim(bottom=min(metric_list) * 0.99,
			top=max(max(metric_list), min(metric_list) + 1) * 1.2)
		name_fig_axis[2].set_xlim(left=min(time_step_list) * 0.99, right=max(time_step_list) * 1.01)
		matplotlib.pyplot.plot(time_step_list, metric_list, color='green')

		if show_average:
			# not the first and last 5 percent, used for fitting
			(time_step_list, metric_list) = self._bound_metric(time_step_list, metric_list,
				time_diff * trunc_frac_low, time_diff * (1.0 - trunc_frac_high))
			if time_step_list and metric_list:
				self._draw_avg(time_step_list, metric_list, unit,
					trunc_frac_low, trunc_frac_high, time_diff)
			else:
				self._log.info('Skipping fit due to the lack of input metric')
		self._log.info('done')


def _diff_metric(job_metrics, key1, key2):
	if (key1 in job_metrics) and (key2 in job_metrics):
		return job_metrics[key1] - job_metrics[key2]


def _extract_job_metrics(job_info, task):
	result = dict.fromkeys(JobMetrics.enum_name_list, None)

	total_size_in = 0
	total_size_out = 0
	for (key, value) in job_info[JobResult.RAW].items():
		job_info_key = JobMetrics.str2enum(key.replace('TIMESTAMP', 'TS'))
		if job_info_key is not None:
			result[job_info_key] = value
		# look for file size information
		if re.match('OUTPUT_FILE_.*_SIZE', key):
			total_size_out += int(value)
		if re.match('INPUT_FILE_.*_SIZE', key):
			total_size_in += int(value)

	result[JobMetrics.FILESIZE_OUT_TOTAL] = total_size_out
	result[JobMetrics.FILESIZE_IN_TOTAL] = total_size_in

	# look for processed events, if available
	result[JobMetrics.EVENT_COUNT] = None
	if task is not None:
		max_events = int(task.get_job_dict(job_info[JobResult.JOBNUM]).get('MAX_EVENTS', -1))
		if max_events > 0:
			result[JobMetrics.EVENT_COUNT] = max_events

	return result


def _get_events(job_metrics):
	# note: can return None if no event count could be determined for the job
	return job_metrics[JobMetrics.EVENT_COUNT]


def _get_events_per_min(job_metrics):
	if (_get_runtime_payload_in_min(job_metrics) > 0) and (_get_events(job_metrics) is not None):
		return _get_events(job_metrics) / _get_runtime_payload_in_min(job_metrics)
	return None


def _get_events_per_min_timeslice(job_metrics, time_start, time_end):
	if _get_runtime_job(job_metrics) > 0:
		return _get_metric_timeslice(job_metrics, time_start, time_end,
			JobMetrics.TS_CMSSW_CMSRUN1_START, JobMetrics.TS_CMSSW_CMSRUN1_DONE,
			_get_events_per_min)


def _get_job_count(job_metrics):
	return 1.0


def _get_job_count_timeslice(job_metrics, time_start, time_end):
	return _get_metric_timeslice(job_metrics, time_start, time_end,
		JobMetrics.TS_WRAPPER_START, JobMetrics.TS_WRAPPER_DONE,
		_get_job_count)


def _get_job_rate_timeslice(job_metrics, time_start, time_end):
	return _get_metric_timeslice_cum(job_metrics, time_start, time_end,
		JobMetrics.TS_WRAPPER_START, JobMetrics.TS_WRAPPER_DONE,
		_get_job_count, use_end_time=True)


def _get_mb_per_sec_se_in(job_metrics):
	file_transfer_time = _get_runtime_se_in(job_metrics)
	if file_transfer_time > 0:
		return job_metrics[JobMetrics.FILESIZE_IN_TOTAL] / file_transfer_time


def _get_mb_per_sec_se_out(job_metrics):
	file_transfer_time = _get_runtime_se_out(job_metrics)
	if file_transfer_time > 0:
		return job_metrics[JobMetrics.FILESIZE_OUT_TOTAL] / file_transfer_time


def _get_mb_se_in(job_metrics):
	return job_metrics[JobMetrics.FILESIZE_IN_TOTAL] / 1e6


def _get_mb_se_in_timeslice(job_metrics, time_start, time_end):
	if _get_runtime_se_in(job_metrics) > 0:
		return _get_metric_timeslice_cum(job_metrics, time_start, time_end,
			JobMetrics.TS_SE_IN_START, JobMetrics.TS_SE_IN_DONE, _get_mb_se_in)


def _get_mb_se_out(job_metrics):
	return job_metrics[JobMetrics.FILESIZE_OUT_TOTAL] / 1e6


def _get_mb_se_out_timeslice(job_metrics, time_start, time_end):
	if _get_runtime_se_out(job_metrics) > 0:
		return _get_metric_timeslice_cum(job_metrics, time_start, time_end,
			JobMetrics.TS_SE_OUT_START, JobMetrics.TS_SE_OUT_DONE, _get_mb_se_out)


def _get_metric_timeslice(job_metrics, time_start, time_end,
		job_time_start_key, job_time_end_key, extract_metric):

	if _get_runtime_job(job_metrics) <= 0:
		return

	assert time_start < time_end
	(job_time_start, job_time_end) = (job_metrics[job_time_start_key], job_metrics[job_time_end_key])

	# will be positive if there is overlap to the left
	outside_left = max(0, job_time_start - time_start)
	# will be positive if there is overlap to the right
	outside_right = max(0, time_end - job_time_end)
	outside_total = outside_left + outside_right
	outside_frac = float(outside_total) / float(time_end - time_start)
	outside_frac = min(1.0, outside_frac)

	metric = extract_metric(job_metrics)
	if metric is not None:
		return metric * (1.0 - outside_frac)


def _get_metric_timeslice_cum(job_metrics, time_start, time_end,
		job_time_start_key, job_time_end_key, extract_metric, use_end_time=False):

	if _get_runtime_job(job_metrics) <= 0:
		return

	assert time_start < time_end
	(job_time_start, job_time_end) = (job_metrics[job_time_start_key], job_metrics[job_time_end_key])

	# simpler version, which does not interpolated between timeslices
	if use_end_time:
		if job_time_end < time_start:
			return extract_metric(job_metrics)
		return 0

	if job_time_start < time_end <= job_time_end:
		# current timeslice ends between job_time_start & job_time_end
		# compute ratio of covered metric
		time_covered = (job_time_end - time_end) / float(job_time_end - job_time_start)
	elif (job_time_start < time_end) and (job_time_end < time_end):
		# current timeslice ends after job_time_start & job_time_end
		time_covered = 1.0
	else:
		# current timeslice ends before job_time_start & job_time_end
		time_covered = 0.0

	return extract_metric(job_metrics) * time_covered


def _get_runtime_job(job_metrics):
	# returns the job payload runtime in seconds
	# - if a CMSSW job was run, only the time spend in the actual cmsRun call will be reported
	# - if a user job was run, the execution time of the user job will be reported
	return _diff_metric(job_metrics, JobMetrics.TS_WRAPPER_DONE, JobMetrics.TS_WRAPPER_START)


def _get_runtime_payload_in_min(job_metrics):
	cmssw_runtime = _diff_metric(job_metrics,
		JobMetrics.TS_CMSSW_CMSRUN1_DONE, JobMetrics.TS_CMSSW_CMSRUN1_START)
	if cmssw_runtime is not None:
		return cmssw_runtime / 60.
	return _diff_metric(job_metrics, JobMetrics.TS_EXECUTION_DONE, JobMetrics.TS_EXECUTION_START) / 60.


def _get_runtime_se_in(job_metrics):
	return _diff_metric(job_metrics, JobMetrics.TS_SE_IN_DONE, JobMetrics.TS_SE_IN_START)


def _get_runtime_se_out(job_metrics):
	return _diff_metric(job_metrics, JobMetrics.TS_SE_OUT_DONE, JobMetrics.TS_SE_OUT_START)


def _get_se_in_bandwidth_timeslice(job_metrics, time_start, time_end):
	if _get_runtime_se_in(job_metrics) > 0:
		return _get_metric_timeslice(job_metrics, time_start, time_end,
			JobMetrics.TS_SE_IN_START, JobMetrics.TS_SE_IN_DONE, _get_mb_per_sec_se_in)


def _get_se_in_count_timeslice(job_metrics, time_start, time_end):
	if _get_runtime_se_in(job_metrics) > 0:
		return _get_metric_timeslice(job_metrics, time_start, time_end,
			JobMetrics.TS_SE_IN_START, JobMetrics.TS_SE_IN_DONE, _get_job_count)


def _get_se_out_bandwidth_timeslice(job_metrics, time_start, time_end):
	if _get_runtime_se_out(job_metrics) > 0:
		return _get_metric_timeslice(job_metrics, time_start, time_end,
			JobMetrics.TS_SE_OUT_START, JobMetrics.TS_SE_OUT_DONE, _get_mb_per_sec_se_out)


def _get_se_out_count_timeslice(job_metrics, time_start, time_end):
	if _get_runtime_se_out(job_metrics) > 0:
		return _get_metric_timeslice(job_metrics, time_start, time_end,
			JobMetrics.TS_SE_OUT_START, JobMetrics.TS_SE_OUT_DONE, _get_job_count)
