#!/usr/bin/env python
# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

try:
	import numpy
except ImportError:
	numpy = None
try:
	import matplotlib
	import matplotlib.pyplot
except ImportError:
	matplotlib = None
import os, re, logging
from grid_control.output_processor import JobInfoProcessor, JobResult
from grid_control.report import Report
from grid_control.utils.data_structures import makeEnum
from python_compat import irange, izip

JobResultEnum = makeEnum([
	"TIMESTAMP_WRAPPER_START",
	"TIMESTAMP_DEPLOYMENT_START",
	"TIMESTAMP_DEPLOYMENT_DONE",
	"TIMESTAMP_SE_IN_START",
	"TIMESTAMP_SE_IN_DONE",
	"TIMESTAMP_CMSSW_CMSRUN1_START",
	"TIMESTAMP_CMSSW_CMSRUN1_DONE",
	"TIMESTAMP_EXECUTION_START",
	"TIMESTAMP_EXECUTION_DONE",
	"TIMESTAMP_SE_OUT_START",
	"TIMESTAMP_SE_OUT_DONE",
	"TIMESTAMP_WRAPPER_DONE",
	"FILESIZE_IN_TOTAL",
	"FILESIZE_OUT_TOTAL",
	"EVENT_COUNT"])


def extractJobTiming(jInfo, task):
	jobResult = dict()
	jobNum = jInfo[JobResult.JOBNUM]

	# intialize all with None
	for key in JobResultEnum.enumNames:
		enumID = JobResultEnum.str2enum(key)
		jobResult[enumID] = None

	total_size_in = 0
	total_size_out = 0
	for (key, val) in jInfo[JobResult.RAW].items():
		enumID = JobResultEnum.str2enum(key)
		if enumID is not None:
			jobResult[enumID] = val

		# look for file size information
		if re.match("OUTPUT_FILE_._SIZE", key):
			total_size_out += int(val)
		if re.match("INPUT_FILE_._SIZE", key):
			total_size_in += int(val)

	jobResult[JobResultEnum.FILESIZE_OUT_TOTAL] = total_size_out
	jobResult[JobResultEnum.FILESIZE_IN_TOTAL] = total_size_in

	# look for processed events, if available
	if task is not None:
		jobConfig = task.getJobConfig(jobNum)
		jobResult[JobResultEnum.EVENT_COUNT] = int(jobConfig["MAX_EVENTS"])
		# make sure an undefined max event count (-1 in cmssw) is treated
		# as unkown event count
		if jobResult[JobResultEnum.EVENT_COUNT] < 0:
			jobResult[JobResultEnum.EVENT_COUNT] = None
	else:
		jobResult[JobResultEnum.EVENT_COUNT] = None

	return jobResult


# returns the job payload runtime in seconds
# - if a CMSSW job was run, only the time spend in the actual
#   cmsRun call will be reported
# - if a user job was run, the execution time of the user job
#   will be reported
def getPayloadRuntime(jobInfo):
	if (jobInfo[JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_START] is not None) and \
			(jobInfo[JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_DONE] is not None):
		return jobInfo[JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_DONE] - \
			jobInfo[JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_START]

	return jobInfo[JobResultEnum.TIMESTAMP_EXECUTION_DONE] - \
		jobInfo[JobResultEnum.TIMESTAMP_EXECUTION_START]


def getSeOutRuntime(jobInfo):
	return jobInfo[JobResultEnum.TIMESTAMP_SE_OUT_DONE] - \
		jobInfo[JobResultEnum.TIMESTAMP_SE_OUT_START]


def getSeInRuntime(jobInfo):
	return jobInfo[JobResultEnum.TIMESTAMP_SE_IN_DONE] - \
		jobInfo[JobResultEnum.TIMESTAMP_SE_IN_START]


def getJobRuntime(jobInfo):
	return jobInfo[JobResultEnum.TIMESTAMP_WRAPPER_DONE] - \
		jobInfo[JobResultEnum.TIMESTAMP_WRAPPER_START]


# note: can return None if no event count could be
# determined for the job
def getEventCount(jobInfo):
	return jobInfo[JobResultEnum.EVENT_COUNT]


def getEventRate(jobInfo):
	if (getPayloadRuntime(jobInfo) > 0) and (getEventCount(jobInfo) is not None):
		return getEventCount(jobInfo) / (getPayloadRuntime(jobInfo) / 60.0)
	else:
		return None


def getSeOutBandwidth(jobInfo):
	seOutTime = getSeOutRuntime(jobInfo)
	fileSize = jobInfo[JobResultEnum.FILESIZE_OUT_TOTAL]

	if seOutTime > 0:
		return fileSize / seOutTime
	else:
		return None


# in MB
def getSeOutFilesize(jobInfo):
	return jobInfo[JobResultEnum.FILESIZE_OUT_TOTAL] / 1000000.0


# in MB
def getSeInFilesize(jobInfo):
	return jobInfo[JobResultEnum.FILESIZE_IN_TOTAL] / 1000000.0


def getSeInBandwidth(jobInfo):
	seInTime = getSeInRuntime(jobInfo)
	fileSize = jobInfo[JobResultEnum.FILESIZE_IN_TOTAL]
	if seInTime > 0:
		return fileSize / seInTime


def getJobCount(jobInfo):
	return 1.0


def getSeOutAverageBandwithAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getSeOutRuntime(jobInfo) > 0:
		return getQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_SE_OUT_START], jinf[JobResultEnum.TIMESTAMP_SE_OUT_DONE]),
			getSeOutBandwidth)


def getSeOutActiveAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getSeOutRuntime(jobInfo) > 0:
		return getQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_SE_OUT_START], jinf[JobResultEnum.TIMESTAMP_SE_OUT_DONE]),
			getJobCount)


def getSeInAverageBandwithAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getSeInRuntime(jobInfo) > 0:
		return getQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_SE_IN_START], jinf[JobResultEnum.TIMESTAMP_SE_IN_DONE]),
			getSeInBandwidth)


def getSeInActiveAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getSeInRuntime(jobInfo) > 0:
		return getQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_SE_IN_START], jinf[JobResultEnum.TIMESTAMP_SE_IN_DONE]),
			getJobCount)


# cumulated transfer sizes

def getSeOutSizeAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getSeOutRuntime(jobInfo) > 0:
		return getCumQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_SE_OUT_START], jinf[JobResultEnum.TIMESTAMP_SE_OUT_DONE]),
			getSeOutFilesize)


def getSeInSizeAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getSeInRuntime(jobInfo) > 0:
		return getCumQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_SE_IN_START], jinf[JobResultEnum.TIMESTAMP_SE_IN_DONE]),
			getSeInFilesize)


def getJobActiveAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getJobRuntime(jobInfo) > 0:
		return getQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_WRAPPER_START], jinf[JobResultEnum.TIMESTAMP_WRAPPER_DONE]),
			getJobCount)


def getEventRateAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getJobRuntime(jobInfo) > 0:
		return getQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_START], jinf[JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_DONE]),
			getEventRate)


def getCompleteRateAtTimeSpan(jobInfo, timeStart, timeEnd):
	if getJobRuntime(jobInfo) > 0:
		return getCumQuantityAtTimeSpan(jobInfo, timeStart, timeEnd,
			lambda jinf: (jinf[JobResultEnum.TIMESTAMP_WRAPPER_START], jinf[JobResultEnum.TIMESTAMP_WRAPPER_DONE]),
			getJobCount, useEndtime=True)


def getQuantityAtTimeSpan(jobInfo, timeStart, timeEnd, timingExtract, quantityExtract):
	assert (timeStart <= timeEnd)

	(theStart, theEnd) = timingExtract(jobInfo)

	# will be positive if there is overlap to the left
	leftOutside = max(0, theStart - timeStart)
	# will be positive if there is overlap to the right
	rightOutside = max(0, timeEnd - theEnd)
	totalOutside = leftOutside + rightOutside
	fractionOutside = float(totalOutside) / float(timeEnd - timeStart)
	fractionOutside = min(1.0, fractionOutside)

	q = quantityExtract(jobInfo)
	if q is not None:
		return q * (1.0 - fractionOutside)


# note: timeStart must be before all timestamps evaluated here
def getCumQuantityAtTimeSpan(jobInfo, timeStart, timeEnd, timingExtract, quantityExtract, useEndtime=False):
	assert (timeStart < timeEnd)

	(theStart, theEnd) = timingExtract(jobInfo)

	# simpler version, which does not interpolated between timeslices
	if useEndtime:
		if theEnd < timeStart:
			return quantityExtract(jobInfo)
		else:
			return 0

	theSpan = theEnd - theStart
	distFront = theStart - timeEnd
	distBack = theEnd - timeEnd

	# current timeslice ends between theStart & theEnd
	# compute ratio of covered quantity
	if distFront < 0 and distBack >= 0:
		partCovered = distBack / theSpan
	# current timeslice ends after theStart & theEnd
	elif distFront < 0 and distBack < 0:
		partCovered = 1.0
	# current timeslice ends before theStart & theEnd
	else:
		partCovered = 0.0

	return quantityExtract(jobInfo) * partCovered


class PlotReport(Report):
	alias = ['plot']

	def initHistogram(self, name, xlabel, ylabel):
		fig = matplotlib.pyplot.figure()

		ax = fig.add_subplot(111)
		ax.set_xlabel(xlabel) # ha="left"
		# y = 0.8 will move the label more to the center
		ax.set_ylabel(ylabel, va="top", y=0.75, labelpad=20.0)

		return (name, fig, ax)

	def finalizeHistogram(self, plotSet, useLegend=False):
		if useLegend:
			matplotlib.pyplot.legend(loc="upper right", numpoints=1, frameon=False, ncol=2)

		imageTypes = ["png", "pdf"]
		for it in imageTypes:
			matplotlib.pyplot.savefig(plotSet[0] + "." + it)

	def plotHistogram(self, histo, jobResult, extractor):
		log = logging.getLogger('report.plotreport')
		log.info("Plotting " + histo[0] + " ...")
		runtime = []
		for res in jobResult:
			val = extractor(res)
			if val is not None:
				runtime.append(val)

		if len(runtime) == 0:
			log.info("Skipping " + histo[0] + ", no input data")
			return None

		#thisHistType = "bar"
		# if transparent:
		#	thisHistType = "step"

		pl = matplotlib.pyplot.hist(runtime, 40)  # , color= plotColor, label = plotLabel, histtype=thisHistType)
		# histo[2].set_ymargin(0.4)
		log.info("done")
		return pl

	def plotOverall(self, histo, jInfos, timespan, extractor, fit=False, unit="MB/s", cumulate=False):
		log = logging.getLogger('report.plotreport')
		log.info("Plotting " + histo[0] + " ...")

		trunctationFractionFront = 0.05
		trunctationFractionBack = 0.3

		relTimeSpan = max(timespan) - min(timespan)
		# compute the amount of slices, for a small timespan, use every step
		# for large timespans, use 1000 slices at most
		stepSize = int(relTimeSpan / min(1000.0, relTimeSpan))

		(timeStep, overAllBandwidth) = \
			self.collectData(min(timespan), max(timespan), stepSize, cumulate, extractor, jInfos)

		self.plotOverallAll(histo, timeStep, overAllBandwidth)
		if fit:
			# not the first and last 5 percent, used for fitting
			(timeStep, overAllBandwidth) = self.truncateData(timeStep, overAllBandwidth,
				relTimeSpan * trunctationFractionFront, relTimeSpan * (1.0 - trunctationFractionBack))
			if timeStep and overAllBandwidth:
				self.plotOverallTruncated(timeStep, overAllBandwidth, unit, trunctationFractionFront, trunctationFractionBack, relTimeSpan)
			else:
				log.info("Skipping fit due to the lack of input data")
		log.info("done")

	def plotOverallAll(self, histo, timeStep, overAllBandwidth):
		# make sure the axis are not exactly the same
		minY = min(overAllBandwidth)
		maxY = max(overAllBandwidth)
		if maxY <= minY:
			maxY = minY + 1.0

		histo[2].set_ylim(bottom= minY * 0.99, top=maxY * 1.2)
		histo[2].set_xlim(left=min(timeStep) * 0.99, right=max(timeStep) * 1.01)
		matplotlib.pyplot.plot(timeStep, overAllBandwidth, color="green")

	def collectData(self, minTime, maxTime, stepSize, cumulate, extractor, jInfos):
		timeStep = []
		overAllBandwidth = []
		for i in irange(minTime, maxTime + 1, stepSize):
			thisBw = 0
			currentTimeStep = i - minTime
			timeStep.append(currentTimeStep)

			for jinfo in jInfos:
				if cumulate:
					val = extractor(jinfo, minTime, i + 1)
				else:
					val = extractor(jinfo, i, i + stepSize)

				if val is not None:
					thisBw += val

			overAllBandwidth.append(thisBw)
		return (timeStep, overAllBandwidth)

	def truncateData(self, timeStep, overAllBandwidth, truncFront, truncBack):
		truncatedTimeStep = []
		truncatedOverAllBandwidth = []
		for currentTimeStep, thisBw in izip(timeStep, overAllBandwidth):
			if (currentTimeStep > truncFront) and (currentTimeStep < truncBack):
				truncatedOverAllBandwidth.append(thisBw)
				truncatedTimeStep.append(currentTimeStep)
		return (truncatedTimeStep, truncatedOverAllBandwidth)

	def plotOverallTruncated(self, truncatedTimeStep, truncatedOverAllBandwidth, unit, trunctationFractionFront, trunctationFractionBack, relTimeSpan):
		fitRes = numpy.polyfit(truncatedTimeStep, truncatedOverAllBandwidth, 0)

		avgVal = fitRes[0]
		matplotlib.pyplot.axhline(y=avgVal, xmin=trunctationFractionFront, xmax=1.0 - trunctationFractionBack,
			color="black", lw=2)

		matplotlib.pyplot.annotate("%.2f" % avgVal + " " + unit, xy=(relTimeSpan * 0.7, avgVal),
			xytext=(relTimeSpan * 0.75, avgVal * 0.85), backgroundcolor="gray")

	def handleMinMaxTiming(self, jResult, minTime, maxTime, stampStart, stampDone):
		if (minTime is None) or (maxTime is None):
			minTime = jResult[stampStart]
			maxTime = jResult[stampDone]
		else:
			minTime = min(minTime, jResult[stampStart])
			maxTime = max(maxTime, jResult[stampDone])

		return(minTime, maxTime)

	def produceHistogram(self, naming, lambdaExtractor):
		histogram = self.initHistogram(naming[0], naming[1], naming[2])
		self.plotHistogram(histogram, self.jobResult, lambdaExtractor)
		self.finalizeHistogram(histogram)

	def produceOverallGraph(self, naming, timespan, lambdaExtractor, fit=False, unit="MB/s", cumulate=False):
		log = logging.getLogger('report.plotreport')
		if (timespan[0] == timespan[1]) or (timespan[0] is None) or (timespan[1] is None):
			log.info("Skipping plot %s because no timespan is available", naming)
			return
		histogram = self.initHistogram(naming[0], naming[1], naming[2])
		self.plotOverall(histogram, self.jobResult, timespan, lambdaExtractor, fit, unit, cumulate)
		self.finalizeHistogram(histogram)

	def display(self):
		if not numpy:
			raise Exception('Unable to find numpy!')
		if not matplotlib:
			raise Exception('Unable to find matplotlib!')
		self.jobResult = []
		log = logging.getLogger('report.plotreport')
		log.info(str(len(self._jobs)) + " job(s) selected for plots")

		# larger default fonts
		matplotlib.rcParams.update({'font.size': 16})

		minSeOutTime = None
		maxSeOutTime = None

		minSeInTime = None
		maxSeInTime = None

		minWrapperTime = None
		maxWrapperTime = None

		minCmsswTime = None
		maxCmsswTime = None

		workdir = self._jobDB.getWorkPath()
		for j in self._jobs:
			try:
				jInfo = JobInfoProcessor().process(os.path.join(workdir, 'output', 'job_%d' % j))
			except Exception:
				log.info("Ignoring job")
				continue

			jResult = extractJobTiming(jInfo, self._task)
			self.jobResult.append(jResult)

			(minSeInTime, maxSeInTime) = self.handleMinMaxTiming(jResult, minSeInTime, maxSeInTime,
				JobResultEnum.TIMESTAMP_SE_IN_START, JobResultEnum.TIMESTAMP_SE_IN_DONE)
			(minSeOutTime, maxSeOutTime) = self.handleMinMaxTiming(jResult, minSeOutTime, maxSeOutTime,
				JobResultEnum.TIMESTAMP_SE_OUT_START, JobResultEnum.TIMESTAMP_SE_OUT_DONE)
			(minWrapperTime, maxWrapperTime) = self.handleMinMaxTiming(jResult, minWrapperTime, maxWrapperTime,
				JobResultEnum.TIMESTAMP_WRAPPER_START, JobResultEnum.TIMESTAMP_WRAPPER_DONE)
			(minCmsswTime, maxCmsswTime) = self.handleMinMaxTiming(jResult, minCmsswTime, maxCmsswTime,
				JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_START, JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_DONE)

		self.produceHistogram(("payload_runtime", "Payload Runtime (min)", "Count"),
			lambda x: getPayloadRuntime(x) / 60.0)
		self.produceHistogram(("event_per_job", "Event per Job", "Count"), getEventCount)
		self.produceHistogram(("event_rate", "Event Rate (Events/min)", "Count"), getEventRate)
		self.produceHistogram(("se_in_runtime", "SE In Runtime (s)", "Count"), getSeInRuntime)
		self.produceHistogram(("se_in_size", "SE IN Size (MB)", "Count"), getSeInFilesize)
		self.produceHistogram(("se_out_runtime", "SE OUT Runtime (s)", "Count"), getSeOutRuntime)
		self.produceHistogram(("se_out_bandwidth", "SE OUT Bandwidth (MB/s)", "Count"), getSeOutBandwidth)
		self.produceHistogram(("se_out_size", "SE OUT Size (MB)", "Count"), getSeOutFilesize)
		self.produceHistogram(("se_out_runtime", "SE Out Runtime (s)", "Count"), getSeOutRuntime)

		# job active & complete
		self.produceOverallGraph(("job_active_total", "Time (s)", "Jobs Active"),
			(minWrapperTime, maxWrapperTime), getJobActiveAtTimeSpan)
		self.produceOverallGraph(("job_complete_total", "Time (s)", "Jobs Complete"),
			(minWrapperTime, maxWrapperTime), getCompleteRateAtTimeSpan)
		# stage out active & bandwidth
		self.produceOverallGraph(("se_out_bandwidth_total", "Time (s)", "Total SE OUT Bandwidth (MB/s)"),
			(minSeOutTime, maxSeOutTime), getSeOutAverageBandwithAtTimeSpan, fit=True)
		self.produceOverallGraph(("se_out_active_total", "Time (s)", "Active Stageouts"),
			(minSeOutTime, maxSeOutTime), getSeOutActiveAtTimeSpan)
		# stage in active & bandwidth
		self.produceOverallGraph(("se_in_bandwidth_total", "Time (s)", "Total SE IN Bandwidth (MB/s)"),
			(minSeInTime, maxSeInTime), getSeInAverageBandwithAtTimeSpan, fit=True)
		self.produceOverallGraph(("se_in_active_total", "Time (s)", "Active Stageins"),
			(minSeInTime, maxSeInTime), getSeInActiveAtTimeSpan)
		# total stageout size
		self.produceOverallGraph(("se_out_cum_size", "Time (s)", "Stageout Cumulated Size (MB)"),
			(minSeOutTime, maxSeOutTime), getSeOutSizeAtTimeSpan, cumulate=True)
		# total stagein size
		self.produceOverallGraph(("se_in_cum_size", "Time (s)", "Stagein Cumulated Size (MB)"),
			(minSeInTime, maxSeInTime), getSeInSizeAtTimeSpan, cumulate=True)
		# event rate
		if (minCmsswTime is not None) and (maxCmsswTime is not None):
			self.produceOverallGraph(("event_rate_total", "Time (s)", "Event Rate (Events/min)"),
				(minCmsswTime, maxCmsswTime), getEventRateAtTimeSpan)
		else:
			log.info("Skipping event_rate_total")
