#!/usr/bin/env python
#-#  Copyright 2009-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#	  http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import sys, os, optparse
from gcSupport import utils, TaskModule, JobManager, JobSelector, Report, parseOptions, handleException, getConfig, getJobInfo

parser = optparse.OptionParser()
parser.add_option('', '--report', dest='reportClass', default='GUIReport')
parser.add_option('-J', '--job-selector', dest='selector', default=None)
parser.add_option('-T', '--use-task', dest='useTask', default=False, action='store_true',
	help='Forward task information to report')
parser.add_option('', '--str', dest='string', default=None)
#parser.add_option('-m', '--map', dest='showMap', default=False, action='store_true',
#	help='Draw map of sites')
#parser.add_option('-C', '--cpu', dest='showCPU', default=False, action='store_true',
#	help='Display time overview')
#Report.addOptions(parser)
(opts, args) = parseOptions(parser)


class JobInfo ( object ) :
	def __init__( self, payloadStart, payloadDone, seStart, seDone, outputSize, eventCount, jobFailed ): 
		self.payloadStart = payloadStart
		self.payloadDone = payloadDone
		self.seStart = seStart
		self.seDone = seDone
		self.outputSize = outputSize
		self.eventCount = eventCount
		self.jobFailed = jobFailed

	## todo write test code
	def seAverageBandwithAtTimeSpan(self, timeStart, timeEnd):
		
		assert ( timeStart < timeEnd )
		
		#print " time start " + str(timeStart) + " time end " + str(timeEnd)
		
		## will be positive if there is overlap to the left
		leftOutside = max(0, self.seStart - timeStart )
		#print " -leftOutside- " + str(leftOutside)
		## will be positive if there is overlap to the right
		rightOutside = max(0, timeEnd - self.seDone )
		#print " -rightOutside- " + str(rightOutside)
		
		totalOutside = leftOutside + rightOutside
		fractionOutside = float( totalOutside ) / float ( timeEnd - timeStart )
		fractionOutside = min (1.0, fractionOutside)
		#print timeEnd - timeStart
		#print fractionOutside
	
		return self.seBandwidth() * ( 1.0 - fractionOutside )

	def seStageOutActive(self, timeStart, timeEnd):
		assert ( timeStart < timeEnd )
		
		#print " time start " + str(timeStart) + " time end " + str(timeEnd)
		
		## will be positive if there is overlap to the left
		leftOutside = max(0, self.seStart - timeStart )
		#print " -leftOutside- " + str(leftOutside)
		## will be positive if there is overlap to the right
		rightOutside = max(0, timeEnd - self.seDone )
		#print " -rightOutside- " + str(rightOutside)
		
		totalOutside = leftOutside + rightOutside
		fractionOutside = float( totalOutside ) / float ( timeEnd - timeStart )
		fractionOutside = min (1.0, fractionOutside)
		#print timeEnd - timeStart
		#print fractionOutside
	
		return 1.0 * ( 1.0 - fractionOutside )

	# in MB / second
	def seBandwidth(self):
		return ( self.outputSize / 1000000.0 ) / self.seRuntime()

	def payloadRuntime(self):
		return self.payloadDone - self.payloadStart

	def seRuntime(self):
		return self.seDone - self.seStart


#def makeEnumAndMapping( entryNames )
#	enums = utils.makeEnum( entryNames )
#	mappingStringToEnum = dict()
	
#	for i in range(0, len ( entryNames ) entryName



TIMESTAMP_WRAPPER_START=1415358689
TIMESTAMP_DEPLOYMENT_START=1415358689
TIMESTAMP_DEPLOYMENT_DONE=1415358689
TIMESTAMP_SE_IN_START=1415358689
TIMESTAMP_SE_IN_DONE=1415358689
TIMESTAMP_EXECUTION_START=1415358689
TIMESTAMP_EXECUTION_DONE=1415358694
TIMESTAMP_SE_OUT_START=1415358694
TIMESTAMP_SE_OUT_DONE=1415358694
TIMESTAMP_WRAPPER_DONE=1415358694

JobResultEnum = utils.makeEnum( [ "TIMESTAMP_WRAPPER_START", 
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
"FILESIZE_OUT_TOTAL" ] )

import matplotlib
import matplotlib.pyplot as plt

def extractJobTiming( jInfo ):
	jobResult = dict()
	
	#intialize all with None
	for key in JobResultEnum.members:
		enumID = JobResultEnum.fromString( key )
		jobResult[ enumID ] = None
	
	for ( key, val ) in jInfo.iteritems():
		enumID = JobResultEnum.fromString( key )
		if ( enumID != None ):
			jobResult[ enumID ] = val

	return jobResult
	#print JobResultEnum.members
	#pi = JobResultEnum.members.index("payloadDone")
	#myEnum = JobResultEnum
	#myEnum.setFromKey( "outputSize" )
	#print myEnum.fromString( "TIMESTAMP_WRAPPER_DONE" )
	#print timingDict


# returns the job payload runtime in seconds
# - if a CMSSW job was run, only the time spend in the actual
#   cmsRun call will be reported
# - if a user job was run, the execution time of the user job 
#   will be reported
def getPayloadRuntime ( jobInfo ):
	if ( jobInfo [ JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_START ] != None ) and \
		( jobInfo [ JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_DONE ] != None ):
		return jobInfo [ JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_DONE ] - \
				jobInfo [ JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_START ]
				
	return jobInfo [ JobResultEnum.TIMESTAMP_EXECUTION_DONE ] - \
				jobInfo [ JobResultEnum.TIMESTAMP_EXECUTION_START ]

def getSeOutRuntime ( jobInfo ):
	return jobInfo [ JobResultEnum.TIMESTAMP_SE_OUT_DONE ] - \
				jobInfo [ JobResultEnum.TIMESTAMP_SE_OUT_START ]

def getSeInRuntime ( jobInfo ):
	return jobInfo [ JobResultEnum.TIMESTAMP_SE_IN_DONE ] - \
				jobInfo [ JobResultEnum.TIMESTAMP_SE_IN_START ]

def getJobRuntime ( jobInfo ):
	return jobInfo [ JobResultEnum.TIMESTAMP_WRAPPER_DONE ] - \
				jobInfo [ JobResultEnum.TIMESTAMP_WRAPPER_START ]

def getSeOutBandwidth ( jobInfo ):
	
	seOutTime = getSeOutRuntime( jobInfo )
	fileSize =  jobInfo [ JobResultEnum.FILESIZE_OUT_TOTAL ]
	
	if ( seOutTime > 0 ) :
		return fileSize / seOutTime
	else:
		return None

def getSeInBandwidth ( jobInfo ):
	
	seInTime = getSeInRuntime( jobInfo )
	fileSize =  jobInfo [ JobResultEnum.FILESIZE_IN_TOTAL ]
	
	if ( seInTime > 0 ) :
		return fileSize / seInTime
	else:
		return None

def getSeOutAverageBandwithAtTimeSpan(jobInfo, timeStart, timeEnd):	
	if getSeOutRuntime( jobInfo ) > 0:	
		return getQuantityAtTimeSpan( jobInfo, timeStart, timeEnd, \
			lambda jinf : (jinf [ JobResultEnum.TIMESTAMP_SE_OUT_START ], jinf [ JobResultEnum.TIMESTAMP_SE_OUT_DONE ] ), \
			lambda jinf : getSeOutBandwidth(jinf) )
	else:
		return None

def getSeOutActiveAtTimeSpan(jobInfo, timeStart, timeEnd):	
	if getSeOutRuntime( jobInfo ) > 0:	
		return getQuantityAtTimeSpan( jobInfo, timeStart, timeEnd, \
			lambda jinf : (jinf [ JobResultEnum.TIMESTAMP_SE_OUT_START ], jinf [ JobResultEnum.TIMESTAMP_SE_OUT_DONE ] ), \
			lambda jinf : 1.0 )
	else:
		return None
		
def getSeInAverageBandwithAtTimeSpan(jobInfo, timeStart, timeEnd):	
	if getSeInRuntime( jobInfo ) > 0:	
		return getQuantityAtTimeSpan( jobInfo, timeStart, timeEnd, \
			lambda jinf : (jinf [ JobResultEnum.TIMESTAMP_SE_IN_START ], jinf [ JobResultEnum.TIMESTAMP_SE_IN_DONE ] ), \
			lambda jinf : getSeInBandwidth(jinf) )
	else:
		return None
		
def getSeInActiveAtTimeSpan(jobInfo, timeStart, timeEnd):	
	if getSeInRuntime( jobInfo ) > 0:	
		return getQuantityAtTimeSpan( jobInfo, timeStart, timeEnd, \
			lambda jinf : (jinf [ JobResultEnum.TIMESTAMP_SE_IN_START ], jinf [ JobResultEnum.TIMESTAMP_SE_IN_DONE ] ), \
			lambda jinf : 1.0 )
	else:
		return None
		
def getJobActiveAtTimeSpan(jobInfo, timeStart, timeEnd):	
	if getJobRuntime( jobInfo ) > 0:	
		return getQuantityAtTimeSpan( jobInfo, timeStart, timeEnd, \
			lambda jinf : (jinf [ JobResultEnum.TIMESTAMP_WRAPPER_START ], jinf [ JobResultEnum.TIMESTAMP_WRAPPER_DONE ] ), \
			lambda jinf : 1.0 )
	else:
		return None

def getQuantityAtTimeSpan(jobInfo, timeStart, timeEnd, timingExtract, quantityExtract ):	
	assert ( timeStart < timeEnd )

	(theStart, theEnd) = timingExtract ( jobInfo )

	## will be positive if there is overlap to the left
	leftOutside = max(0, theStart - timeStart )
	## will be positive if there is overlap to the right
	rightOutside = max(0, timeEnd - theEnd )
	totalOutside = leftOutside + rightOutside
	fractionOutside = float( totalOutside ) / float ( timeEnd - timeStart )
	fractionOutside = min (1.0, fractionOutside)

	return quantityExtract(jobInfo) * ( 1.0 - fractionOutside )

class PlotReport( Report ):
	
	def initHistogram ( self, name, xlabel, ylabel ):
		fig = plt.figure()

		ax = fig.add_subplot(111)
		ax.set_xlabel(xlabel)#, ha="left" )
		# y = 0.8 will move the label more to the center
		ax.set_ylabel(ylabel, va="top", y=0.75, labelpad = 20.0)
				
		return (name, fig, ax)
	
	def finalizeHistogram( self, plotSet, useLegend = False ):
		if ( useLegend ):
			plt.legend(loc="upper right", numpoints=1, frameon=False, ncol=2)
		
		self.imageTypes = ["png", "pdf"]
		for it in self.imageTypes:
			plt.savefig( plotSet[0] + "." + it ) 

	def plotHistogram ( self, histo, jobResult, extractor ):

		runtime = []
		for res in jobResult:
			val = extractor( res )
			if val != None:
				runtime = runtime + [ val ]

		if len( runtime ) == 0:
			print "Skipping " + histo[0] + ", no input data"
			return None

		thisHistType = "bar"
		#if transparent:
		#	thisHistType = "step"
		
		pl = plt.hist( runtime, 40 )#, color= plotColor, label = plotLabel, histtype=thisHistType )
		#histo[2].set_ymargin( 0.4 )
		return pl
		
	def plotOverall( self, histo, jInfos, minSeTime, maxSeTime, extractor ):
		overAllBandwith = []
		timeStep = []
		
		stepSize = 1# int( ( maxSeTime - minSeTime) / 10000.0 )

		for i in xrange( minSeTime, maxSeTime + 1, stepSize ):

			thisBw = 0
			timeStep = timeStep + [ i - minSeTime ]

			for jinfo in jInfos:
				#if ( jinfo.jobFailed ):
				#	continue		
				
				val = extractor( jinfo, i, i + stepSize )
				#print getSeOutRuntime(jinfo)
				if val != None:
					#runtime = runtime + [ ( jinfo.eventCount ) / ( jinfo.payloadRuntime() / 60.0 ) ]
					#print getSeAverageBandwithAtTimeSpan( jinfo, i, i + stepSize )
					thisBw = thisBw + val
					
			overAllBandwith = overAllBandwith + [thisBw]

		histo[2].set_ylim( bottom = min(overAllBandwith)*0.99, top = max(overAllBandwith) * 1.2 )
		histo[2].set_xlim( left = min(timeStep)*0.99, right = max(timeStep) * 1.01 )
		#print overAllBandwith
		pl = plt.plot( timeStep, overAllBandwith, color="green" )

		
	def display(self):
		jobResult = []
		
		#larger default fonts
		matplotlib.rcParams.update({'font.size': 16})
		
		minSeOutTime = None
		maxSeOutTime = None
		
		allJobs = self._jobDB.getJobs()
		workdir = os.path.join( self._jobDB._dbPath, ".." ) 
		for j in allJobs:
			job = self._jobDB.get( j )
			#print job.state
			
			jInfo = getJobInfo ( workdir, j, preserveCase = True )
			#print jInfo
			if ( jInfo == None ):
				continue
				
			jResult = extractJobTiming( jInfo )
			
			# todo: read from job info
			jResult[ JobResultEnum.FILESIZE_IN_TOTAL ] = 3000.0
			jResult[ JobResultEnum.FILESIZE_OUT_TOTAL ] = 3000.0
			
			jobResult = jobResult + [jResult]
			#print jobResult
			
			if (minSeOutTime == None) or ( maxSeOutTime == None ):
				minSeOutTime = jResult [ JobResultEnum.TIMESTAMP_SE_OUT_START ]
				maxSeOutTime = jResult [ JobResultEnum.TIMESTAMP_SE_OUT_DONE ]
			else:
				minSeOutTime = min ( minSeOutTime, jResult [ JobResultEnum.TIMESTAMP_SE_OUT_START ] )
				maxSeOutTime = max ( maxSeOutTime, jResult [ JobResultEnum.TIMESTAMP_SE_OUT_DONE ] )
			
		print str( minSeOutTime ) + " : " + str( maxSeOutTime ) 
		
		histo =	self.initHistogram( "payload_runtime", "Payload Runtime (s)", "Count" )
		self.plotHistogram( histo, jobResult, lambda x: getPayloadRuntime( x ) )
		self.finalizeHistogram( histo )

		histo =	self.initHistogram( "se_out_runtime", "SE Out Runtime (s)", "Count" )
		self.plotHistogram( histo, jobResult, lambda x: getSeOutRuntime( x ) )
		self.finalizeHistogram( histo )
		
		histo =	self.initHistogram( "se_in_runtime", "SE In Runtime (s)", "Count" )
		self.plotHistogram( histo, jobResult, lambda x: getSeInRuntime( x ) )
		self.finalizeHistogram( histo )

		histo =	self.initHistogram( "se_out_runtime", "SE OUT Runtime (s)", "Count" )
		self.plotHistogram( histo, jobResult, lambda x: getSeOutRuntime( x ) )
		self.finalizeHistogram( histo )

		histo =	self.initHistogram( "se_out_bandwidth", "SE OUT Bandwidth (MB/s)", "Count" )
		self.plotHistogram( histo, jobResult, lambda x: getSeOutBandwidth( x ) )
		self.finalizeHistogram( histo )
		
		# job active
		histo =	self.initHistogram( "job_active_total", "Time (s)", "Active Jobs" )
		self.plotOverall( histo, jobResult, minSeOutTime, maxSeOutTime, \
			lambda x, i, ip: getJobActiveAtTimeSpan(x,i,ip) )
		self.finalizeHistogram( histo )

		# stage out active & bandwidth
		histo =	self.initHistogram( "se_out_bandwidth_total", "Time (s)", "Total SE OUT Bandwidth (MB/s)" )
		self.plotOverall( histo, jobResult, minSeOutTime, maxSeOutTime, \
			lambda x, i, ip: getSeOutAverageBandwithAtTimeSpan(x,i,ip) )
		self.finalizeHistogram( histo )

		histo =	self.initHistogram( "se_out_active_total", "Time (s)", "Active Stageouts" )
		self.plotOverall( histo, jobResult, minSeOutTime, maxSeOutTime, \
			lambda x, i, ip: getSeOutActiveAtTimeSpan(x,i,ip) )
		self.finalizeHistogram( histo )

		# stage in active & bandwidth
		histo =	self.initHistogram( "se_in_bandwidth_total", "Time (s)", "Total SE IN Bandwidth (MB/s)" )
		self.plotOverall( histo, jobResult, minSeOutTime, maxSeOutTime, \
			lambda x, i, ip: getSeInAverageBandwithAtTimeSpan(x,i,ip) )
		self.finalizeHistogram( histo )

		histo =	self.initHistogram( "se_in_active_total", "Time (s)", "Active Stageins" )
		self.plotOverall( histo, jobResult, minSeOutTime, maxSeOutTime, \
			lambda x, i, ip: getSeInActiveAtTimeSpan(x,i,ip) )
		self.finalizeHistogram( histo )

if len(args) != 1:
	utils.exitWithUsage('%s [options] <config file>' % sys.argv[0])

def main():
	# try to open config file
	config = getConfig(args[0], section = 'global')

	# Initialise task module
	task = None
	tags = []
	if opts.useTask:
		task = config.getClass(['task', 'module'], cls = TaskModule).getInstance()
		tags = [task]

	# Initialise job database
	jobManagerCls = config.getClass('job manager', 'SimpleJobManager', cls = JobManager, tags = tags)
	jobDB = jobManagerCls.getInstance(task, None).jobDB
	log = utils.ActivityLog('Filtering job entries')
	selected = jobDB.getJobs(JobSelector.create(opts.selector, task = task))
	del log

	report = Report.getInstance(opts.reportClass, jobDB, task, selected, opts.string)
	report.display()

	sys.exit()

	# Show reports
	report = Report(jobs, selected)
	if opts.showCPU:
		cpuTime = 0
		for jobNum in selected:
			jobObj = jobs.get(jobNum)
			cpuTime += jobObj.get('runtime', 0)
		print 'Used wall time:', utils.strTime(cpuTime)
		print 'Estimated cost: $%.2f' % ((cpuTime / 60 / 60) * 0.1)
	elif opts.showMap:
		from grid_control_gui import geomap
		geomap.drawMap(report)
	else:
		report.show(opts, task)

if __name__ == '__main__':
	handleException(main)
