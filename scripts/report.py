#!/usr/bin/env python
#-#  Copyright 2009-2014 Karlsruhe Institute of Technology
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

import sys, os, optparse
from gcSupport import utils, Config, TaskModule, JobManager, JobSelector, Report, parseOptions, handleException, getConfig, getJobInfo

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
#    enums = utils.makeEnum( entryNames )
#    mappingStringToEnum = dict()
	
#    for i in range(0, len ( entryNames ) entryName



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
"TIMESTAMP_WRAPPER_DONE" ] )

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
#	print jobInfo
#	print JobResultEnum.TIMESTAMP_EXECUTION_DONE
	if ( jobInfo [ JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_START ] != None ) and \
		( jobInfo [ JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_DONE ] != None ):
		return jobInfo [ JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_DONE ] - \
				jobInfo [ JobResultEnum.TIMESTAMP_CMSSW_CMSRUN1_START ]
				
	return jobInfo [ JobResultEnum.TIMESTAMP_EXECUTION_DONE ] - \
				jobInfo [ JobResultEnum.TIMESTAMP_EXECUTION_START ]

class PlotReport( Report ):
	
	
	def initHistogram ( self, name, xlabel, ylabel ):
		fig = plt.figure()

		ax = fig.add_subplot(111)
		ax.set_xlabel(r"Time (s)", ha="right", x=1)
		ax.set_ylabel(r"Bandwith (MB/s)", va="top", y=1, labelpad = 20.0)
		
		return (name, fig, ax)
	
	def finalizeHistogram( self, plotSet, useLegend = False ):
		if ( useLegend ):
			plt.legend(loc="upper right", numpoints=1, frameon=False, ncol=2)
		
		self.imageTypes = ["png", "pdf"]
		for it in self.imageTypes:
			plt.savefig( plotSet[0] + "." + it ) 

	def plotPayloadRuntime ( self, histo, jobResult ):

		runtime = []
		for res in jobResult:
			runtime = runtime + [ getPayloadRuntime( res ) ]

		thisHistType = "bar"
		#if transparent:
		#	thisHistType = "step"

		pl = plt.hist( runtime, 40 )#, color= plotColor, label = plotLabel, histtype=thisHistType )
		return pl
		

	def display(self):
		jobResult = []
		
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
			jobResult = jobResult + [jResult]
			#print jobResult
			
		
		histo =	self.initHistogram( "payload_runtime", "Payload Runtime (s)", "Count" )
		self.plotPayloadRuntime( histo, jobResult )
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

	report = Report.open(opts.reportClass, jobDB, task, selected, opts.string)
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
