#!/usr/bin/python

"""
This is the Dashboard API Module for the Worker Node
"""
import os, sys, time
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import apmon
sys.path.pop()
#
# Methods for manipulating the apmon instance
#

# Config attributes
apmonUseUrl = False

# Internal attributes
apmonInstance = None
apmonInit = False

# Monalisa configuration
apmonUrlList = ["http://lxgate35.cern.ch:40808/ApMonConf?app=dashboard", \
                "http://monalisa.cacr.caltech.edu:40808/ApMonConf?app=dashboard"]
apmonConf = {'cms-jobmon.cern.ch:8884': {'sys_monitoring' : 0, \
                                    'general_info'   : 0, \
                                    'job_monitoring' : 0} }
apmonLoggingLevel = apmon.Logger.FATAL

#
# Method to create a single apmon instance at a time
#
def getApmonInstance():
    global apmonInstance
    global apmonInit
    if apmonInstance is None and not apmonInit :
        apmonInit = True
        if apmonUseUrl :
            apm = None
            #print "Creating ApMon with dynamic configuration/url"
            try :
                apm = apmon.ApMon(apmonUrlList, apmonLoggingLevel);
            except Exception:
                pass
            if apm is not None and not apm.initializedOK():
                #print "Setting ApMon to static configuration"
                try :
                    apm.setDestinations(apmonConf)
                except Exception:
                    apm = None
            apmonInstance = apm
        if apmonInstance is None :
            #print "Creating ApMon with static configuration"
            try :
                apmonInstance = apmon.ApMon(apmonConf, apmonLoggingLevel)
            except Exception:
                pass
    return apmonInstance 

#
# Method to free the apmon instance
#
def apmonFree() :
    global apmonInstance
    global apmonInit
    if apmonInstance is not None :
        time.sleep(1)
        try :
            apmonInstance.free()
        except Exception:
            pass
        apmonInstance = None
    apmonInit = False

#
# Method to send params to Monalisa service
#
def apmonSend(taskid, jobid, params) :
    apm = getApmonInstance()
    if apm is not None :
        try :
            apm.sendParameters(taskid, jobid, params)
        except Exception:
            pass

#
# Common method for writing debug information in a file
#
def logger(msg) :
    msg = str(msg)
    if not msg.endswith('\n') :
        msg += '\n'
    try :
        fh = open('report.log','a')
        fh.write(msg)
        fh.close()
    except Exception:
        pass

#
# Context handling for CLI
#

# Format envvar, context var name, context var default value
contextConf = {'MonitorID'    : ('MonitorID', 'unknown'),
               'MonitorJobID' : ('MonitorJobID', 'unknown') }

#
# Method to return the context
#
def getContext(overload=None) :
    context = {}
    overload = overload or {}
    for paramName in contextConf.keys() :
        paramValue = None
        if paramName in overload:
            paramValue = overload[paramName]
        if paramValue is None :
            envVar = contextConf[paramName][0]
            paramValue = os.getenv(envVar)
        if paramValue is None :
            defaultValue = contextConf[paramName][1]
            paramValue = defaultValue
        context[paramName] = paramValue
    return context

#
# Methods to read in the CLI arguments
#
def readArgs(lines) :
    argValues = {}
    for line in lines :
        paramName = 'unknown'
        paramValue = 'unknown'
        line = line.strip()
        if line.find('=') != -1 :
            split = line.split('=')
            paramName = split[0]
            paramValue = '='.join(split[1:])
        else :
            paramName = line
        if paramName != '' :
            argValues[paramName] = paramValue
    return argValues    

def filterArgs(argValues) :

    contextValues = {}
    paramValues = {}

    for paramName in argValues.keys() :
        paramValue = argValues[paramName]
        if paramValue is not None :
            if paramName in contextConf.keys() :
                contextValues[paramName] = paramValue
            else :
                paramValues[paramName] = paramValue 
        else :
            logger('Bad value for parameter :' + paramName) 

    return contextValues, paramValues

#
# SHELL SCRIPT BASED JOB WRAPPER
# Main method for the usage of the report.py script
#
def report(args) :
    argValues = readArgs(args)
    contextArgs, paramArgs = filterArgs(argValues)
    context = getContext(contextArgs)
    taskId = context['MonitorID']
    jobId = context['MonitorJobID']
    logger('SENDING with Task:%s Job:%s' % (taskId, jobId))
    logger('params : ' + repr(paramArgs))
    apmonSend(taskId, jobId, paramArgs)
    apmonFree()
    print("Parameters sent to Dashboard.")

#
# PYTHON BASED JOB WRAPPER
# Main class for Dashboard reporting
#
class DashboardAPI :
    def __init__(self, monitorId = None, jobMonitorId = None, lookupUrl = None) :
        self.defaultContext = {}
        self.defaultContext['MonitorID']  = monitorId
        self.defaultContext['MonitorJobID']  = jobMonitorId
        # cannot be set from outside
        self.defaultContext['MonitorLookupURL']  = lookupUrl

    def publish(self,**message) :
        self.publishValues(None, None, message)

    def publishValues(self, taskId, jobId, message) :
        contextArgs, paramArgs = filterArgs(message)
        if taskId is not None :
            contextArgs['MonitorID'] = taskId
        if jobId is not None :
            contextArgs['MonitorJobID'] = jobId
        for key in contextConf.keys() :
            if (key not in contextArgs) and (self.defaultContext[key] is not None):
                contextArgs[key] = self.defaultContext[key]
        context = getContext(contextArgs)
        taskId = context['MonitorID']
        jobId = context['MonitorJobID']
        apmonSend(taskId, jobId, paramArgs)

    def sendValues(self, message, jobId=None, taskId=None) :
        self.publishValues(taskId, jobId, message)

    def free(self) :
        apmonFree()
