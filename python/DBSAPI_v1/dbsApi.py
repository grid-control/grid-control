#!/usr/bin/env python
#
# $Id: dbsApi.py,v 1.17 2006/08/09 16:24:14 sekhri Exp $
#
# Base DBS API class. All implementation should implement interfaces
# listed here. Logging configuration methods are provided here for convenience
# (it can be done directly using the DbsLogManager singleton).
#

import dbsException
#import dbsLogManager

# Log levels used as masks (defined in dbsLogManager). 
#DBS_LOG_LEVEL_QUIET_ = dbsLogManager.LOG_LEVEL_QUIET_       # no output
#DBS_LOG_LEVEL_INFO_ = dbsLogManager.LOG_LEVEL_INFO_         # info for users
#DBS_LOG_LEVEL_TRACE_ = dbsLogManager.LOG_LEVEL_TRACE_       # execution trace
#DBS_LOG_LEVEL_DEBUG_ = dbsLogManager.LOG_LEVEL_DEBUG_       # debugging
#DBS_LOG_LEVEL_WARNING_ = dbsLogManager.LOG_LEVEL_WARNING_   # warnings
#DBS_LOG_LEVEL_ERROR_ = dbsLogManager.LOG_LEVEL_ERROR_       # errors
#DBS_LOG_LEVEL_ALL_ = dbsLogManager.LOG_LEVEL_ALL_           # all messages

##############################################################################
# DBS API exceptions.

class DbsApiException(dbsException.DbsException):

  def __init__ (self, **kwargs):
    """ Initialization. """
    dbsException.DbsException.__init__(self, **kwargs)

class InvalidDatasetPathName(DbsApiException):

  def __init__ (self, **kwargs):
    """ Initialization. """
    DbsApiException.__init__(self, **kwargs)

class InvalidDataTier(DbsApiException):

  def __init__ (self, **kwargs):
    """ Initialization. """
    DbsApiException.__init__(self, **kwargs)

##############################################################################
# DBS API interface class.

class DbsApi:

  # No constructor.
  
  # Methods which should be implemented in the derived classes.

  def listPrimaryDatasets(self, pattern="*"):
    """ Retrieve list of primary datasets matching the pattern. """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def listProcessedDatasets(self, pattern="*"):
    """ Retrieve list of processed datasets matching the pattern. """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def listParameterSets(self, pattern="*"):
    """ Retrieve list of parameter sets(s) matching the pattern. """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def listApplications(self, pattern="*"):
    """ Retrieve list of application(s) matching the pattern. """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def listApplicationConfigs(self, pattern="*"):
    """ Retrieve list of application config(s) matching the pattern. """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def getDatasetContents(self, dataset):
    """ Retrieve event collections given the dataset path name string. """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def getDatasetProvenance(self, dataset, dataTierList):
    """
    Retrieve list of dataset parents for the given dataTiers.
    """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def createPrimaryDataset(self, dataset):
    """
    Create primary dataset.
    """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def createProcessedDataset(self, dataset):
    """
    Create processed dataset.
    """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def createProcessing(self, processing):
    """
    Create processing.
    """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")


  def insertEventCollections(self, dataset, eventCollectionList):
    """
    Insert event collections for a given processed dataset.
    """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def createFileBlock(self, dataset, fileBlock):
    """
    Insert event collections for a given processed dataset.
    """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")

  def getDatasetFileBlocks(self, dataset):
    """ Retrives list of FileBlocks associated with a Processed Dataset """
    raise dbsException.MethodNotImplemented(args="This method should be overridden in the derived DBS API class.")   

  # Methods common for all API implementations.
  def getLogManagerInstance(self):
    """ Return log manager instance. """
    return dbsLogManager.getInstance()
    
  def setLogLevel(self, logLevel):
    """
    Set logging level. Example of usage would be:

    Get info messages and warnings:
      dbsApi.setLogLevel(DBS_LOG_LEVEL_INFO_|DBS_LOG_LEVEL_WARNING_)

    Get all messages:
      dbsApi.setLogLevel(DBS_LOG_LEVEL_ALL_)

    """
    dbsLogManager.getInstance().setLogLevel(logLevel)

  def setWriteToStdOut(self, writeToStdOut):
    """ Set flag which determines logging into stdout. """
    dbsLogManager.getInstance().setWriteToStdOut(writeToStdOut)

  def setLogFileName(self, fileName):
    """ This method is used to enable logging into a file. """
    dbsLogManager.getInstance().setLogFileName(fileName)
    
##############################################################################
# Unit testing.

if __name__ == "__main__":
  try:
    api = DbsApi()
    #api.setLogLevel(DBS_LOG_LEVEL_INFO_|DBS_LOG_LEVEL_ERROR_)
    api.getDatasetContents("myowner/mydataset")
  except dbsException.DbsException, ex:
    print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
  print "Done"
