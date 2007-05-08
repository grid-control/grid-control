#!/usr/bin/env python
#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
# DBS API exceptions.
#
#
#
import logging
#
from dbsException import DbsException
 
class DbsApiException(DbsException):
    
  def __init__ (self, **kwargs):
    """ Initialization. """
    DbsException.__init__(self, **kwargs)
    msg = "\nCaught API Exception %s: %s "  % (self.getClassName(), self.getErrorMessage() )
    if self.getErrorCode() not in (None, ""):
       msg += "\nDBS Exception Error Code: %s " % str(self.getErrorCode())
    logging.error(msg)
  
class InvalidDatasetPathName(DbsApiException):
    
  def __init__ (self, **kwargs):
    """ Initialization. """
    DbsApiException.__init__(self, **kwargs)

class InvalidDataTier(DbsApiException):

  def __init__ (self, **kwargs):
    """ Initialization. """
    DbsApiException.__init__(self, **kwargs)

class DbsNoObject(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsObjectExists(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsToolError(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsBadRequest(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsBadXMLData(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsBadData(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsExecutionError(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsConnectionError(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsDatabaseError(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsBadResponse(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)

class DbsProxyNotFound(DbsApiException):
  def __init__ (self, **kwargs):
    DbsApiException.__init__(self, **kwargs)


