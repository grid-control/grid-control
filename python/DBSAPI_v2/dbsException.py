#!/usr/bin/env python
#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
#
# Contains base DBS exception class, as well as other common exceptions.

from exceptions import Exception

##############################################################################
# Base exception class.

class DbsException(Exception):

  def __init__(self, **kwargs):
    """
    DBS exception can be initialized in following ways:
      DBSException(args=exceptionString)
      DBSException(exception=exceptionObject)      
    """ 

    args = kwargs.get("args", "")
    ex = kwargs.get("exception", None)
    self.code = kwargs.get("code", "")
  
    if ex != None:
      if isinstance(ex, Exception):
	 exArgs = "%s" % (ex)
	 if args == "":
	   args = exArgs
	 else:
	   args = "%s (%s)" % (args, exArgs)
    Exception.__init__(self, args)

  def getArgs(self):
    """ Return exception arguments. """
    return self.args

  def getErrorMessage(self):
    """ Return exception error. """
    return "%s" % (self.args)

  def getClassName(self):
    """ Return class name. """
    return "%s" % (self.__class__.__name__)

  def getErrorCode(self):
    """ Return class name. """
    return "%s" % (self.code)

    
##############################################################################
# Other exception classes.

class MethodNotImplemented(DbsException):

  def __init__ (self, **kwargs):
    """ Initialization. """
    DbsException.__init__(self, **kwargs)

class DataNotInitialized(DbsException):

  def __init__ (self, **kwargs):
    """ Initialization. """
    DbsException.__init__(self, **kwargs)

class InvalidArgument(DbsException):

  def __init__ (self, **kwargs):
    """ Initialization. """
    DbsException.__init__(self, **kwargs)



##############################################################################
# Unit testing.

if __name__ == "__main__":
  try:
    raise MethodNotImplemented(args="This is my test exception.")
  except DbsException, ex:
    print "Caught exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
    try:
      raise DbsException(exception=ex)
    except DbsException, ex:
      print "Caught yet another exception %s: %s" % (ex.getClassName(), ex.getErrorMessage())
  print "Done"

