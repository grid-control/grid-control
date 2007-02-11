#!/usr/bin/env python
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

