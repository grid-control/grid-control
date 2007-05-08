#
# $Id: dlsApi.py,v 1.18 2006/09/21 15:19:48 delgadop Exp $
#
# DLS Client. $Name: DLS_0_1_1 $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module defines the CMS Dataset Location Service (DLS) client interface.
 Python applications interacting with a given DLS catalog implementation will
 use methods defined in the DlsApi class, defined in this module.

 This class serves as an interface definition. It should not be instantiated
 directly, but all instantiable API implementations should provide the code
 for the methods listed here (they could be derived classes).

 This module also includes an API exception class, to propagate error
 conditions when interacting with the DLS catalog.
"""

#########################################
# Imports 
#########################################
from os import environ


#########################################
# Module globals
#########################################
DLS_VERB_NONE = 0    # print nothing
DLS_VERB_INFO = 5    # print info
DLS_VERB_WARN = 10   # print only warnings (to stdout)
DLS_VERB_HIGH = 20   # print warnings (stdout) and error messages (stderr)


#########################################
# DlsApiError class
#########################################

class DlsApiError(Exception):
  """
  Exception class for the interaction with the DLS catalog using the DlsApi class.
  It normally contains a string message (empty by default), and optionally an
  error code (e.g.: if such is returned from the DLS).

  The exception may be printed directly, or its data members accessed.

  Actual (instantiable) implementations of the DLS API may extend this class to 
  define their own exceptions.
  """

  def __init__(self, message="", error_code=0):
    self.msg = message
    self.rc = error_code

  def __str__(self):
    return str(self.msg)

class NotImplementedError(DlsApiError):
  """
  Exception class for methods of the DlsApi that are not implemented (and
  should be by a instantiable API class).
  """

class ValueError(DlsApiError):
  """
  Exception class for invocations of DlsApi methods with an incorrect
  value as argument.
  """



#########################################
# DlsApi class
#########################################

class DlsApi(object):
  """
  This class serves as a DLS interface definition. It should not be instantiated
  directly, but all instantiable API implementations should provide the code
  for the methods listed here.
  
  Some DLS implementations may support hierarchy in the FileBlock namespace.
  Some others may have a flat FileBlock namespace. This should not affect use of
  the methods. Every method accepting FileBlock names as argument should require
  them in their complete form (that is including any subdirectories). 

  Unless specified, in the instantiable implementations, all methods that can raise
  an exception will raise one derived from DlsApiError, but further information
  should be provided by those implementations documentation.
  """

  def __init__(self, dls_endpoint = None, verbosity = DLS_VERB_WARN):
    """
    This constructor is used as a general data members initialiser.
    But remember that this class should not be instantiated, since no method
    here is implemented!

    The variables set are the DLS server endpoint (optionally with a port
    number, otherwise a default is used) and the verbosity. For some
    implementations, also a path to the DLS root directory is required.
    Implementations not using this path should accept it and just ignore it.
 
    Notice that this method allows to have an empty DLS endpoint, but instantiable
    DLS API classes should not allow this. They should deny instantiation if no
    DLS endpoint can be retrieved from (in this order):
         - specified dls_endpoint
         - DLS_ENDPOINT environmental variable
         - DLS catalog advertised in the Information System (if implemented)
         - Possibly some default value (if defined in a given implementation)
  
    The DLS_ENDPOINT variable is checked in this constructor. Other environmental
    variables may be used in particular DLS API implementations.

    The verbosity level affects invocations of all methods in this object. See
    the setVerbosity method for information on accepted values.

    @param dls_endpoint: the DLS server, as a string "hostname[:port][/path/to/DLS]"
    @param verbosity: value for the verbosity level
    """

    self.setVerbosity(verbosity)

    self.server = dls_endpoint

    if(not self.server):
      self.server = environ.get("DLS_ENDPOINT")


  ############################################
  # Methods defining the main public interface
  ############################################

  def add(self, dlsEntryList, **kwd):
    """
    Adds the specified DlsEntry object (or list of objects) to the DLS.

    For each specified DlsEntry with a non-registered FileBlock, a new entry is
    created in the DLS, and all locations listed in the DlsLocation list of the object
    are also registered. For the DlsEntry objects with an already registered FileBlock,
    only the locations are added.

    For DLS implementations with hierarchical FileBlock namespace, the method may
    support a flag, createParent (**kwd), that, if true, will cause the check for
    existence of the specified FileBlock parent directory tree, and the creation of
    that tree if it does not exist. The default for this flag is True.

    The supported attributes of the FileBlocks and of the locations, which are not null
    are also set for all the new registrations. Unsupported attributes are just ignored.

    Check the documentation of the concrete DLS client API implementation for
    supported attributes.

    The method will not raise an exception in case there is an error adding a FileBlock
    or location, but will go on with the rest, unless errorTolerant (**kwd) is set 
    to False. This last may be useful when transactions are used. In that case an error
    may cause and automatic roll-back so it is better that the method exits after 
    the first failure.

    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.add(x, session = True)
    is equivalent to::
      api.startSession()
      api.add(x)
      api.endSession()

    If trans(**kwd) is set to True, the whole operation of the method is
    performed within a transaction (if the DLS implementation supports it),
    in the same way as if it was within a session. If trans is True, the
    errorTolerant and session flags are ignored and considered False.

    In some implementations it is not safe to nest sessions or transactions,
    so do not do it.

    @exception XXXX: On error with the DLS catalog

    @param dlsEntryList: the DlsEntry object (or list of objects) to be added to the DLS
    @param kwd: Flags:
      - createParent: boolean (default True) for parent directory creation
      - errorTolerant: boolean (default True) for raising an exception after failure
      - trans: boolean (default False) for using a transaction for the operations
      - session: boolean (default False) for using a session for the operations
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg) 
 
  def update(self, dlsEntryList, **kwd):
    """
    Updates the attributes of the specified DlsEntry object (or list of objects)
    in the DLS.

    For each specified DlsEntry, the supported not null attributes of the composing
    DlsFileBlock object and the supported not null attributes of the DlsLocation
    objects of the composing locations list are updated.

    Check the documentation of the concrete DLS client API implementation for
    supported attributes.

    The method will not raise an exception in case there is an error updating the
    attributes of a FileBlock or location, but will go on with the rest, unless
    errorTolerant (**kwd) is set to False. This last may be useful when transactions
    are used. In that case an error may cause and automatic roll-back so it is better
    that the method exits after the first failure.

    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.update(x, session = True)
    is equivalent to::
      api.startSession()
      api.update(x)
      api.endSession()

    If trans(**kwd) is set to True, the whole operation of the method is
    performed within a transaction (if the DLS implementation supports it),
    in the same way as if it was within a session. If trans is True, the
    errorTolerant and session flags are ignored and considered False.
    
    In some implementations it is not safe to nest sessions or transactions,
    so do not do it.

    @exception XXXX: On error with the DLS catalog

    @param dlsEntryList: the DlsEntry object (or list of objects) to be updated
    @param kwd: Flags:
     - errorTolerant: boolean (default True) for raising an exception after failure
     - trans: boolean (default False) for using a transaction for the operations
     - session: boolean (default False) for using a session for the operations
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg) 


  def delete(self, dlsEntryList, **kwd):
    """
    Deletes the locations of the list composing the specified DLSEntry object
    (or list of objects) from the DLS. If the last location associated with
    the FileBlock is deleted, the FileBlock is also removed.

    For each specified DlsEntry, the locations specified in the composing
    location list are removed (none if empty). However, if all (**kwd) is set
    to True, then all locations associated with the FileBlock are removed,
    regardless of the contents of the specified location list.
    
    In any case, if the last location associated in the catalog with the
    specified FileBlock is deleted, the FileBlock itself is also removed, 
    unless keepFileBlock (**kwd) is set to true.

    For DLS implementations with a hierarchical FileBlock namespace, in the
    all==True case, the method will also delete empty directories in the
    hierarchy. Non empty directories will refuse to be removed (raising 
    an exception or printing a warning).

    A location will not be removed if it is custodial (f_type  == "P"),
    unless force (*kwd) is set to True.

    The method will not raise an exception for every error, but will try to
    go on with all the asked deletions, unless errorTolerant (**kwd) is set 
    to False. 
    
    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.delete(x, session = True)
    is equivalent to::
      api.startSession()
      api.delete(x)
      api.endSession()

    In some implementations it is not safe to nest sessions, so do not do it.

    NOTE: In some implementations it is not safe to use this method within a
    transaction. 

    @exception XXXX: On error with the DLS catalog

    @param dlsEntryList: the DlsEntry object (or list of objects) to be deleted 
    @param kwd: Flags:
      - all: boolean (default False) for removing all locations 
      - keepFileBlock: boolean (default False) for not deleting empty Fileblocks
      - force: boolean (default False) for removing custodial locations 
      - errorTolerant: boolean (default True) for raising an exception after failure
      - session: boolean (default False) for using a session for the operations
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)

    
  def getLocations(self, fileBlockList, **kwd):
    """
    Returns a list of DlsEntry objects holding the locations in which the specified
    FileBlocks are stored.
    
    A single FileBlock or a list of those may be used as argument. Each FileBlock may
    be specified as simple strings (hostnames) or as DlsFileBlock objects. The
    returned list contains a DlsEntry object per specified FileBlock, in the same
    order as in the argument.

    The returned objects will have a composing DlsFileBlock object containing
    the specified FileBlock name, and a composing DlsLocation object list holding
    the corresponding retrieved locations.

    If longList (**kwd) is set to true, some location attributes are also included
    in the returned DlsLocation objects. Check the documentation of the concrete
    DLS client API implementation for the list of attributes.

    NOTE: In some implementations, the long listing may be quite more expensive
    (slow) than the normal invocation.

    The method may raise an exception if an error in the DLS operation occurs.

    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.getLocations(x, session = True)
    is equivalent to::
      api.startSession()
      api.getLocations(x)
      api.endSession()

    In some implementations it is not safe to nest sessions, so do not do it.

    NOTE: Normally, it makes no sense to use this method within a transaction.

    @exception XXXX: On error with the DLS catalog

    @param fileBlockList: the FileBlock as string/DlsFileBlock (or list of those)
    @param kwd: Flags:
     - longList: boolean (default false) for the listing of location attributes
     - session: boolean (default False) for using a session for the operations

    @return: a list of DlsEntry objects containing the locations
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)
    

  def getFileBlocks(self, locationList, **kwd):
    """
    Returns a list of DlsEntry objects holding the FileBlocks stored in the
    specified locations.
    
    NOTE: Depending on the implementation, this method may be a very expensive
    operation and affect DLS response, so use it only with care!!

    A single location or a list of those may be used as argument. The locations
    may  be specified as simple strings (hostnames) or as DlsLocation objects.
    The returned list contains a DlsEntry object per FileBlock-location pair; 
    i.e.: for each specified location, one DlsEntry object per FileBlock
    stored there.

    The returned objects will have a composing DlsFileBlock object containing the
    interesting FileBlock name, and a composing DlsLocation object holding the 
    corresponding specified location.

    The method may raise an exception if an error in the DLS operation occurs.

    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.getFileBlocks(x, session = True)
    is equivalent to::
      api.startSession()
      api.getFileBlocks(x)
      api.endSession()

    In some implementations it is not safe to nest sessions, so do not do it.

    NOTE: Normally, it makes no sense to use this method within a transaction, so
    please avoid it. 

    @exception XXXX: On error with the DLS catalog

    @param locationList: the location as string/DlsLocation (or list of those)
    @param kwd: Flags:
     - session: boolean (default False) for using a session for the operations

    @return: a list of DlsEntry objects containing the FileBlocks
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)


  def listFileBlocks(self, fileBlockList, **kwd):
    """
    Returns a list of DlsFileBlock objects holding the information of the
    specified FileBlocks, or, for implementations with hierarchical FileBlock
    namespace, of the FileBlocks under the specified FileBlock directory.
    
    A single FileBlock, or a list of those, or a single Fileblock directory (not 
    a list) may be used as argument. In the case of FileBlocks, they may be
    specified as simple strings (FileBlock names) or as DlsFileBlock objects,
    and the returned list will contain a DlsFileBlock object per specified
    FileBlock, in the same order. In the case of directories, the argument
    should be a string holding the directory name, and the returned list will
    hold a DlsFileBlock object per FileBlock under that directory.

    The returned DlsFileBlock objects will contain both the FileBlock name
    and, if longList (**kwd) is set to true, some FileBlock attributes.
    Check the documentation of the concrete DLS client API implementation
    for the list of attributes

    For the case of directory listing, if recursive (**kwd) is set to True,
    the returned list will contain also the FileBlocks of the subdirectories
    under the specified one in a recursive way. DLS implementations with 
    flat FileBlock namespace will just ignore this flag.

    NOTE: Be aware that depending on the catalog a recursive listing may be
    a very costly operation and affect DLS response, so please use this
    flag only with care!!
    
    The method may raise an exception if an error in the DLS operation occurs.

    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.listFileBlocks(x, session = True)
    is equivalent to::
      api.startSession()
      api.listFileBlocks(x)
      api.endSession()

    In some implementations it is not safe to nest sessions, so do not do it.

    NOTE: Normally, it makes no sense to use this method within a transaction, so
    please avoid it. 

    @exception XXXX: On error with the DLS catalog

    @param fileBlockList: the FileBlock, as string or DlsFileBlock object (or
    list of those), or a FileBlock namespace directory, as a string
    @param kwd: Flags:
     - longList: boolean (default True) for the listing of location attributes
     - session: boolean (default False) for using a session for the operations
     - recursive: boolean (default False) for recursive listing of a directory 

    @return: a list of DlsFileBlock objects containing the FileBlock information
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)


  def renameFileBlock(self, oldFileBlock, newFileBlock, **kwd):
    """
    Renames the specified oldFileBlock to the new name specified as
    newFileBlock. Both arguments can be specified either as DlsFileBlock objects
    or simple strings (holding the name of the FileBlocks).

    If newFileBlock exists alread, it will be removed before the rename
    takes place.

    For DLS implementations with hierarchical FileBlock namespace, directory 
    FileBlocks may also be renamed. Check the corresponding implementation 
    documentation. Also, the method may support a flag, createParent (**kwd),
    that, if true, will cause the check for existence of the specified
    newFileBlock parent directory tree, and the creation of that tree if it
    does not exist. The default for this flag is False.

    The method will raise an exception in case there is an error renaming the
    FileBlock or creating the parent directories of the new FileBlock.

    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.add(x, session = True)
    is equivalent to::
      api.startSession()
      api.add(x)
      api.endSession()

    If trans(**kwd) is set to True, the whole operation of the method is
    performed within a transaction (if the DLS implementation supports it),
    in the same way as if it was within a session. If trans is True, the
    session flag is ignored and considered False.

    In some implementations it is not safe to nest sessions or transactions,
    so do not do it.

    @exception XXXX: On error with the DLS catalog

    @param oldFileBlock: the FileBlock to rename, as DlsFileBlock object or string
    @param newFileBlock: the new name for the FileBlock, as DlsFileBlock object or string
    @param kwd: Flags:
      - createParent: boolean (default True) for parent directory creation
      - trans: boolean (default False) for using a transaction for the operations
      - session: boolean (default False) for using a session for the operations
    """

    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg) 
 

  def getAllLocations(self, **kwd):
    """
    Returns all the locations in the DLS that are associated with any
    FileBlock in the catalog. The locations are returned as a list
    of DlsLocation objects.

    This methods accepts no arguments (other than possibly some kwd flags).

    NOTE: Depending on the implementation, this method may be a very expensive
    operation and affect DLS response, so use it only with care!!
    
    The method may raise an exception if an error in the DLS operation occurs.

    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.getAllLocations(session = True)
    is equivalent to::
      api.startSession()
      api.getAllLocations(x)
      api.endSession()

    In some implementations it is not safe to nest sessions, so do not do it.

    NOTE: Normally, it makes no sense to use this method within a transaction, so
    please avoid it. 

    @exception XXXX: On error with the DLS catalog

    @param kwd: Flags:
     - session: boolean (default False) for using a session for the operations

    @return: a list of DlsLocation objects containing the locations information
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)


  def dumpEntries(self, dir = "/", **kwd):
    """
    Returns the DLS entries under the specified directory in the FileBlock
    namespace, as a list of DlsEntry objects. These objects will include both
    FileBlock names and associated locations. In the case of DLS
    implementations with a flat FileBlock namespace, the directory argument
    will be ignored (i.e.: it will behave as if "/" is specified).
    
    A FileBlock directory (not a list) must be specified as argument
    of the method, in the form of a string or a DlsFileBlock object.
    
    If recursive (**kwd) is set to True, the returned list will contain also
    the FileBlocks of the subdirectories under the specified one in a
    recursive way. DLS implementations with flat FileBlock namespace will
    just ignore this flag.

    NOTE: Be aware that depending on the catalog a recursive listing may be
    a very costly operation and affect DLS response, so please use this flag
    only with care!!
    
    The method may raise an exception if an error in the DLS operation occurs.

    If session(**kwd) is set to True, the whole operation of the method is
    performed within a session (if the DLS implementation supports it).
    That is, the following call::
      api.dumpEntries(x, session = True)
    is equivalent to::
      api.startSession()
      api.dumpEntries(x)
      api.endSession()

    In some implementations it is not safe to nest sessions, so do not do it.

    NOTE: Normally, it makes no sense to use this method within a transaction, so
    please avoid it. 

    @exception XXXX: On error with the DLS catalog

    @param dir: the FileBlock dir, as string or DlsFileBlock object
    @param kwd: Flags:
     - session: boolean (default False) for using a session for the operations
     - recursive: boolean (default False) for recursive listing of a directory 

    @return: a list of DlsEntry objects representing the DLS data
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)


  def startSession(self):
    """
    For DLS implementations supporting sessions (for performance improvements
    in the DLS access), starts a session. If the DLS implementation does
    not support sessions, the method does nothing.

    The session is opened with the server set at object creation time and 
    should be ended with the endSession method afterwards.    

    A transaction with the DLS implies a session with it, so do not include
    one within another. Do not nest transactions or sessions either.

    @exception XXXX: On error with the DLS catalog
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)
  
 
  def endSession(self):
    """
    For DLS implementations supporting sessions (for performance improvements
    in the DLS access), ends a previoulsy opened session. If the DLS
    implementation does not support sessions, the method does nothing.

    The session to end is the one opened previously with the startSession
    method.

    @exception XXXX: On error with the DLS catalog
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)
  
 
  def startTrans(self):
    """
    For DLS implementations supporting transactions (for performance
    improvements and atomic operations in the DLS access), starts a transaction.
    If the DLS implementation does not support transactions, the method does
    nothing.

    The transaction is opened with the server set at object creation time. 
    From that moment on, every operation is performed within a transaction, so 
    any failure or a call to the abortTrans method will cause a roll-back 
    of the operations. If no error is produced and the transaction is ended
    with the endTrans method, the operations are then executed at once.

    A transaction with the DLS implies a session with it, so do not include
    one within another. Do not nest transactions or sessions either.

    Please check the DLS implementation notes on handling transactions in DLS
    interaction, since this is not always trivial.

    @exception XXXX: On error with the DLS catalog
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)
  
 
  def endTrans(self):
    """
    For DLS implementations supporting transactions (for performance
    improvements and atomic operations in the DLS access), ends a transaction,
    causing the execution of all the operations performed during the 
    transaction (since startTrans was used to start it). If the DLS
    implementation does not support transactions, the method does nothing.

    If a failure on the interaction with DLS occurs within the transaction,
    the operations are roll-backed.

    Please check the DLS implementation notes on handling transactions in DLS
    interaction.

    @exception XXXX: On error with the DLS catalog
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)
  
  
  def abortTrans(self):
    """
    For DLS implementations supporting transactions (for performance
    improvements and atomic operations in the DLS access), aborts a transaction,
    causing the roll-back of all operations performed within the
    transaction (since startTrans was used to start it). If the DLS
    implementation does not support transactions, the method does nothing.

    Please check the DLS implementation notes on handling transactions in DLS
    interaction.

    @exception XXXX: On error with the DLS catalog
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)
  
 

  ##################################
  # Other public methods (utilities)
  ##################################

  def changeFileBlocksLocation(self, org_location, dest_location):
    """
    For all the FileBlocks registered in the DLS server in the location
    "org_location", changes them so that they no longer exist in "org_location",
    but they are now in "dest_location".

    The method may raise an exception if there is an error in the operation.

    @param org_location: original location to be changed (hostname), as a string
    @param dest_location: new location for FileBlocks (hostname), as a string
    """
    msg = "This is just a base class!"
    msg += " This method should be implemented in an instantiable DLS API class"
    raise NotImplementedError(msg)

        
  def setVerbosity(self, value = DLS_VERB_WARN):
    """
    Sets the verbosity level for all subsequent DlsApi methods.
    
    Currently admitted values are:    
     - DLS_VERB_NONE => print nothing
     - DLS_VERB_WARN => print only warnings (to stdout)
     - DLS_VERB_HIGH => print warnings (stdout) and debug messages (stdout)

    @exception ValueError: if the specified value is not one of the admitted ones

    @param value: the new value for the verbosity level 
    """
    admitted_vals = [DLS_VERB_NONE, DLS_VERB_INFO, DLS_VERB_WARN, DLS_VERB_HIGH]
    if(value not in admitted_vals):
      msg = "The specified value is not one of the admitted ones"
      raise ValueError(msg)

    self.verb = value

