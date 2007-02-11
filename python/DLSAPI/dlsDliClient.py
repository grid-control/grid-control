#
# $Id: dlsDliClient.py,v 1.4 2006/05/03 14:59:18 delgadop Exp $
#
# DLS Client. $Name: DLS_0_1_1 $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module implements part of the CMS Dataset Location Service (DLS)
 client interface as defined by the dlsApi module. This implementation
 relies on a DLS server that supports a Data Location Interface (DLI)
 as a web service.

 The module contains the DlsDliClient class that extends the DlsApi
 class and implements the getLocations method. This reduced 
 functionality is enough for some clients that just query the DLS.
 This reduced implementation can be installed with less requirements
 than a complete DLS client API.

 To perform its function, it uses the dliClient.DliClient class.
 That class uses the Zolera SOAP Infrastructure (ZSI)
 (http://pywebsvcs.sourceforge.net/). ZSI requires PyXML.
 
 It also contains some exception classes to propagate error conditions
 when interacting with the DLS catalog.

 NOTE: This implementation does not support transactions (which is not
 a big problem, since it is only used for querys).
"""

#########################################
# Imports 
#########################################
import dlsApi
DLS_VERB_HIGH = dlsApi.DLS_VERB_HIGH
DLS_VERB_WARN = dlsApi.DLS_VERB_WARN
from dlsDataObjects import *
import dliClient
from os import environ

#########################################
# Module globals
#########################################


#########################################
# DlsDliClientError class
#########################################

class DlsDliClientError(dlsApi.DlsApiError):
  """
  Exception class for the interaction with the DLS catalog using the
  DlsDliClient class. It normally contains a string message (empty by default),
  and optionally an error code (e.g.: if such is returned from the DLS).

  The exception may be printed directly, or its data members accessed.
  """

class SetupError(DlsDliClientError):
  """
  Exception class for errors when setting up the system (configuration,
  communication errors...)
  """



#########################################
# DlsDliClient class
#########################################

class DlsDliClient(dlsApi.DlsApi):
  """
  This class is an implementation of the a subset of the DLS client interface,
  defined by the dlsApi.DlsApi class. This implementation relies on a DLI being
  supported by the DLS back-end.
  """

  def __init__(self, dli_endpoint = None, verbosity = dlsApi.DLS_VERB_WARN):
    """
    Constructor of the class. It sets the DLI endpoint to communicate with
    and the verbosity level, and creates the binding with the DLI interface.
    The DLI endpoint may include a path to the DLS root, since this is 
    necessary for some back-ends. Notice that if this is not correctly
    set the queries will probably fail.
    
    It tries to retrieve that value from several sources (in this order):
    
         - specified dli_endpoint
         - DLS_ENDPOINT environmental variable
         - DLI_ENDPOINT environmental variable
         - DLI endpoint advertised in the Information System (if implemented)

    If the DLI endpoint cannot be set in any of these ways, the instantiation is 
    denied and a SetupError is raised.
 
    The verbosity level affects invocations of all methods in this object. See
    the dlsApi.DlsApi.setVerbosity method for information on accepted values.
      
    @exception SetupError: if no DLI can be found.

    @param dli_endpoint: the DLI endpoint, as a string "hname[:port][/path/to/DLS]"
    @param verbosity: value for the verbosity level
    """

    # Let the parent set the server (if possible) and verbosity
    dlsApi.DlsApi.__init__(self, dli_endpoint, verbosity)
   
     # If the server is not there yet, try from DLI_ENDPOINT
    if(not self.server):
      self.server = environ.get("DLI_ENDPOINT")

    # If still not there, give up 
    if(not self.server):
       raise SetupError("Could not set the DLS server to use")
      
    # Extract the root directory
    dlsserver=self.server.split('/')[0]
    dlspath=self.server.replace(dlsserver,'')
    dlspath = dlspath.rstrip('/')

    # Set the server for LFC API use
    self.server=dlsserver

    if (not dlspath):
       raise SetupError("No LFC's root directory specified for DLS use")

    # Set the root directory (might be empty, since DLI back-end may be other than LFC)
    self.root = dlspath

    # Create the binding 
    try:    
       if(self.verb >= DLS_VERB_HIGH):
          print "--DliClient.init(%s)" % self.server
       self.iface = dliClient.DliClient(self.server)
    except dliClient.SetupError, inst:
       raise SetupError("Error creating the binding with the DLI interface: "+str(inst))


  ############################################
  # Methods defining the main public interface
  ############################################

  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The longlist (**kwd) flag is ignored. No attributes can be retrieved
    from the DLI.

    If an error occurs in the interaction with the DLI interface, an
    exception is raised. If the error is a SOAP fault, the code field 
    stores the SOAP "faultcode" element.

    @exception DlsDliClientError: On errors in the interaction with the DLI interface
    """

    result = []
    
    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList
    else:
       theList = [fileBlockList]

    # Query the DLI
    for fB in theList:
      # Check what was passed (DlsFileBlock or string)
      if(isinstance(fB, DlsFileBlock)):
        lfn = fB.name
      else:
        lfn = fB
      lfn = self._checkDlsHome(lfn)
      userlfn = self._removeRootPath(lfn)
      entry = DlsEntry(DlsFileBlock(userlfn))

      # Get the list of locations
      locList = []
      if(self.verb >= DLS_VERB_HIGH):
         print "--DliClient.listLocations(%s)" % lfn
      try:    
         for host in self.iface.listLocations(lfn, fileType = "lfn"):
            locList.append(DlsLocation(host))
      except dliClient.DliClientError, inst:
        msg = inst.msg
        msg = "Error querying for %s: %s" % (userlfn, inst.msg)
        e = DlsDliClientError(msg)
        if(isinstance(inst, dliClient.SoapError)):
           for i in [inst.actor, inst.detail]:
              if(i):  e.msg += ". " + str(i)
           if(inst.faultcode):  
              if(inst.faultstring):  e.code = inst.faultcode + ", " + inst.faultstring
              else:                  e.code = inst.faultcode    
           else:
              if(inst.faultstring):  e.code = inst.faultstring
        raise e

      entry.locations = locList
      result.append(entry)

    # Return
    return result





  #########################
  # Internal methods 
  #########################
  

  def _checkDlsHome(self, fileBlock, working_dir=None):
    """
    It completes the specified FileBlock name, by prepending the fixed 
    root path (starting point for the DLS server in the LFC's namespace).

    This method is required to hide the first LFC directories to the users.

    Further specification of working directory (on top of the root path)
    could be supported but it is not implemented at the moment (for the
    sake of simplicity).

    @param fileBlock: the FileBlock to be changed, as a string
    @param working_dir: Ignored at the moment 
      
    @return: the FileBlock name (with the root path prepended) as a string
    """
    
    # Current functionality just pre-prends the LFC's root path
    # Remove next block to allow for relative fileblocks support (LFC_HOME...)
    if(fileBlock.startswith('/')):
       return self.root + fileBlock
    else:
       return self.root + '/' + fileBlock

    # This makes the whole thing
#    Checks if the specified FileBlock is relative (not starting by '/') and if so,
#    it tries to complete it with the DLS client working directory.
#
#    This method is required to support the use of relative FileBlocks by client 
#    applications (like the CLI for example).
#
#    The DLS client working directory should be read from (in this order):
#       - specified working_dir
#       - DLS_HOME environmental variable
#       - LFC_HOME environmental variable
#
#    If this DLS client working directory cannot be read, or if the specified
#    FileBlock is an absolute path, the original FileBlock is returned.
#
#    @param fileBlock: the FileBlock to be changed, as a string
#    @param working_dir: the DLS client working directory (FileBlock), as a string
#      
#    @return: the FileBlock name (possibly with a path prepended) as a string
    if(fileBlock.startswith('/')):
       absFB = fileBlock
    else:
       lfc_home = working_dir
       if(not lfc_home):
          lfc_home = environ.get("DLS_HOME")
       if(not lfc_home):
          lfc_home = environ.get("LFC_HOME")          
       if(lfc_home):
          if(not lfc_home.startswith('/')):
            lfc_home = '/' + lfc_home
          absFB = lfc_home + '/' + fileBlock
       else:
          absFB = '/' + fileBlock

    return self.root + absFB


  def _removeRootPath(self, fileBlock):
    """
    Returns a FileBlock name with the leading root path (self.root) removed.
    If fileBlock does not start with the root path, the name is returned
    without modification, and a warning is printed.
 
    This method is used to hide the LFC's root path of DLS in the FileBlock
    names that are shown to users (since LFC's API gets and returns them in
    an absolute form).
    
    @param fileBlock: the FileBlock to be changed, as a string
      
    @return: the FileBlock name (with the root path removed) as a string
    """
    if(fileBlock.startswith(self.root+'/')):
       result = fileBlock.replace(self.root+'/', "", 1)
       result = result.strip('/')
       if(not result): result = '/'
       return result       
    else:
       if(self.verb >= DLS_VERB_WARN):
          msg = "Warning: Error when adapting name. FileBlock %s " % (fileBlock)
          msg += "does not start with root path (%s)." % (self.root+'/')
          print msg
       return fileBlock

#    if(fileBlock.startswith(self.root+'/')):
#       return fileBlock.replace(self.root+'/', "", 1)
#    else:
#       if(self.verb >= DLS_VERB_WARN):
#          msg = "Warning: Error when adapting name. FileBlock %s " % (fileBlock)
#          msg += "does not start with root path (%s)." % (self.root+'/')
#          print msg
#       return fileBlock

