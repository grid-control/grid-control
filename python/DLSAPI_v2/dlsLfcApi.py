#
# $Id: dlsLfcApi.py,v 1.28 2006/09/24 15:17:42 afanfani Exp $
#
# DLS Client. $Name: DLS_0_1_1 $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module implements a CMS Dataset Location Service (DLS) client
 interface as defined by the dlsApi module. This implementation relies
 on a DLS server using a LCG File Catalog (LFC) as back-end.

 For the implementation of the getLocations method, the dlsDliClient
 module is used (for performance reasons). Check that module for
 dependencies.

 The module contains the DlsLfcApi class that implements all the methods
 defined in dlsApi.DlsApi class and a couple of extra convenient
 (implementation specific) methods. Python applications interacting with
 a LFC-based DLS will instantiate a DlsLFCApi object and use its methods.

 It also contains some exception classes to propagate error conditions
 when interacting with the DLS catalog.
"""

#########################################
# Imports 
#########################################
import dlsApi
DLS_VERB_HIGH = dlsApi.DLS_VERB_HIGH
DLS_VERB_WARN = dlsApi.DLS_VERB_WARN
#import dlsDliClient   # for a fast getLocations implementation
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
# TODO: From what comes next, should not import whole modules, but what is needed...
import lfc
import sys
import commands
import time
import getopt
from os import environ, putenv
from stat import S_IFDIR
#########################################
# Module globals
#########################################
#S_IFDIR = 0x4000

#########################################
# DlsLfcApiError class
#########################################

class DlsLfcApiError(dlsApi.DlsApiError):
  """
  Exception class for the interaction with the DLS catalog using the DlsLfcApi
  class. It normally contains a string message (empty by default), and optionally
  an  error code (e.g.: if such is returned from the DLS).

  The exception may be printed directly, or its data members accessed.
  """

class NotImplementedError(DlsLfcApiError):
  """
  Exception class for methods of the DlsApi that are not implemented (and
  should be by a instantiable API class).
  """

class ValueError(DlsLfcApiError):
  """
  Exception class for invocations of DlsApi methods with an incorrect value
  as argument.
  """
  
class SetupError(DlsLfcApiError):
  """
  Exception class for errors when setting up the system (configuration,
  communication errors...).
  """

class NotAccessibleError(DlsLfcApiError):
  """
  Exception class for errors when trying to access a FileBlock (or directory).
  """

#########################################
# DlsApì class
#########################################

class DlsLfcApi(dlsApi.DlsApi):
  """
  This class is an implementation of the DLS client interface, defined by
  the dlsApi.DlsApi class. This implementation relies on a Lcg File Catalog
  (LFC) as DLS back-end.

  Unless specified, all methods that can raise an exception will raise one
  derived from DlsLfcApiError.
  """

  def __init__(self, dls_endpoint= None, verbosity = DLS_VERB_WARN):
    """
    Constructor of the class. It sets the DLS (LFC) server to communicate
    with, the path to the root directory of the server, and the verbosity level.
    The server and root path are got from a string in the form
    "hname[:port]/path/to/DLS".
    
    It tries to retrieve that value value from several sources (in this order):
    
         - specified dls_endpoint 
         - DLS_ENDPOINT environmental variable
         - LFC_HOST environmental variable
         - DLS catalog advertised in the Information System (if implemented)

    If it cannot be obtained in any of these ways, the instantiation is denied
    and a SetupError is raised.
 
    The verbosity level affects invocations of all methods in this object. See
    the dlsApi.DlsApi.setVerbosity method for information on accepted values.
      
    @exception SetupError: if no DLS server can be found.

    @param dls_endpoint: the DLS server to be used, as a string "hname[:port]/path/to/DLS"
    @param verbosity: value for the verbosity level
    """

    # Let the parent set the server (if possible) and verbosity
    dlsApi.DlsApi.__init__(self, dls_endpoint, verbosity)

    # If the server is not there yet, try from LFC_HOST
    if(not self.server):
      self.server = environ.get("LFC_HOST")

    # If still not there, give up 
    if(not self.server):
       raise SetupError("Could not set the DLS server to use")

    # Extract the root directory
    dlsserver=self.server.split('/')[0]
    dlspath=self.server.replace(dlsserver,'')

    # Set the server for LFC API use
    self.server=dlsserver
    putenv("LFC_HOST", self.server)

    # Set the root directory (required!)
    if (not dlspath):
       raise SetupError("No LFC's root directory specified for DLS use")    
    else:
       fstat = lfc.lfc_filestatg()
       if(lfc.lfc_statg(dlspath, "", fstat)<0):
          code = lfc.cvar.serrno
          msg = "Specied LFC's root dir for DLS (%s) " % (dlspath)
          msg += "not accessible: %s" % (lfc.sstrerror(code))
          raise SetupError(msg, code)    
    dlspath = '/'+dlspath.strip('/')
    self.root = dlspath

    

  ############################################
  # Methods defining the main public interface
  ############################################

  def add(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.add method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Accepts the createParent flag as described in dlsApi.DlsApi.add.

    The list of supported attributes for the FileBlocks is:
     - guid
     - filemode
     - filesize
     - csumtype ("CS", "AD" or "MD": standard 32 bits, adler 32 b., MD5 128 b.)
     - csumvalue
    
    The list of supported attributes for the locations is:
     - sfn
     - f_type
     - ptime

    If the composing DlsFileBlock objects used as argument include the GUID of
    the FileBlock, then that value is used in the catalog. Likewise, if the
    composing DlsLocation objects include the SURL of the FileBlock copies,
    that value is also used. Notice that both uses are discouraged as they may
    lead to catalog corruption if used without care.
    """

    # Keywords
    createParent = True
    if(kwd.has_key("createParent")):
       createParent = kwd.get("createParent")
       
    errorTolerant = True 
    if(kwd.has_key("errorTolerant")):
       errorTolerant = kwd.get("errorTolerant")
    
    trans = False 
    if(kwd.has_key("trans")):
       trans = kwd.get("trans")

    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")

    if(trans):
      errorTolerant = False
      session = False
   
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList 
    else:
       theList = [dlsEntryList]

    # Start transaction/session
    if(trans): self.startTrans()
    else:
       if(session): self.startSession()

    # Loop on the entries
    for entry in theList:
      # FileBlock
      try:
         guid = self._addFileBlock(entry.fileBlock, createParent=createParent)
      except DlsLfcApiError, inst:
         if(not errorTolerant):
           if(session): self.endSession()
           if(trans):
                  self.abortTrans()
                  inst.msg += ". Transaction operations rolled back"
           raise inst
         else: # Can't add locations without guid, so go to next FileBlock
           continue
      # Locations
      for loc in entry.locations:
         try:
            self._addLocationToGuid(guid, loc, entry.fileBlock.name)
         except DlsLfcApiError, inst:
            if(not errorTolerant):
               if(session): self.endSession()
               if(trans):
                  self.abortTrans()
                  inst.msg += ". Transaction operations rolled back"
               raise inst

    # End transaction/session
    if(trans): self.endTrans()
    else:
       if(session): self.endSession()

 
  def update(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.update method.
    Refer to that method's documentation.

    Implementation specific remarks:

    For a given FileBlock, specified locations that are not registered in the
    catalog will be ignored.

    The list of supported attributes for the FileBlocks is:
     - filesize
     - csumtype ("CS", "AD" or "MD": standard 32 bits, adler 32 b., MD5 128 b.)
     - csumvalue
    
    The list of supported attributes for the locations is:
     - ptime
     - atime*

    (*) NOTE: For "atime", the value of the attribute is not considered, but the
    access time is set to current time.
    """

    # Keywords
    errorTolerant = True 
    if(kwd.has_key("errorTolerant")):
       errorTolerant = kwd.get("errorTolerant")
       
    trans = False
    if(kwd.has_key("trans")):
       trans = kwd.get("trans")

    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")

    if(trans):
      errorTolerant = False
      session = False   
 
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList 
    else:
       theList = [dlsEntryList]

    # Start transaction/session
    if(trans): self.startTrans()
    else:
       if(session): self.startSession()

    # Loop on the entries
    for entry in theList:
      # FileBlock
      try:
         self._updateFileBlock(entry.fileBlock)
      except DlsLfcApiError, inst:
         if(not errorTolerant):
           if(session): self.endSession()
           if(trans):
                  self.abortTrans()
                  inst.msg += ". Transaction operations rolled back"
           raise inst
         else:
           # For FileBlocks not accessible, go to next
           if(isinstance(inst, NotAccessibleError)):
              if(self.verb >= DLS_VERB_WARN):
                 print "Warning: Not updating unaccessible FileBlock: %s" % (inst.msg)
              continue
           # For error on attributes, just warn and go on
           else:
              if(self.verb >= DLS_VERB_WARN):
                 print "Warning: Error when updating FileBlock: %s" % (inst.msg)

      # Locations (must retrieve the SURLs from the catalog and compare)
      seList = []
      for loc in entry.locations:
         seList.append(loc.host)
      lfn = entry.fileBlock.name
      lfn = self._checkDlsHome(lfn)
      userlfn = self._removeRootPath(lfn)
      if(not userlfn): userlfn = lfn
      if(self.verb >= DLS_VERB_HIGH):
         print "--lfc.lfc_getreplica(\""+ lfn +"\", \"\",\"\")"
      err, repList = lfc.lfc_getreplica(lfn, "", "")
      if(err):
         if(not errorTolerant):
           if(session): self.endSession()
           code = lfc.cvar.serrno
           msg = "Error retrieving locations for(%s): %s" % (userlfn, lfc.sstrerror(code))
           if(trans):
                   self.abortTrans()
                   msg += ". Transaction operations rolled back"
           raise DlsLfcApiError(msg, code)
         else: continue
    
      for filerep in repList:
      
         if (filerep.host in seList):
            loc = entry.getLocation(filerep.host)
            loc.setSurl(filerep.sfn)
         
            # Don't look for this SE further
            seList.remove(filerep.host)            
            
            # Update location
            try:
              self._updateSurl(loc)
            except DlsLfcApiError, inst:
              if(not errorTolerant): 
                 if(session): self.endSession()
                 if(trans):
                   self.abortTrans()
                   inst.msg += ". Transaction operations rolled back"
                 raise inst

            # And if no more SEs, exit
            if(not seList):
               break
   
      # For the SEs specified, warn if they were not all updated 
      if(seList and (self.verb >= DLS_VERB_WARN)):
         print "Warning: FileBlock %s - Not all locations could be found and updated"%(userlfn)

    # End transaction/session
    if(trans): self.endTrans()
    else:
       if(session): self.endSession()


    
  def delete(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.delete method.
    Refer to that method's documentation.

    Implementation specific remarks:

    The LFC-based DLS supports a hierarchical FileBlock namespace. This 
    method, in the case that all (**kwd) is set to True, will delete empty
    directories in the hierarchy.

    The LFC-based DLS supports symlinks to a FileBlock. This method accepts both
    original FileBlocks or symlinks. Only the specified one will be deleted,
    unless removeLinks (**kwd) is set to True; in that case, all the symlinks and
    the original FileBlock will be removed from the DLS.
    NOTE: THIS FLAG IS NOT YET PROPERLY IMPLEMENTED!

    NOTE: It is not safe to use this method within a transaction.
    
    Additional parameters:
    @param kwd: Flags:
     - removeLinks: boolean (default False) for removing all the symlinks. THIS
       FLAG IS NOT YET PROPERLY IMPLEMENTED!
    """

    # Keywords
    force = False 
    if(kwd.has_key("force")):    force = kwd.get("force")
       
    all = False 
    if(kwd.has_key("all")):      all = kwd.get("all")

    removeLinks = False 
    if(kwd.has_key("removeLinks")):    removeLinks = kwd.get("removeLinks")

    keepFileBlock = False 
    if(kwd.has_key("keepFileBlock")):  keepFileBlock = kwd.get("keepFileBlock")

    session = False
    if(kwd.has_key("session")):  session = kwd.get("session")

    errorTolerant = True 
    if(kwd.has_key("errorTolerant")):  errorTolerant = kwd.get("errorTolerant")

       
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList 
    else:
       theList = [dlsEntryList]

    # Start session
    if(session): self.startSession()

    # Loop on the entries
    for entry in theList:
      
      # Get the FileBlock name
      lfn = entry.fileBlock.name
      lfn = self._checkDlsHome(lfn)
      try:
         userlfn = self._removeRootPath(lfn, strict = True)
      except ValueError, inst:
         if(not errorTolerant): 
            if(session): self.endSession()
            raise 
         else: continue

      # Get the specified locations
      seList = []
      if(not all):
         for loc in entry.locations:
            seList.append(loc.host)



      ###### Directory part #####

      # Check if the entry is a directory
      fstat = lfc.lfc_filestatg()
      if(lfc.lfc_statg(lfn, "", fstat)<0):
         code = lfc.cvar.serrno
         msg = "Error accessing FileBlock %s: %s" % (userlfn, lfc.sstrerror(code))
         if(not errorTolerant): 
            if(session): self.endSession()
            raise DlsLfcApiError(msg, code)
         else: 
            if(self.verb >= DLS_VERB_WARN):
               print "Warning: Skipping FileBlock. %s" % (msg)
            continue

      if(fstat.filemode & S_IFDIR):
         # If all was specified, remove it, otherwise go on     
         if(all):
            try:
               self._deleteDir(lfn)            
            except DlsLfcApiError, inst:
               if(not errorTolerant):
                  if(session): self.endSession()
                  raise inst
            continue
         else:
            if(self.verb >= DLS_VERB_WARN):
               print "Warning: Without \"all\" option, skipping directory %s" %(userlfn)
            continue



      ###### Locations part #####

      # Retrieve the existing associated locations (from the catalog)
      if(self.verb >= DLS_VERB_HIGH):
         print "--lfc.lfc_getreplica(\""+lfn+"\", \"\",\"\")"
      err, locList = lfc.lfc_getreplica(lfn, "", "")
      if(err):
            code = lfc.cvar.serrno
            msg = "Error retrieving locations for FileBlock (%s): %s"%(userlfn, lfc.sstrerror(code))
            if(not errorTolerant): 
               if(session): self.endSession()
               raise DlsLfcApiError(msg, code)
            else: 
               if(self.verb >= DLS_VERB_WARN):
                  print "Warning: Skipping FileBlock. %s" % (msg)
               continue

     
      # Make a copy of location list (to keep control of how many are left)
      remainingLocs = []
      for i in xrange(len(locList)):
         remainingLocs.append(locList[i].host)
         
      # Loop on associated locations
      for filerep in locList:
      
         # If this host (or all) was specified, remove it
         if(all or (filerep.host in seList)):
         
            # Don't look for this SE further
            if(not all): seList.remove(filerep.host)
            
            # But before removal, check if it is custodial
            if ((filerep.f_type == 'P') and (not force)):
               if(self.verb >= DLS_VERB_WARN):
                  print "Warning: Not deleting custodial replica in",filerep.host,"of",userlfn
               continue
               
            # Perform the deletion
            try:
               err = self._deleteSurl(filerep.sfn)
               remainingLocs.remove(filerep.host)
            except DlsLfcApiError, inst:
               if(not errorTolerant): 
                  if(session): self.endSession()
                  raise inst
        
            # And if no more SEs, exit
            if((not all) and (not seList)):
               break
   
      # For the SEs specified, warn if they were not all removed
      if(not all):
         if(seList and (self.verb >= DLS_VERB_WARN)):
            print "Warning: Not all specified locations could be found and removed"
   

      ###### FileBlock part #####
      
      # If all the replicas (even custodial) were deleted and no keepFileBlock specified
      if((not remainingLocs) and (not keepFileBlock)):
  
         # If it was specified, delete all links (even main FileBlock name)
         if(removeLinks):
  
            if(self.verb >= DLS_VERB_HIGH):
            # TODO: Have to check this call... never did the typemap
               print "--lfc.lfc_getlinks(\""+lfn+"\", \"\",\"\")"
            err, linkList = lfc.lfc_getlinks(lfn, "", "")
  
            if(err):
               if(session): self.endSession()
               code = lfc.cvar.serrno
               msg = "Error retrieving links for FileBlock (%s): %s"%(userlfn,lfc.sstrerror(code))
               raise DlsLfcApiError(msg, code)
  
            for link in linkList:
               try:
                  err = self._deleteFileBlock(link.path)
               except DlsLfcApiError, inst:
                  if(not errorTolerant):
                     if(session): self.endSession()
                     raise inst
            
         # Not links, delete only specified name
         else:
            try:
               err = self._deleteFileBlock(lfn)
            except DlsLfcApiError, inst:
               if(not errorTolerant):
                  if(session): self.endSession()
                  raise inst

    # End session
    if(session): self.endSession()

    
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    If longList (**kwd) is set to True, some location attributes are also
    included in the returned DlsLocation objects. Those attributes are:
     - atime
     - ptime
     - f_type.

    NOTE: The long listing may be quite more expensive (slow) than the normal
    invocation, since the first one has to query the LFC directly (secure),
    while the normal gets the information from the DLI (insecure), by using
    the dlsDliClient.DlsDliClient class.

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """
    # Keywords
    longList = False 
    if(kwd.has_key("longList")):   longList = kwd.get("longList")

    session = False
    if(kwd.has_key("session")):    session = kwd.get("session")

    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList 
    else:
       theList = [fileBlockList]

    entryList = []
    
    # For long listing (need to query the LFC directly)    
    if(longList):

      # Start session
      if(session): self.startSession()

      # Loop on the entries
      for fB in theList:
         # Check what was passed (DlsFileBlock or string)
         if(isinstance(fB, DlsFileBlock)):
           lfn = fB.name
         else:
           lfn = fB
         lfn = self._checkDlsHome(lfn)
         try:
            userlfn = self._removeRootPath(lfn, strict = True)
         except ValueError, inst:
            if(not errorTolerant): 
               if(session): self.endSession()
               raise 
            else: continue

         entry = DlsEntry(DlsFileBlock(userlfn))
 
         # Get the locations for the given FileBlock
         if(self.verb >= DLS_VERB_HIGH):
             print "--lfc.lfc_getreplica(\""+lfn+"\", \"\", \"\")"   
         err, filerepList = lfc.lfc_getreplica(lfn, "", "")
         if(err):
            if(session): self.endSession()
            code = lfc.cvar.serrno
            msg = "Error retrieving locations for %s: %s" % (userlfn, lfc.sstrerror(code))
            raise DlsLfcApiError(msg, code)
         
         # Build the result
         locList = []
         for filerep in filerepList:
            attrs = {"atime": filerep.atime, "ptime": filerep.ptime,
                     "f_type": filerep.f_type, "sfn": filerep.sfn}
            
            loc = DlsLocation(filerep.host, attrs)
            locList.append(loc)
         entry.locations = locList
         entryList.append(entry)

      # End session
      if(session): self.endSession()

    

    # For normal listing (go through the DLI)
    else:      

      import dlsDliClient

      # Build a FileBlock name list with absolute paths
      dliList = []
      for fB in theList:
         # Check what was passed (DlsFileBlock or string)
         if(isinstance(fB, DlsFileBlock)):
           lfn = fB.name
         else:
           lfn = fB
         dliList.append(lfn)
         
      # Create the binding 
      try:
        dliIface = dlsDliClient.DlsDliClient(self.server+self.root, verbosity = self.verb)
      except dlsDliClient.SetupError, inst:
        raise DlsLfcApiError("Error creating the binding with the DLI interface: "+str(inst))

      # Query the DLI 
      try:
          entryList = dliIface.getLocations(dliList)
      except dlsDliClient.DlsDliClientError, inst:
        raise DlsLfcApiError(inst.msg)


    # Return what we got
    return entryList

      
  
  def getFileBlocks(self, locationList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    NOTE: This method may be quite more expensive (slow) than the getLocations
    method.

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """

    # Keywords
    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")

    # Make sure the argument is a list
    if (isinstance(locationList, list)):
       theList = locationList 
    else:
       theList = [locationList]

    entryList = []

    # Start session
    if(session): self.startSession()

    try:
       # Loop on the entries
       for loc in theList:
          
          # Check what was passed (DlsLocation or string)
          if(isinstance(loc, DlsLocation)):
            host = loc.host
          else:
            host = loc

          # Retrieve list for each location and add it to the general list
          partialList = self._getEntriesFromDir("/", host, True)
          for i in partialList:
#AF-begin: add starting slash to fileblock name
            if(not i.fileBlock.name.startswith('/')): i.fileBlock.name = '/' + i.fileBlock.name
#AF-end
            entryList.append(i)
       

    except DlsLfcApiError, inst:
       if(session): self.endSession() 
       raise
       
    # End session
    if(session): self.endSession()
    
    # Return what we got
    return entryList


  def listFileBlocks(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.listFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:

    This implementation supports a FileBlock directory (not a list) 
    as argument of the method, as well as a FileBlock name/object, or
    list of those.
    
    For the returned entries, a slash is appended to the name of those
    that are directories in the FileBlocks namespace, to make it clear
    that they are directories and not normal FileBlocks.

    The implementation also supports recursive listing as described
    in dlsApi.DlsApi.listFileBlocks. In this case, the resulting list
    contains information on the FileBlocks under the specified directory
    and its subdirectories also. The directory FileBlocks themselves are
    not included in the list (for compatibility with other implementations).
    As an exception, empty directories do appear in the list (since they
    are not visible in the path of the files they contain).

    If longList (**kwd) is set to True, the attributes returned with
    the FileBlock are the following:
     - filemode
     - nlink (number of files in a directory, 1 for a FileBlock)
     - uid (owner)
     - gid (group owner)
     - filesize
     - mtime (last modification date)
     - csumtype
     - csumvalue
    Additionally, the GUID is set in the corresponding dlsFileBlock object.

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """
    # Keywords
    longList = True 
    if(kwd.has_key("longList")):   longList = kwd.get("longList")

    session = False
    if(kwd.has_key("session")):    session = kwd.get("session")

    recursive = False
    if(kwd.has_key("recursive")):    recursive = kwd.get("recursive")

    # Start session
    if(session): self.startSession()

    # Check if the argument is list / single fB / single dir
    if (not isinstance(fileBlockList, list)):
       lfn = fileBlockList
       try:
          result = self._listFileBlock(lfn, True) 
          if(result.attribs["filemode"] & S_IFDIR):
             if(self.verb >= DLS_VERB_HIGH):
                 print "--listDir(%s)" % (lfn)
             result = self._listDir(lfn, longList, recursive)
          else:
             if(not longList):
                result = DlsFileBlock(lfn)
             result = [result]
       except DlsLfcApiError, inst:
          if(session): self.endSession() 
          raise

    # It is a list (fileBlocks), loop on the entries
    else: 
      result = []
      for fB in fileBlockList:
         try:
            result.append(self._listFileBlock(fB, longList))
         except DlsLfcApiError, inst:
            if(session): self.endSession() 
            raise

    # End session
    if(session): self.endSession()

    # Return what we got
    return result



  def renameFileBlock(self, oldFileBlock, newFileBlock, **kwd):
    """
    Implementation of the dlsApi.DlsApi.renameFileBlock method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Both arguments of the method may be refer to the same type of Fileblock:
    both normal FileBlocks or both directories.
    
    If a directory is specified as newFileBlock, it must not be a descendant
    of oldFileBlock. Also, if newFileBlock existed already, it must be empty
    and it will be removed prior to renaming.
    
    Write permission is required on both parents. If oldFileBlock is a directory,
    write permission is required on it and if newFileBlock is an existing
    directory, write permission is also required on it.
    """
    
    # Keywords
    createParent = False 
    if(kwd.has_key("createParent")):
       createParent = kwd.get("createParent")
       
    trans = False 
    if(kwd.has_key("trans")):
       trans = kwd.get("trans")

    session = False
    if(kwd.has_key("session")):
       session = kwd.get("session")

    if(trans):
      session = False
   
    # Check what was passed and extract interesting values 
    if(isinstance(oldFileBlock, DlsFileBlock)):
       oldLfn = oldFileBlock.name
    else:
       oldLfn = oldFileBlock
    if(isinstance(newFileBlock, DlsFileBlock)):
       newLfn = newFileBlock.name
    else:
       newLfn = newFileBlock


    # Start transaction/session
    if(trans): self.startTrans()
    else:
       if(session): self.startSession()

    try:
       self._renameFileBlock(oldLfn, newLfn, createParent=createParent)
    except DlsLfcApiError, inst:
       if(session): self.endSession()
       if(trans):
              self.abortTrans()
              inst.msg += ". Transaction operations rolled back"
       raise inst

    # End transaction/session
    if(trans): self.endTrans()
    else:
       if(session): self.endSession()




  def getAllLocations(self, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getAllLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """
    locList = []
 
    # Keywords
    session = False
    if(kwd.has_key("session")):    session = kwd.get("session")

    # Start session
    if(session): self.startSession()

    # Get all the locations from the root dir
    lfn = "/"
    try:
       if(self.verb >= DLS_VERB_HIGH):
          print "--self._getLocsFromDir(%s)" % (lfn)
       hostList = self._getLocsFromDir(lfn, recursive=True)
    except DlsLfcApiError, inst:
          if(session): self.endSession() 
          raise

    # End session
    if(session): self.endSession()

    # Build the list of DlsLocation objects
    for i in hostList:
       locList.append(DlsLocation(i))

    # Return what we got
    return locList


  def dumpEntries(self, dir = "/", **kwd):
    """
    Implementation of the dlsApi.DlsApi.dumpEntries method.
    Refer to that method's documentation.

    Implementation specific remarks:

    A FileBlock directory (not a list) must be specified as argument
    of the method, in the form of a string or a DlsFileBlock object.
    
    For the returned entries, a slash is appended to the name of those
    that are directories in the FileBlocks namespace, to make it clear
    that they are directories and not normal FileBlocks.

    The implementation also supports recursive listing as described
    in dlsApi.DlsApi.dumpEntries. In this case, the resulting list
    contains information on the FileBlocks under the specified directory
    and its subdirectories also. The directory FileBlocks themselves are
    not included in the list (for compatibility with other implementations).
    As an exception, empty directories do appear in the list (since they
    are not visible in the path of the files they contain).

    NOTE: Normally, it makes no sense to use this method within a transaction,
    so please avoid it. 
    """
    # Keywords
    session = False
    if(kwd.has_key("session")):    session = kwd.get("session")

    recursive = False
    if(kwd.has_key("recursive")):    recursive = kwd.get("recursive")

    # Start session
    if(session): self.startSession()

    # Call the internal method that does the work
    try:
       result = self._getEntriesFromDir(dir, "", recursive) 
    except DlsLfcApiError, inst:
       if(session): self.endSession() 
       raise

    # End session
    if(session): self.endSession()

    # Return what we got
    return result



  def startSession(self):
    """
    Implementation of the dlsApi.DlsApi.startSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Starting session with "+self.server
    if(lfc.lfc_startsess("", "")):
      code = lfc.cvar.serrno
      msg = "Error starting session with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)

 
  def endSession(self):
    """
    Implementation of the dlsApi.DlsApi.endSession method.
    Refer to that method's documentation.
    
    Implementation specific remarks:

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Ending session with "+self.server
    if(lfc.lfc_endsess()):
      code = lfc.cvar.serrno
      msg = "Error ending session with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)
  
 
  def startTrans(self):
    """
    Implementation of the dlsApi.DlsApi.startTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    Dealing with transactions in LFC is not trivial. Within a transaction, some of
    the methods in the LFC API see the normal view of the catalog (as it was before
    the transaction was initiated) and some see the updated view (after changes
    produced by the not yet comitted operations). For that reason, modifying the 
    catalog and then checking it can lead to unpredictable results in some
    situations. As a general rule, please use transactions only for addition or 
    update operations and do not enclose too many operations within a
    transaction.
    
    Please check LFC documentation for details on transactions.

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Starting transaction with "+self.server
    if(lfc.lfc_starttrans("", "")):
      code = lfc.cvar.serrno
      msg = "Error starting transaction with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)


  def endTrans(self):
    """
    Implementation of the dlsApi.DlsApi.endTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    See the startTrans method.

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Ending transaction with "+self.server
    if(lfc.lfc_endtrans()):
      code = lfc.cvar.serrno
      msg = "Error ending transaction with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)
  
  
  def abortTrans(self):
    """
    Implementation of the dlsApi.DlsApi.abortTrans method.
    Refer to that method's documentation.

    Implementation specific remarks:

    See the startTrans method.

    @exception DlsLfcApiError: On error in the interaction with DLS
    """
    if(self.verb >= DLS_VERB_HIGH):
      print "--Aborting transaction with "+self.server
    if(lfc.lfc_aborttrans()):
      code = lfc.cvar.serrno
      msg = "Error aborting transaction with LFC-based DLS: %s" % (lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)
  
 

  
  ##################################
  # Other public methods (utilities)
  ##################################

  def changeFileBlocksLocation(self, org_location, dest_location):
    """
    NOT YET IMPLEMENTED (IF EVER).

    Implementation of the dlsApi.DlsApi.changeFileBlocksLocation method.
    Refer to that method's documentation.
    """

    # Implement here...
    msg += "Not yet implemented"
    raise NotImplementedError(msg)

        
  ################################################
  # Other public methods (implementation specific)
  ################################################

  def getGUID(self, fileBlock):
    """
    Returns the GUID used in the DLS for the specified FileBlock, by querying
    the DLS.

    The FileBlock may be specified as a DlsFileBlock object or as a simple string
    (holding the FileBlock name). In the first case, the method returns a
    reference to the object, after having set its internal GUID variable. In the
    second case, the method returns the GUID as a string (without any prefix,
    such as "guid://").

    Notice that this method will only work if the FileBlock has already been
    registered with the DLS catalog.

    @exception XXXX: On error with the DLS catalog (non-existing FileBlock, etc.)
    
    @param fileBlock: the FileBlock, as a string/DlsFileBlock object

    @return: the GUID, as a string or the DlsFileBlock object with the GUID set
    """
    # Check what was passed and extract the FileBlock name
    if(isinstance(fileBlock, DlsFileBlock)):
       objectPassed = True
       lfn = fileBlock.name
    else:
       objectPassed = False
       lfn = fileBlock

    # Adapt name... 
    lfn = self._checkDlsHome(lfn)
    userlfn = self._removeRootPath(lfn)
    if(not userlfn): userlfn = lfn
   
    # Query the DLS
    fstat=lfc.lfc_filestatg()
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_statg(%s)" % (lfn)
    if(lfc.lfc_statg(lfn, "", fstat) <0):
       code = lfc.cvar.serrno
       msg = "Error accessing FileBlock %s: %s" % (userlfn, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
         print "Warning: " + msg
       raise DlsLfcApiError(msg, code)
    else:
       guid=fstat.guid

    # Return the result
    if(objectPassed):
       fileBlock.setGuid(guid)
       return fileBlock
    else:
       return guid
 

    
  def getSURL(self, dlsEntry):
    """
    Gets the SURLs associated with the specified DlsEntry object. It querys the
    DLS and sets the SURLs (one per each location in the composing location list)
    in the specified DlsEntry object. It returns a reference to this completed
    object. 

    @exception XXXX: On error with the DLS catalog (non-existing FileBlock, etc.)
    
    @param dlsEntry: the DlsEntry object for which the SURLs must be retrieved

    @return: a reference to the same DlsEntry object with the SURLs added for
    each location
    """
    # Extract and adapt FileBlock name
    lfn = self._checkDlsHome(dlsEntry.fileBlock.name)
    userlfn = self._removeRootPath(lfn)
    if(not userlfn): userlfn = lfn

    # Now the locations (must compare with those of the catalog)
    repList = []
    seList = []    
    for loc in dlsEntry.locations:
       seList.append(loc.host)
   
    # Query the DLS
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_getreplica(\""+ lfn +"\", \"\",\"\")"
    err, repList = lfc.lfc_getreplica(lfn, "", "")
    if(err):
      code = lfc.cvar.serrno
      msg = "Error retrieving locations for(%s): %s" % (userlfn, lfc.sstrerror(code))
      raise DlsLfcApiError(msg, code)
    
    for filerep in repList:

       # Set the SURL in the passed object
       if (filerep.host in seList):
          loc = dlsEntry.getLocation(filerep.host)
          loc.setSurl(filerep.sfn)
       
          # Don't look for this SE further
          seList.remove(filerep.host)            
          
          # And if no more SEs, exit
          if(not seList):
             break
   
    # For the SEs specified, warn if they were not all found
    if(seList and (self.verb >= DLS_VERB_WARN)):
       print "Warning: FileBlock %s - not all locations could be found and their SURL set"%(userlfn)

    # Return the result
    return dlsEntry 






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
    
    # Just pre-prend the LFC's root path
    if(fileBlock.startswith('/')):
       return self.root + fileBlock
    else:
       return self.root + '/' + fileBlock



  def _removeRootPath(self, fileBlock, strict = False):
    """
    Returns a FileBlock name with the leading root path (self.root)
    removed.

    The argument strict controls the behaviour of the method for the
    case when the FileBlock does not start with the root path (which is
    unexpected). If strict is False, it just prints a warning and
    returns None. If strict is True, it prints the warning and also
    raises an exception.

    This method is used to hide the LFC's root path of DLS in the
    FileBlock names that are shown to users (since LFC's API gets and
    returns them in an absolute form). That fileBlock does not start with
    the root path means probably that the FileBlock is not in the tree
    under the specified root path.
   
    @exception ValueError: If fB does not start with root path and strict=True
    
    @param fileBlock: the FileBlock to be changed, as a string
    @param strict: boolean (def. False) for an exception to be raised on error
      
    @return: the adapted FileBlock name as a string, or None if fileBlock
             does not start with the root path
    """

    if(fileBlock.startswith(self.root+'/')):
       result = fileBlock.replace(self.root+'/', "", 1)
       result = result.strip('/')
       if(not result): result = '/'
       return result       
    else:
       msg = "FileBlock %s not under root path %s" % (fileBlock, self.root)
       if(strict):
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: "+msg+". Skipping."
          code = 100
          raise ValueError("Error: "+msg, code)
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: "+msg
          return None



  def _checkAndCreateDir(self, dir, filemode=0755):
    """
    Checks if the specified directory and all its parents exist in the DLS server.
    Otherwise, it creates the whole tree (with the specified filemode).

    This method is required to support the automatic creation of parent directories
    within the add() method.

    If the specified directory exists, or if it can be created, the method returns 
    correctly. Otherwise (there is an error creating the specified dir or one of its
    parents), the method returns raises an exception.

    @exception DlsLfcApiError: On error with the DLS catalog

    @param dir: the directory tree to be created, as a string
    @param filemode: the filemode to be used in the directories creation
    """
    dir = dir.rstrip('/')

    if(dir == ""):
       # The root directory is already there
       return
    
    parentdir = dir[0:dir.rfind('/')+1]  
    fstat = lfc.lfc_filestatg()
    if(lfc.lfc_statg(dir, "", fstat)<0):
       self._checkAndCreateDir(parentdir, filemode)
       guid = commands.getoutput('uuidgen')         
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_mkdirg(",dir,",",guid,",",filemode,")"
       if(lfc.lfc_mkdirg(dir, guid, filemode) < -1):
          code = lfc.cvar.serrno
          msg = "Error creating parent directory %s: %s" % (dir, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)


  def _addFileBlock(self, dlsFileBlock, **kwd):  
    """
    Adds the specified FileBlock to the DLS and returns its GUID.

    If the DlsFileBlock object includes the GUID of the FileBlock, or
    it is included as "guid" attribute then that value is used in the
    catalog, otherwise one is generated.

    The DlsFileBlock object may include FileBlock attributes. See the
    add method for the list of supported attributes.
    
    The method will raise an exception in the case that there is an
    error adding the FileBlock.

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param dlsFileBlock: the dlsFileBlock object to be added to the DLS

    @return: the GUID of the FileBlock (newly set or existing one)
    """

    # Extract interesting values 
    lfn = dlsFileBlock.name
    lfn = self._checkDlsHome(lfn)
    userlfn = self._removeRootPath(lfn, strict = True)
    attrList = dlsFileBlock.attribs
    guid = dlsFileBlock.getGuid()

    # Keywords
    createParent = True
    if(kwd.has_key("createParent")):
       createParent = kwd.get("createParent")
    
    if(self.verb >= DLS_VERB_HIGH):
      print "--addFileBlock("+str(dlsFileBlock)+")"

  # Analyze attribute list
  
    # Defaults
    [filemode, filesize, csumtype, csumvalue] = [0775, long(1000), '', '']
 
    # Get what was passed
    for attr in attrList:
       if(attr == "guid"):
          guid=attrList[attr]
          continue
       if(attr == "filemode"): 
          if(isinstance(attrList[attr], str)):
             if((attrList[attr])[0] == '0'):
                filemode = int(attrList[attr], 8)
             else:
                filemode = int(attrList[attr])
          else:
             filemode = int(attrList[attr])              
          continue
       if(attr == "filesize"):     
          filesize=long(attrList[attr])
          continue
       if(attr == "csumtype"):  
          csumtype=attrList[attr]
          continue
       if(attr == "csumvalue"): 
          csumvalue=attrList[attr]
          continue
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: Attribute %s of FileBlock (%s) unknown." % (attr, userlfn)
 
  # Check if entry exists
    fstat=lfc.lfc_filestatg()
    if(lfc.lfc_statg(lfn, "", fstat) <0):
    
       # If it does not exist...
 
       # First, check parents only for absolute paths (always the case!), and if asked for
       if(createParent and lfn.startswith('/')):
          if(self.verb >= DLS_VERB_HIGH):
             print "--Checking parents of requested FileBlock: "+lfn
          parentdir = lfn[0:lfn.rfind('/')+1]
          self._checkAndCreateDir(parentdir, filemode)
 
       # Now, create it
       if(not guid):
          guid=commands.getoutput('uuidgen')         
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_creatg(\""+lfn+"\", \""+guid+"\",",filemode,")"   
       if(lfc.lfc_creatg(lfn, guid, filemode) < 0):
          code = lfc.cvar.serrno
          msg = "Error creating the FileBlock %s: %s" % (userlfn, lfc.sstrerror(code))
          if(self.verb >= DLS_VERB_WARN):
            print "Warning: " + msg
          raise DlsLfcApiError(msg, code)
          
       # And set the size and cksum
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_setfsizeg(\""+guid+"\",",filesize,",",csumtype,",",csumvalue,")"
       if (lfc.lfc_setfsizeg(guid, filesize, csumtype, csumvalue)):
          code = lfc.cvar.serrno
          msg = "Error setting filesize/cksum for LFN %s: %s" % (userlfn, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)
 
    else:
       # If it exists, get the real GUID
       guid=fstat.guid
 
     # If everything went well, return the GUID
    return guid



  def _addLocationToGuid(self, guid, dlsLocation, fileBlock = None):  
    """
    Adds the specified location to the FileBlock identified by the specified
    GUID in the DLS.
    
    If the DlsFileBlock object includes the SURL of the location, or
    it is included as "sfn" attribute then that value is used in the catalog,
    otherwise one is generated.

    The GUID should be specified as a string in the format defined for the 
    Universally Unique IDentifier (UUID). Do not include any prefix, such as
    "guid://".

    The DlsLocation object may include location attributes. See the add
    method for the list of supported attributes.
    
    The method will raise an exception in the case that the specified GUID
    does not exist or there is an error adding the location.

    @exception DlsLfcApiError: On error with the DLS catalog

    @param guid: the GUID for the FileBlock, as a string
    @param dlsLocation: the DlsLocation objects to be added as location
    @param fileBlock: the FileBlock name matching the guid (for messages only)
    """
    se = dlsLocation.host 
    sfn = dlsLocation.getSurl()
    attrList = dlsLocation.attribs
    if(fileBlock): userfile = fileBlock
    else:          userfile = guid

    # Default values
    if(not sfn):
      sfn = "srm://"+str(se)+'/'+str(guid) 
    [f_type, ptime] = ['V', 0]
 
    # Get what was passed
    for attr in attrList:
       if(attr == "sfn"):      
          sfn = attrList[attr]
          continue
       if(attr == "f_type"):      
          f_type = attrList[attr]
          continue
       if(attr == "ptime"):      
          ptime = int(attrList[attr])
          continue
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: Attribute %s of location %s of %s unknown." % (attr, sfn, userfile)
 
  # Register location 
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_addreplica(\""+guid+"\", None, \""+\
              se+"\", \""+sfn+"\",'-',\""+f_type+"\", \"\", \"\")"
    if(lfc.lfc_addreplica(guid, None, se, sfn, '-', f_type, "", "") < 0):
       code = lfc.cvar.serrno
       msg = "Error adding location %s for FileBlock %s: %s" % (se, userfile, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
         print "Warning: " + msg
       raise DlsLfcApiError(msg, code)
    
  # Set pin time
    if(ptime):
       if(lfc.lfc_setptime(sfn, ptime)<0):
          code = lfc.cvar.serrno
          codestr = lfc.sstrerror(code)
          msg = "Error setting pin time for location %s of %s: %s" % (sfn, userfile, codestr)
          raise DlsLfcApiError(msg, code)
 
    return(0)


  def _updateFileBlock(self, dlsFileBlock):  
    """
    Updates the attributes of the specified FileBlock in the DLS.

    If the DlsFileBlock object includes the GUID of the FileBlock, then
    that value is used to identify the FileBlock in the catalog, rather
    than its name. Otherwise the name is used.

    The attributes are specified as a dictionary in the attributes data
    member of the spefied dlsFileBlock object. See the update method for
    the list of supported FileBlock attributes.
    
    The method will raise an exception in the case that the FileBlock does
    not exist or that there is an error updating its attributes.

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param dlsFileBlock: the dlsFileBlock object to be added to the DLS
    """
 
    lfn = dlsFileBlock.name 
    lfn = self._checkDlsHome(lfn)
    userlfn = self._removeRootPath(lfn, strict = True)

    guid = dlsFileBlock.getGuid()
    attrList = dlsFileBlock.attribs
       
    if(self.verb >= DLS_VERB_HIGH):
       print "--updateFileBlock("+str(dlsFileBlock)+")"
 
    # Check if entry exists
    fstat=lfc.lfc_filestatg()
    if(guid):
      if(lfc.lfc_statg("", guid, fstat) <0):
        code = lfc.cvar.serrno
        msg = "Error accessing FileBlock(%s): %s" % (guid, lfc.sstrerror(code))
        raise NotAccessibleError(msg, code)
    else:
      if(lfc.lfc_statg(lfn, "", fstat) <0):
        code = lfc.cvar.serrno
        msg = "Error accessing FileBlock(%s): %s" % (userlfn, lfc.sstrerror(code))
        raise NotAccessibleError(msg, code)
 
    # Get current guid, filesize, csumtype, csumvalue
    guid = fstat.guid
    filesize = fstat.filesize
    csumtype = fstat.csumtype
    csumvalue = fstat.csumvalue
    
    # Analyze attribute list to modify what was passed
    update = False
    for attr in attrList:
       if(attr == "filesize"):
          filesize = long(attrList[attr])
          update = True
          continue
       if(attr == "csumtype"):  
          csumtype = attrList[attr]
          update = True
          continue
       if(attr == "csumvalue"): 
          csumvalue = attrList[attr]
          update = True
          continue
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: Attribute %s of FileBlock %s unknown." % (attr, userlfn)
 
    # Set the size and cksum
    if(update):
       if(self.verb >= DLS_VERB_HIGH):
         print "--lfc.lfc_setfsizeg(\""+guid+"\",",filesize,",",csumtype,",",csumvalue,")"
       if(lfc.lfc_setfsizeg(guid, filesize, csumtype, csumvalue)):
         code = lfc.cvar.serrno
         msg = "Error setting the size/cksum for FileBlock(%s): %s" % (guid, lfc.sstrerror(code))
         raise DlsLfcApiError(msg, code)



  def _updateSurl(self, dlsLocation):  
    """
    Updates the attributes of the specified FileBlock location (identified 
    by its SURL).
   
    The DlsLocation object must include internal SURL field set, so that the 
    FileBlock copy can be identified. Otherwise, an exception is raised.

    The attributes are specified as a dictionary in the attributes data member
    of the dlsLocation object. See the update method for the list of supported
    location attributes.
    
    The method will raise an exception in case the specified location does not
    exist or there is an error updating its attributes.

    @exception DlsLfcApiError: On error with the DLS catalog

    @param dlsLocation: the DlsLocation object whose attributes are to be updated
    """

    sfn = dlsLocation.getSurl()
    if(not sfn):
      msg = "Error updating location(%s): the SURL field is not specified" % dlsLocation.name
      raise DlsLfcApiError(msg)
      
    if(self.verb >= DLS_VERB_HIGH):
      print "--updateSurl("+str(dlsLocation)+")"

    attrList = dlsLocation.attribs
 
    # Analyze attribute list to modify what was passed
    update_atime = False
    update_ptime = False
    for attr in attrList:
       if(attr == "atime"):      
          update_atime = True
          continue
       if(attr == "ptime"):
          ptime = int(attrList[attr])
          update_ptime = True
          continue
       else:
          if(self.verb >= DLS_VERB_WARN):
             print "Warning: Attribute %s of location (%s) unknown." % (attr, sfn)
 
    # Set pin time
    if(update_ptime):
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_setptime(\""+sfn+"\",",ptime,")"   
       if(lfc.lfc_setptime(sfn, ptime)<0):
          code = lfc.cvar.serrno
          msg = "Error setting pin time for location(%s): %s" % (sfn, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)         
 
    # Set access time
    if(update_atime):
       if(self.verb >= DLS_VERB_HIGH):
          print "--lfc.lfc_atime(\""+sfn+"\")"   
       if(lfc.lfc_setratime(sfn)<0):
          code = lfc.cvar.serrno
          msg = "Error accessing access time for location(%s): %s" % (sfn, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)



  def _deleteFileBlock(self, fileBlock):  
    """
    Removes the specified FileBlock from the DLS catalog.

    The FileBlock is specified as a string, and it can be a primary
    FileBlock name or a symlink. In  the first case, the removal will only
    succeed if the FileBlock has no associated location. The second one
    will succeed even in that case.

    The method will raise an exception in the case that there is an
    error removing the FileBlock (e.g. if the FileBlock does not exist, or
    not all the associated locations have been removed).

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param fileBlock: the FileBlock to be removed, as a string
    """
 
    lfn = fileBlock
    userlfn = self._removeRootPath(lfn, strict = True)
   
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_unlink(\""+lfn+"\")"
    if(lfc.lfc_unlink(lfn)<0):
       code = lfc.cvar.serrno
       msg = "Error deleting FileBlock %s: %s" % (userlfn, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
          print "Warning: "+msg
       raise DlsLfcApiError(msg, code)


  def _deleteSurl(self, surl): 
    """
    Removes the specified FileBlock location (identified by its SURL),
    
    The method will raise an exception in case the specified location does not
    exist or there is an error removing it.

    The SURL should be specified as a string with a particular format (which
    is sometimes SE dependent). It is usually something like:
    "srm://SE_host/some_string".

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param surl: the SURL to be deleted, as a string
    """
 
    sfn = surl 
 
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_delreplica(\"\", None, \""+sfn+"\")"   
    if(lfc.lfc_delreplica("", None, sfn) < 0):   
       code = lfc.cvar.serrno
       msg = "Error deleting location %s: %s" % (sfn, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
          print "Warning: "+msg
       raise DlsLfcApiError(msg, code)


  def _deleteDir(self, dir):  
    """
    Removes the specified directory from FileBlock namespace in the DLS
    catalog, if the directory is empty.

    The directory is specified as a string.

    The method will raise an exception in the case that there is an
    error removing the directory (e.g. if it is not empty, or it does
    exist). 

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param dir: the directory to be removed, as a string
    """
 
    lfn = dir
    userlfn = self._removeRootPath(lfn, strict = True)
   
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_rmdir(\""+lfn+"\")"
    if(lfc.lfc_rmdir(lfn)<0):
       code = lfc.cvar.serrno
       msg = "Error deleting FileBlock directory %s: %s" % (userlfn, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
          print "Warning: "+msg
       raise DlsLfcApiError(msg, code)

       
  def _listFileBlock(self, lfn, longList = True):
    """
    Returns a FileBlock object with information regarding the specified FileBlock.

    The FileBlock is specified as a DlsFileBlock object or as a string with
    its name.

    If longList is specified, the returned object contains some attributes as
    specifed in the listFileBlocks method.

    The method will raise an exception in the case that there is an error
    listing the FileBlock. 

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param lfn: the FileBlock to be listed, as a string or DlsFileBlock object
    @param longList: boolean (default True) for adding attrs to the FileBlock

    @return: the FileBlock object for the specified FileBlock
    """
    # Check what was passed (DlsFileBlock or string)
    if(isinstance(lfn, DlsFileBlock)):
      lfn = lfn.name
    else:
      lfn = lfn
    lfn = self._checkDlsHome(lfn)
    userlfn = self._removeRootPath(lfn, strict = True)

    # Get info
    fstat = lfc.lfc_filestatg()
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_statg(%s)" % (lfn)
    if(lfc.lfc_statg(lfn, "", fstat)<0):
       code = lfc.cvar.serrno
       msg = "Error accessing FileBlock %s: %s" % (userlfn, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
          print "Warning: %s" % (msg)
       raise DlsLfcApiError(msg, code)

    # Set the GUID in any case
    result = DlsFileBlock(userlfn)
    result.setGuid(fstat.guid)
    
    # Not listing, just the name    
    if(not longList):
       return result

    # Long listing
    result.attribs["filemode"] =  fstat.filemode
    result.attribs["nlink"] =  fstat.nlink
    result.attribs["uid"] =  fstat.uid
    result.attribs["gid"] =  fstat.gid
    result.attribs["filesize"] =  fstat.filesize
    result.attribs["mtime"] =  fstat.mtime
    result.attribs["csumtype"] =  fstat.csumtype
    result.attribs["csumvalue"] =  fstat.csumvalue

    # Return
    return result


  def _listDir(self, dir, longList = False, recursive = False):
    """
    Returns the entries of the specified directory from FileBlock namespace in
    the DLS catalog, as a list of FileBlock objects. A slash is appended to the
    name of those entries that are directories in the FileBlocks namespace ir name,
    to make it clear that they are directories and not normal FileBlocks.

    The directory is specified as a string.

    If longList is specified, each returned object contains some attributes as
    specifed in the listFileBlocks method.

    If recursive is used, the list contains information on the FileBlocks
    contained under the specified directory and its subdirectories. In this
    case, the directory FileBlock themselves are not included in the list,
    unless the are empty.
    
    The method will raise an exception in the case that there is an error
    listing the directory. 

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param dir: the directory to be listed, as a string
    @param longList: boolean (default False) for adding attrs to the FileBlocks

    @return: the list of FileBlock objects for the specified directory
    """
    # Check what was passed (DlsFileBlock or string)
    if(isinstance(dir, DlsFileBlock)):
      lfn = dir.name
    else:
      lfn = dir
    dir = self._checkDlsHome(lfn)
    userdir = self._removeRootPath(dir, strict = True)

    if(recursive):
       subdirlist = [] # Subdirectories to remove when we are done with current one

    # Open dir 
    dir_p = lfc.lfc_DIR()
    dir_entry = lfc.lfc_direnstatg()
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_opendirg(%s, \"\")"  % (dir)
    dir_p = lfc.lfc_opendirg(dir , "")
    if(not dir_p):
       code = lfc.cvar.serrno
       msg = "Error opening specified dir %s: %s" % (userdir, lfc.sstrerror(code))
       raise DlsLfcApiError(msg, code)

    # Loop on dir
    fBList = []
    dir_entry = lfc.lfc_readdirg(dir_p)
    if(not dir_entry):
       code = lfc.cvar.serrno
       if(code != 0):
          lfc.lfc_closedir(dir_p)
          msg = "Error reading dir %s: %s" % (userdir, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)

       
    while(dir_entry):

      # Set always the name and GUID
      fB = DlsFileBlock(dir_entry.d_name)
      fB.setGuid(dir_entry.guid)

      if(longList):
         # Long listing
         fB.attribs["filemode"] =  dir_entry.filemode
         fB.attribs["nlink"] =  dir_entry.nlink
         fB.attribs["uid"] =  dir_entry.uid
         fB.attribs["gid"] =  dir_entry.gid
         fB.attribs["filesize"] =  dir_entry.filesize
         fB.attribs["mtime"] =  dir_entry.mtime
         fB.attribs["csumtype"] =  dir_entry.csumtype
         fB.attribs["csumvalue"] =  dir_entry.csumvalue
       
      # Check for subdirectories (if recursive)
      if(dir_entry.filemode & S_IFDIR):
         if(recursive):
            if(not userdir.endswith('/')):
               subdir = userdir + '/' + dir_entry.d_name
            else:
               subdir = userdir + dir_entry.d_name
            fB.name = subdir
            subdirlist.append(fB)
         else:
            fB.name += "/"
            fBList.append(fB)

      # Normal FileBlock -> append it
      else:
         fBList.append(fB)

                 
      # Next entry
      dir_entry = lfc.lfc_readdirg(dir_p)
      if(not dir_entry):
         code = lfc.cvar.serrno
         if(code != 0):
            lfc.lfc_closedir(dir_p)
            msg = "Error reading dir %s: %s" % (userdir, lfc.sstrerror(code))
            raise DlsLfcApiError(msg, code)

           
    # Close the directory
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_closedir(%s, \"\")"  % (dir)
    if(lfc.lfc_closedir(dir_p) < 0):
       code = lfc.cvar.serrno
       msg = "Error closing dir %s: %s" % (userdir, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
          print "Warning: %s" % (msg)


    # Now loop on the subdirectories (if recursive)
    if(recursive):
       for subdir in subdirlist:
          # Stat subdir (just to avoid the 60 seconds timeout!)
          fstat = lfc.lfc_filestatg()
          if(lfc.lfc_statg(self._checkDlsHome(subdir.name), "", fstat)<0):
             code = lfc.cvar.serrno
             msg = "Error accessing FileBlock %s: %s" % (subdir.name, lfc.sstrerror(code))
             if(self.verb >= DLS_VERB_WARN):
                print "Warning: %s" % (msg)
          # List subdir
          subList = self._listDir(subdir, longList, recursive)
          subdir_tokens = (subdir.name).split('/')
          bare_subdir = subdir_tokens.pop()

          # If the directory was empty, append just itself
          if(not subList):
              subdir.name = bare_subdir + '/'
              fBList.append(subdir)
          # Otherwise, append its contents
          else:
            for i in subList:
                i.name = bare_subdir + '/' + i.name
                fBList.append(i)

    # Return
    return fBList


  def _renameFileBlock(self, oldPath, newPath, **kwd):
    """
    Renames a FileBlock in the DLS as described in
    dlsLfcApi.renameFileBlock.

    The FileBlock names (paths) are specified as strings.

    The method will raise an exception in the case that there is an
    error adding the FileBlock or creating the necessary parent
    directories of the new FileBlock.

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param oldPath: the FileBlock to be added to the DLS, as a string
    @param newPath: the FileBlock to be added to the DLS, as a string
    """
    # Adapt names
    oldLfn = self._checkDlsHome(oldPath)
    userOldLfn = self._removeRootPath(oldLfn, strict = True)
    newLfn = self._checkDlsHome(newPath)
    userNewLfn = self._removeRootPath(newLfn, strict = True)
    
    # Keywords
    createParent = False 
    if(kwd.has_key("createParent")):
       createParent = kwd.get("createParent")
       
    # First, check parents for destination FileBlock, if asked for
    if(createParent and newLfn.startswith('/')):
       if(self.verb >= DLS_VERB_HIGH):
          print "--Checking parents of requested FileBlock: "+newLfn
       parentdir = newLfn[0:newLfn.rfind('/')+1]
       self._checkAndCreateDir(parentdir)

    # Now, perform the renaming
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_rename(\""+oldLfn+"\", \""+newLfn+"\")"   
    if(lfc.lfc_rename(oldLfn, newLfn) < 0):
       code = lfc.cvar.serrno
       msg = "Error renaming FileBlock %s as %s: %s"%(userOldLfn, userNewLfn, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
         print "Warning: " + msg
       raise DlsLfcApiError(msg, code)
          



  def _getLocsFromDir(self, dir, recursive = False):
    """
    Returns a list of locations where there is a replica of any of the
    FileBlocks in the specified directory from FileBlock namespace in
    the DLS catalog, as a list of strings (hostnames).

    The directory is specified as a string.

    If recursive is used, the list contains information on the FileBlocks
    contained under the specified directory and its subdirectories.

    The method will raise an exception in the case that there is an error
    listing the directory. 

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param dir: the directory to be listed, as a string
    
    @return: the list of locations for the specified dir, as a list of strings
    """
    # Check what was passed (DlsFileBlock or string)
    if(isinstance(dir, DlsFileBlock)):
      lfn = dir.name
    else:
      lfn = dir
    dir = self._checkDlsHome(lfn)
    userdir = self._removeRootPath(dir, strict = True)

    locList = {}
    if(recursive):
       subdirlist = [] # Subdirectories to remove when we are done with current one

    # Open dir 
    dir_p = lfc.lfc_DIR()
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_opendirg(%s, \"\")"  % (dir)
    dir_p = lfc.lfc_opendirg(dir , "")
    if(not dir_p):
       code = lfc.cvar.serrno
       msg = "Error opening specified dir %s: %s" % (userdir, lfc.sstrerror(code))
       raise DlsLfcApiError(msg, code)

    # Loop on dir
    dir_entry = lfc.lfc_direnrep()
    dir_read = lfc.lfc_readdirxr(dir_p, "")
    if(not dir_read):
       code = lfc.cvar.serrno
       if(code != 0):
          lfc.lfc_closedir(dir_p)
          msg = "Error reading dir %s: %s" % (userdir, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)
    else:
       dir_entry, repList = dir_read
       
    while(dir_read):
    
      if(self.verb >= DLS_VERB_HIGH):    
         print "--Read:",dir_entry.d_name

      # Is it a subdirectory?
      if(dir_entry.filemode & S_IFDIR):
         if(recursive):
            if(not userdir.endswith('/')):
               subdir = userdir + '/' + dir_entry.d_name
            else:
               subdir = userdir + dir_entry.d_name
            fB = DlsFileBlock(subdir)
            subdirlist.append(fB)
            
      # Normal FileBlock -> add its locations (dict will avoid repetitions)
      else:
         if(repList):
            for rep in repList:
               locList[rep.host] = 1
               if(self.verb >= DLS_VERB_HIGH):    
                  print "--Added:",rep.host
      
      # Next entry
      dir_read = lfc.lfc_readdirxr(dir_p, "")
      if(not dir_read):
         code = lfc.cvar.serrno
         if(code != 0):
            lfc.lfc_closedir(dir_p)
            msg = "Error reading dir %s: %s" % (userdir, lfc.sstrerror(code))
            raise DlsLfcApiError(msg, code)
      else:
         dir_entry, repList = dir_read

           
    # Close the directory
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_closedir(%s, \"\")"  % (dir)
    if(lfc.lfc_closedir(dir_p) < 0):
       code = lfc.cvar.serrno
       msg = "Error closing dir %s: %s" % (userdir, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
          print "Warning: %s" % (msg)


    # Now loop on the subdirectories (if recursive)
    if(recursive):
       for subdir in subdirlist:
          # Stat subdir (just to avoid the 60 seconds timeout!)
          fstat = lfc.lfc_filestatg()
          if(lfc.lfc_statg(self._checkDlsHome(subdir.name), "", fstat)<0):
             code = lfc.cvar.serrno
             msg = "Error accessing FileBlock %s: %s" % (subdir.name, lfc.sstrerror(code))
             if(self.verb >= DLS_VERB_WARN):
                print "Warning: %s" % (msg)
          # List subdir
          subList = self._getLocsFromDir(subdir, recursive)
          for loc in subList:
             locList[loc]=1

    # Return
    return locList.keys()


  def _getEntriesFromDir(self, dir, location = "", recursive = False):
    """
    Returns a list of FileBlocks in the specified directory that
    have a replica in the specified location. If no location is
    specified, then entries for all the FileBlocks in the directory
    and their respective locations are returned.
    
    The result is returned as a list of DlsEntry objects.

    The directory is specified as a string or a DlsFileBlock object.

    If recursive is used, the list contains information on the FileBlocks
    contained under the specified directory and its subdirectories.

    The method will raise an exception in the case that there is an error
    listing the directory. 

    @exception DlsLfcApiError: On error with the DLS catalog
    
    @param dir: the directory to be listed, as a string or DlsFileBlock object
    @param location: only FileBlocks of this location will be returned, as a string,
                     or "" for FileBlocks in any location
    
    @return: the list of DlsEntry objects for the specified dirs and location
    """

    # Check what was passed (DlsFileBlock or string)
    if(isinstance(dir, DlsFileBlock)):
      lfn = dir.name
    else:
      lfn = dir
    dir = self._checkDlsHome(lfn)
    userdir = self._removeRootPath(dir, strict = True)

    entryList = []
    if(recursive):
       subdirlist = [] # Subdirectories to remove when we are done with current one

    # Open dir 
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_opendirg(%s, \"\")"  % (dir)
    dir_p = lfc.lfc_opendirg(dir , "")
    if(not dir_p):
       code = lfc.cvar.serrno
       msg = "Error opening specified dir %s: %s" % (userdir, lfc.sstrerror(code))
       raise DlsLfcApiError(msg, code)

    # Loop on dir
    # TODO: The location should be specified, but we get a strange abort
    #       However, results are performance are basically the same
#    dir_read = lfc.lfc_readdirxr(dir_p, location)
    dir_read = lfc.lfc_readdirxr(dir_p, "")
    if(not dir_read):
       code = lfc.cvar.serrno
       if(code != 0):
          lfc.lfc_closedir(dir_p)
          msg = "Error reading dir %s: %s" % (userdir, lfc.sstrerror(code))
          raise DlsLfcApiError(msg, code)
    else:
       dir_entry, repList = dir_read


    while(dir_read):
    
      if(self.verb >= DLS_VERB_HIGH):    
         print "--Read:",dir_entry.d_name

      locList = []
      fB = DlsFileBlock(dir_entry.d_name)
      fB.attribs["filemode"] =  dir_entry.filemode
      fB.setGuid(dir_entry.guid)
      fB.attribs["filesize"] =  dir_entry.filesize
       
      # Check for subdirectories (if recursive)
      if(dir_entry.filemode & S_IFDIR):
         if(recursive):
            if(not userdir.endswith('/')):
               subdir = userdir + '/' + dir_entry.d_name
            else:
               subdir = userdir + dir_entry.d_name
            fB.name = subdir
            subdirlist.append(fB)
         else:
            fB.name += "/"
            entryList.append(DlsEntry(fB))

      # Normal FileBlock 
      else:
         # Append the entry only for the specified location
         # If location = "", append all the found entries
         if(repList):
            if (not (isinstance(repList, list) or isinstance(repList, tuple))):
               repList = [repList]
            for i in repList:
               if((location == i.host) or (not location)):
                  loc = DlsLocation(i.host)
                  loc.setSurl(i.sfn)
                  locList.append(loc)                  

         if(locList or (not location)):
            entry = DlsEntry(fB, locList)
            entryList.append(entry)
            if(self.verb >= DLS_VERB_HIGH):    
               print "--Added:",entry.fileBlock.name
            
      # Next entry
      # TODO: The location should be specified, but we get a strange abort
      #       However, results and performance are basically the same
#      dir_read = lfc.lfc_readdirxr(dir_p, location)
      dir_read = lfc.lfc_readdirxr(dir_p, "")
      if(not dir_read):         
         code = lfc.cvar.serrno
         if(code != 0):
            lfc.lfc_closedir(dir_p)
            msg = "Error reading dir %s: %s" % (userdir, lfc.sstrerror(code))
            raise DlsLfcApiError(msg, code)
      else:
         dir_entry, repList = dir_read

           
    # Close the directory
    if(self.verb >= DLS_VERB_HIGH):
       print "--lfc.lfc_closedir(%s, \"\")"  % (dir)
    if(lfc.lfc_closedir(dir_p) < 0):
       code = lfc.cvar.serrno
       msg = "Error closing dir %s: %s" % (userdir, lfc.sstrerror(code))
       if(self.verb >= DLS_VERB_WARN):
          print "Warning: %s" % (msg)


    # Now loop on the subdirectories (if recursive)
    if(recursive):
       for subdir in subdirlist:
          # Stat subdir (just to avoid the 60 seconds timeout!)
          fstat = lfc.lfc_filestatg()
          if(lfc.lfc_statg(self._checkDlsHome(subdir.name), "", fstat)<0):
             code = lfc.cvar.serrno
             msg = "Error accessing FileBlock %s: %s" % (subdir.name, lfc.sstrerror(code))
             if(self.verb >= DLS_VERB_WARN):
                print "Warning: %s" % (msg)
          # List subdir
          subList = self._getEntriesFromDir(subdir, location, recursive)
          subdir_tokens = (subdir.name).split('/')
          bare_subdir = subdir_tokens.pop()

          # If the directory was empty, append just itself (for the location ="" case)
          if((not subList) and (not location)):
              subdir.name = bare_subdir + '/'
              entryList.append(DlsEntry(subdir))
          # Otherwise, append its contents
          else:
            for i in subList:
                i.fileBlock.name = bare_subdir + '/' + i.fileBlock.name
                entryList.append(i)

    # Return
    return entryList



##################################################333
# Unit testing                                                                                                   
if __name__ == "__main__":
                                                                                    
   import dlsClient
   from dlsDataObjects import *                                                                                                   
## use DLS server
   type="DLS_TYPE_LFC"
   server ="lfc-cms-test.cern.ch/grid/cms/DLS/LFCProto"
   try:
     api = dlsClient.getDlsApi(dls_type=type,dls_endpoint=server)
   except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()

## get Locations given a fileblock
   fb="bt_DST871_2x1033PU_g133_CMS/bt03_tt_2tauj"
   try:
     entryList=api.getLocations([fb])
   except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
     sys.exit()
   if(not isinstance(entryList, list)):
     entryList = [entryList]
   for entry in entryList:
    for loc in entry.locations:
     print loc.host


