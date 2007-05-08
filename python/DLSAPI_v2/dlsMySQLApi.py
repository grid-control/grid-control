#
# $Id: dlsMySQLApi.py,v 1.13 2006/09/24 17:21:06 afanfani Exp $
#
# DLC Client. $Name: DLS_0_1_1 $. 
# Antonio Delgado Peris. CIEMAT. CMS.
# client for MySQL prototype : A. Fanfani  

"""
 This module implements a CMS Dataset Location Service (DLS) client
 interface as defined by the dlsApi module. This implementation relies
 on a DLS server using a LCG File Catalog (LFC) as back-end.

 The module contains the DlsMySQLApi class that implements all the methods
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
#AF adding:
#import dlsDataObjects
from dlsDataObjects import *
import socket
import string
#########################################
# Module globals
#########################################
#DLS_VERB_NONE = 0    # print nothing
#DLS_VERB_INFO = 5    # print info
#DLS_VERB_WARN = 10   # print only warnings (to stdout)
#DLS_VERB_HIGH = 20   # print warnings (stdout) and error messages (stderr)

#########################################
# DlsMySQLApiError class
#########################################

class DlsMySQLApiError(dlsApi.DlsApiError):
  """
  Exception class for the interaction with the DLS catalog using the DlsMySQLApi
  class. It normally contains a string message (empty by default), and optionally
  an  error code (e.g.: if such is returned from the DLS).

  The exception may be printed directly, or its data members accessed.
  """

class NotImplementedError(DlsMySQLApiError):
  """
  Exception class for methods of the DlsApi that are not implemented (and
  should be by a instantiable API class).
  """

class ValueError(DlsMySQLApiError):
  """
  Exception class for invocations of DlsApi methods with an incorrect value
  as argument.
  """
  
class SetupError(DlsMySQLApiError):
  """
  Exception class for errors when setting up the system (configuration,
  communication errors...)
  """



#########################################
# DlsApi class
#########################################

class DlsMySQLApiError(dlsApi.DlsApiError):
  """
  Exception class for the interaction with the DLS catalog using the DlsMySQLApi
  class. It normally contains a string message (empty by default), and optionally
  an  error code (e.g.: if such is returned from the DLS).
                                                                                                     
  The exception may be printed directly, or its data members accessed.
  """

class DlsMySQLApi(dlsApi.DlsApi):
  """
  This class is an implementation of the DLS client interface, defined by
  the dlsApi.DlsApi class. This implementation relies on a Lcg File Catalog
  (LFC) as DLS back-end.

  Unless specified, all methods that can raise an exception will raise one
  derived from DlsMySQLApiError.
  """

  def __init__(self, dls_endpoint= None, verbosity = dlsApi.DLS_VERB_WARN):
    """
    Constructor of the class. It sets the DLS (MySQL proto) server to communicate with
    and the verbosity level.
 
    The verbosity level affects invocations of all methods in this object. See
    the dlsApi.DlsApi.setVerbosity method for information on accepted values.
      
    @exception SetupError: if no DLS server can be found.

    @param dls_endpoint: the DLS server to be used, as a string of form "hostname[:port]"
    @param verbosity: value for the verbosity level
    """

    dlsApi.DlsApi.__init__(self, dls_endpoint, verbosity)    
    
    #if(self.server):
    #  # Do whatever... 
    #  print " Using server %s"%self.server

    if(not self.server):
       raise SetupError("Could not set the DLS server to use")

    # Remove (ignore) the root directory
    dlsserver = self.server.split('/')[0]
    self.server = dlsserver

  ############################################
  # Methods defining the main public interface
  ############################################

  def add(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.add method.
    Refer to that method's documentation.

    Implementation specific remarks:

    No attribute are supported in the MySQL prototype
    """
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList
    else:
       theList = [dlsEntryList]

    for entry in theList:
      fb=entry.fileBlock.name
      for location in entry.locations:
            se=location.host
            #print "fb %s"%fb
            #print "ses %s"%se
            self.dls_connect()
            msg='add_replica?%s?%s'%(fb,se)
            if ( self.verb > 10 ) :
                print "Send:%s"%(msg)
            self.dls_send(msg)           
            msg=self.dls_receive()
            
            if ( self.verb > 10 ):
                if msg=="0":
                    print "Replica Registered"
                elif msg=="1":
                    print "Replica already stored"
                else:
                    msg="2"
                    print "Replica not registered"
            self.__client.close()
            #return int(msg)
#TODO : error code
    return 

  def delete(self, dlsEntryList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.delete method.
    Refer to that method's documentation.

    Implementation specific remarks:

    """
    # Make sure the argument is a list
    if (isinstance(dlsEntryList, list)):
       theList = dlsEntryList
    else:
       theList = [dlsEntryList]

    for entry in theList:
      fb=entry.fileBlock.name
      for location in entry.locations:
            se=location.host
            #print "fb %s"%fb
            #print "ses %s"%se
            self.dls_connect()
            msg='remove_replica?%s?%s'%(fb,se)
            if ( self.verb > 10 ):
                print "Send:%s"%(msg)
            self.dls_send(msg)
            msg=self.dls_receive()
            
            if ( self.verb > 10 ):
                if msg=="0":
                    print "Replica Deleted"
                elif msg=="1":
                    print "Replica Not present"
                else:
                    print "error: %s not Stored"%(msg) 
                    msg="2"
            
            self.__client.close()
            #return int(msg)
#TODO : error code
    return

    
  def getLocations(self, fileBlockList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getLocations method.
    Refer to that method's documentation.

    Implementation specific remarks:
    
    No attribute are supported in the MySQL prototype
    """
    # Make sure the argument is a list
    if (isinstance(fileBlockList, list)):
       theList = fileBlockList
    else:
       theList = [fileBlockList]

    entryList = []
    for fblock in theList:
            # Check what was passed (DlsFileBlock or string)
            if(isinstance(fblock, DlsFileBlock)):
              fb = fblock.name
            else:
              fb = fblock
            entry = DlsEntry(DlsFileBlock(fb))
            self.dls_connect()
            msg='show_replica_by_db?%s'%(fb)
            self.dls_send(msg)
            if ( self.verb > 10 ):
                print "Send: %s"%(msg)
            msg=self.dls_receive()
            if ( self.verb > 10 ):
                print "Received from server:"
            #print msg
            ses=string.split(msg,'\n')
            locList = []
            if ses == ['']:
              msg=" No locations found for %s"%fb
              code=4
              raise DlsMySQLApiError(msg, code)
            for se in ses: 
             loc = DlsLocation(se)
             locList.append(loc)
            entry.locations = locList
            entryList.append(entry)
            self.__client.close()
            #return 0
    return entryList
    

  def getFileBlocks(self, locationList, **kwd):
    """
    Implementation of the dlsApi.DlsApi.getFileBlocks method.
    Refer to that method's documentation.

    Implementation specific remarks:
    
    No attribute are supported in the MySQL prototype
    """ 
    # Make sure the argument is a list
    if (isinstance(locationList, list)):
       theList = locationList
    else:
       theList = [locationList]

    entryList = []
    for loc in theList:
            # Check what was passed (DlsLocation or string)
            if(isinstance(loc, DlsLocation)):
              se = loc.host
            else:
              se = loc 
            self.dls_connect()
            msg='show_replica_by_se?%s'%(se)
            self.dls_send(msg)
            if ( self.verb > 10 ):
                print "Send: %s"%(msg) 
            msg=self.dls_receive()
            if ( self.verb > 10 ):
                print "Received from server:"
            #print msg
            fblocks=string.split(msg,'\n')
            if fblocks == ['']:
              msg=" No fileblocks found for %s"%se
              code=4
              raise DlsMySQLApiError(msg, code)
            for fb in fblocks:
              entry = DlsEntry(DlsFileBlock(fb),[DlsLocation(se)])
              entryList.append(entry)
            self.__client.close()
            #return 0
    return entryList

  def getAllLocations(self,**kwd):
    """
    Implementation of the dlsApi.DlsApi.getAllLocations method.
    Refer to that method's documentation.
    """
    locList = []

    self.dls_connect()
    msg='show_allreplicas?'
    self.dls_send(msg)
    if ( self.verb > 10 ):
       print "Send: %s"%(msg)
    msg=self.dls_receive()
    if ( self.verb > 10 ):
        print "Received from server:"
    #print msg
    ses=string.split(msg,'\n')
    if ses == ['']:
      msg=" No locations found "
      code=4
      raise DlsMySQLApiError(msg, code)
    for se in ses:
     loc = DlsLocation(se)
     locList.append(loc)
    return locList 

  def startSession(self):
     pass
 
  def endSession(self):
     pass
 

  def renameFileBlock(self, oldFileBlock, newFileBlock, **kwd):
    """
    Implementation of the dlsApi.DlsApi.renameFileBlock method.
    Refer to that method's documentation.
    """
    # Check what was passed and extract interesting values
    if(isinstance(oldFileBlock, DlsFileBlock)):
       oldLfn = oldFileBlock.name
    else:
       oldLfn = oldFileBlock
    if(isinstance(newFileBlock, DlsFileBlock)):
       newLfn = newFileBlock.name
    else:
       newLfn = newFileBlock


    self.dls_connect()
    msg='rename_db?%s?%s'%(oldLfn,newLfn)
    if ( self.verb > 10 ) :
        print "Send:%s"%(msg)
    self.dls_send(msg)
    msg=self.dls_receive()
     
    if msg=="0":
          if ( self.verb > 10 ): print "Fileblock renamed"
    elif msg=="1":
          msgtxt = "Error renaming FileBlock %s as %s: %s does not exist"%(oldLfn, newLfn, oldLfn)
          print "Warning: "+ msgtxt
          raise DlsMySQLApiError(msgtxt, msg)
    elif msg=="3":
          msgtxt = "Error renaming FileBlock %s as %s: %s exists"%(oldLfn, newLfn,newLfn) 
          print "Warning: "+ msgtxt
          raise DlsMySQLApiError(msgtxt, msg)
    else:
          msg="2"
          msgtxt="Error renaming FileBlock %s as %s: Fileblock not renamed"%(oldLfn, newLfn)
          print "Warning: "+ msgtxt
          raise DlsMySQLApiError(msgtxt, msg)

    self.__client.close()



  ##################################
  # Other public methods (utilities)
  ##################################

  def changeFileBlocksLocation(self, org_location, dest_location):
    """
    Implementation of the dlsApi.DlsApi.changeFileBlocksLocation method.
    Refer to that method's documentation.
    """

    # Implement here...
    pass


  #########################
  # Internal methods
  #########################

  def clientsocket(self):
        """
        """
        self.__client=socket.socket ( socket.AF_INET, socket.SOCK_STREAM )

  def dls_connect(self):
        """
        """       
        host=self.server.split(':')[0]
        try:
          port=self.server.split(':')[1]
        except:
          port=18080
          pass
        if port==None:
           port=18080

        if ( self.verb > 10 ):
            print "Connecting to host: %s port: %d"%(host,int(port))

        self.clientsocket()
        
        try:
            self.__client.connect ( (host, int(port)) )
        except:
            msg="DLS Server don't respond. Server host: %s port: %d "%(host,int(port))
            code=3
            raise DlsMySQLApiError(msg, code)

  def dls_send(self,msg):
        """
        """
        totalsent=0
        MSGLEN=len(msg)
        sent=  sent=self.__client.send(str(MSGLEN).zfill(16))
        #print "Sent %s"%(str(MSGLEN).zfill(16))
        if sent == 0:
            raise RuntimeError,"Socket connection broken"
        else:
            while totalsent < MSGLEN:
                sent = self.__client.send(msg[totalsent:])
                #print "Sent %s"%(msg[totalsent:])
                if sent == 0:
                    raise RuntimeError,"Socket connection broken"
                totalsent = totalsent + sent


  def dls_receive(self):
        """
        """
        chunk =  self.__client.recv(16)
        #print "Received %s"%(chunk)
        if chunk == '':
            raise RuntimeError,"Socket connection broken"
        MSGLEN= int(chunk)
        msg = ''
        while len(msg) < MSGLEN:
            chunk =  self.__client.recv(MSGLEN-len(msg))
            #print "Received %s"%(chunk)
            if chunk == '':
                raise RuntimeError,"Socket connection broken"
            msg = msg + chunk
        return msg


##################################################333
# Unit testing

if __name__ == "__main__":

   import dlsClient
   from dlsDataObjects import *

## use DLS server
   type="DLS_TYPE_MYSQL"
   server ="lxgate10.cern.ch:18081"
   try:
     api = dlsClient.getDlsApi(dls_type=type,dls_endpoint=server)
   except dlsApi.DlsApiError, inst:
      msg = "Error when binding the DLS interface: " + str(inst)
      print msg
      sys.exit()
                                                                                                                 
## get FileBlocks given a location
   se="cmsboce.bo.infn.it"
   try:
     entryList=api.getFileBlocks(se)
   except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
     sys.exit()
   for entry in entryList:
      print entry.fileBlock.name


## get Locations given a fileblock
   fb="bt_DST871_2x1033PU_g133_CMS/bt03_tt_2tauj"
   #fb="testblock"
   try:
     entryList=api.getLocations([fb])
   except dlsApi.DlsApiError, inst:
     msg = "Error in the DLS query: %s." % str(inst)
     print msg
     sys.exit()
   for entry in entryList:
    for loc in entry.locations:
     print loc.host

## add a DLS entry
   fileblock=DlsFileBlock("testblock")
   location=DlsLocation("testSE")
   entry=DlsEntry(fileblock,[location])
   api.add([entry])
                                                                                                                 
## check the inserted entry
   entryList=api.getLocations(fileblock)
   #entryList=api.getLocations("testblock")
   for entry in entryList:
    for loc in entry.locations:
     print loc.host
                                                                                                                 
## delete a DLS entry
   loc=DlsLocation("testSE")
   entry=DlsEntry(fileblock,[loc])
   api.delete([entry])


