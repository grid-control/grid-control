#
# $Id: dliClient_types.py,v 1.4 2006/04/21 11:39:04 delgadop Exp $
#
# DliClient. $Name: DLS_0_1_1 $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module contains the classes Required to access the Data Location Interface
 (DLI) web service that are used by the main API for user applications: the 
 dliClient.DliClient class.

 The classes in this module use the Zolera SOAP Infrastructure (ZSI)
 (http://pywebsvcs.sourceforge.net/). The following code is a simplification
 of the generated one by ZSI (based on the WSDL of the DLI). ZSI requires PyXML.
"""

#########################################
# Imports 
#########################################
import ZSI
from ZSI import client
from ZSI.TCcompound import Struct
import urlparse, types
from dliClient import TypeError
from dliClient import ValueError


#########################################
# Module globals
#########################################
NAMESPACE = "urn:DataLocationInterface" 
METHOD_NAME = "listReplicas"


  
#########################################
# DliSOAP class
#########################################

class DliSOAP:
    """
    Main class for the binding with the remote DLI web service.
    It contains the method for querying the DLI.
    """

    def __init__(self, addr, **kw):
        """
        Constructor of the class. It receives the DLI web service endpoint
        as a required argument.

        @param addr: the DLI endpoint, as a string with format "[http://]hostname[:port]"
        """
        
        # Parse the service endpoint (extract host, port, url, for the Binding)
        try:
           if(addr.find("://") == -1):
             addr = "http://" + addr
           netloc = (urlparse.urlparse(addr)[1]).split(":") + [8085,]
           if not kw.has_key("host"):
               kw["host"] = netloc[0]
           if not kw.has_key("port"):
               kw["port"] = int(netloc[1])
           if not kw.has_key("url"):
               kw["url"] =  urlparse.urlparse(addr)[2]
        except Exception, inst:
           msg = "Incorrect format of specified DLI endpoint (%s): %s" % (addr, str(inst))
           raise ValueError(msg)

        # Create the Binding (connect to the web service)
        self.binding = client.Binding(**kw)


    def listReplicas(self, request):
        """
        Queries the DLI for the list of SURLs for the LFN/GUID/Dataset specified
        in the request. The request can be built with the
        dliClient_types.new_listReplicasRequest method.
        
        @param request: a listReplicaRequest object containing the input data and its type

        @return: a listReplicasResponseWrapper object, containing the list of SURLs 
        """

        # Check correct type is passed
        if not isinstance(request, listReplicasRequest) and\
            not issubclass(listReplicasRequest, request.__class__):
            raise TypeError, "%s incorrect request type" %(request.__class__)
            
        # Query    
        kw = {}
        response = self.binding.Send(None, None, request, soapaction="", **kw)
        response = self.binding.Receive(listReplicasResponseWrapper())

        # Check correct reply was received
        if not isinstance(response, listReplicasResponse) and\
            not issubclass(listReplicasResponse, response.__class__):
            raise TypeError, "%s incorrect response type" %(response.__class__)

        # Return received value
        return response



#######################################################
# Dli Client Data Objects (request, response) classes
#######################################################

class ArrayOfstring(ZSI.TCcompound.Array):
  """
  Helper class that wrappes the list of strings for use by the
  listReplicaResponse method
  """
  def __init__(self, name = None, ns = None, **kw):
     ZSI.TCcompound.Array.__init__(self, 'arrayTypeNS:string[]',\
                                   ZSI.TC.String(), pname = name, **kw)


class listReplicasRequest (ZSI.TCcompound.Struct): 
   """
   Packer of the request information for the listReplica method of the DLI service.
   Members inputDataType and inputData must be set to "lfn"/"guid"/"dataset"
   the first, and the filename whose replicas are to be retrieved, the second.
   """

   def __init__(self, name=METHOD_NAME, ns=NAMESPACE):
        """
        Constructor of the class.

        @param name: DLI method this class will be used with (should be the default)
        @param ns: namespace where this DLI method is defined (should be the default)
        """
   
        # Required method arguments
        self.inputDataType = None
        self.inputData = None
   
        # The output name (including namespace)
        oname = None
        if name:
            oname = name
            if ns:
                oname += ' xmlns="%s"' % ns

            ZSI.TC.Struct.__init__(self, listReplicasRequest,\
                   [\
                    ZSI.TC.String(pname="inputDataType",aname="inputDataType",optional=1),\
                    ZSI.TC.String(pname="inputData",aname="inputData",optional=1),\
                   ],\
                    pname=name, aname="%s" % name, oname=oname )



def new_listReplicasRequest(file, fileType = "lfn"):
   """
   Helper function to create the request, setting the typecode at the same time.

   @param file: the LFN/GUID/DataSet Id of the file/dataset to query upon
   @param fileType: the type of file identifier being used ("lfn"/"guid"/"dataset")

   @return: a listReplicaRequest object, ready to pass to the listReplicas method
   """

   listReplicasRequest.typecode = listReplicasRequest()
   aux = listReplicasRequest()
   aux.inputDataType = fileType
   aux.inputData = file
   return aux



class listReplicasResponse (ZSI.TCcompound.Struct): 
   """
   Packer of the response information from the listReplica method of the
   DLI service. It contains the replicas' SURLs as a list of strings (one
   string per SURL) in the publicly accessible "urlList" field. 
   """
   def __init__(self, name="listReplicasResponse", ns=NAMESPACE):
        """
        Constructor of the class.

        @param name: type of response object as defined in the WSDL (should be the default)
        @param ns: namespace where the "name" is defined (should be the default)
        """

        self.urlList = ArrayOfstring()

        oname = None
        if name:
            oname = name
            if ns:
                oname += ' xmlns="%s"' % ns
            ZSI.TC.Struct.__init__(self, listReplicasResponse,\
                                   [ArrayOfstring( name="urlList", ns=ns ),],\
                                   pname=name, aname="%s" % name, oname=oname )
                                   


class listReplicasResponseWrapper(listReplicasResponse):
    """
    Wrapper class around listReplicasResponse to have the typecode included
    (cannot be done inside the own class definition).
    """

    typecode = listReplicasResponse()
    
    def __init__( self, name=None, ns=None, **kw ):
        """
        Constructor of the class. It does nothing else that invoke its
        parent's contructor with default values.
        """

        listReplicasResponse.__init__(self)
