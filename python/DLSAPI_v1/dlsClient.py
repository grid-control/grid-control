#
# $Id: dlsClient.py,v 1.7 2006/04/28 08:32:16 delgadop Exp $
#
# DLS Client. $Name: DLS_0_1_1 $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
 This module defines a function to retrieve a usable DLS client interface.
 
 Any DLS API implementation must provide the functionality defined by the
 methods of the dlsApi.DlsApi class, or at lest a subset of those (such
 circumstance must be clearly stated in the documentation). Each of the
 implementations will work with a given DLS back-end, and probably not
 with the others. The purpose of this module is to ease the election of
 the concrete API implementation for the client application. This election
 is automated by taking into account environment settings.
 
 The getDlsApi function, defined here, will return a usable API object.
 Refer to the documentation of the function for details.
"""
 

#########################################
# Imports 
#########################################
import dlsApi
from os import environ


#########################################
# Module globals
#########################################
# DlsLfcApi (complete API with LFC back-end)
DLS_TYPE_LFC = "DLS_TYPE_LFC"  

# DlsDliClient (getLocations only API with LFC back-end)
DLS_TYPE_DLI = "DLS_TYPE_DLI" 

# DlsMySQLApi (complete API with MySQL proto back-end)
DLS_TYPE_MYSQL = "DLS_TYPE_MYSQL"

#########################################
# getDlsApi function 
#########################################

def getDlsApi(dls_type = None, dls_endpoint = None, verbosity = dlsApi.DLS_VERB_WARN):
  """
  Returns a usable DLS API object, implementing (some of) the methods defined
  in the dlsApi.DlsApi class.

  The election of which concrete implementation is chosen depends on (in this
  order):
    - The specified dls_type argument
    - The DLS_TYPE environmental variable
    - DLS catalog type advertised in the Information System (if implemented)

  If specified, the dls_type argument (or the contents of the DlsType variable)
  should be one of the supported values (defined in this module).
  
  Currently admitted values are:    
   - DLS_TYPE_LFC  =>  DlsLfcApi class (complete API with LFC back-end)
   - DLS_TYPE_DLI  =>  DlsDliClient class (getLocations only API with LFC back-end)
   - DLS_TYPE_MYSQL =>  DlsMySQLApi  class (complete API with MySQL proto back-end)

  The other arguments (dls_endpoint and verbosity) are passed to the constructor 
  of the DLS API as they are. See the dlsApi.DlsApi documentation for details.
      
  @exception dlsApi.ValueError: if the specified value is not one of the admitted ones
  @exception SetupError (from the implementation class): on errors instantiating the interface

  @param dls_type: the type of API that should be retrieved, see supported values
  @param dls_endpoint: the DLS server, as a string "hname[:port][/path/to/DLS]"
  @param verbosity: value for the verbosity level, from the supported values
      
  @return: a DLS API implementation object
  """

  admitted_vals = [DLS_TYPE_LFC, DLS_TYPE_DLI, DLS_TYPE_MYSQL]
  candidate = None
 
  # First set the candidate from the arguments or environment
  if(dls_type):
    candidate = dls_type
  else:
    candidate = environ.get("DLS_TYPE")

  # Check value is supported
  if(candidate not in admitted_vals):
    msg = "Specified DLS type (%s) is not one of the admitted values: %s" %(candidate,admitted_vals)
    raise dlsApi.ValueError(msg)

  # If everything ok, return corresponding API
  if(candidate == DLS_TYPE_LFC):
     from dlsLfcApi import DlsLfcApi as api
  if(candidate == DLS_TYPE_DLI):
     from dlsDliClient import DlsDliClient as api
  if(candidate == DLS_TYPE_MYSQL):
     from dlsMySQLApi import DlsMySQLApi as api
                                                                                                 

  return api(dls_endpoint, verbosity)
