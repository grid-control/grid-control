
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsDataTier import DbsDataTier
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListTiers(self, dataset):

    """
    Retrieve list of Tiers inside a Processed Dataset.

    returns: list of DbsDataTier objects.

    params:
        dataset: Not Defaulted user need to provide a dataset path.

    examples: 

        List ALL Data Tiers for Dataset Path /test_primary_anzar_001/TestProcessedDS002/SIM 
          Note: Mind that the TIER(s) portion of Path will have NO effect on the selection
                 This is still an open question, this call will return ALL tiers in the Processed Dataset.


           api.listTiers("/test_primary_anzar_001/TestProcessedDS002/SIM")

       Using a Dataset Object 
            primary = DbsPrimaryDataset (Name = "test_primary_anzar_001")
            proc = DbsProcessedDataset (
                            PrimaryDataset=primary,
                            Name="TestProcessedDS002",
                            PhysicsGroup="BPositive",
                            Status="VALID",
                            TierList=['SIM', 'RECO'],
                            AlgoList=[WhateverAlgoObject],
                            )
             api.listTiers(proc)
        
    raise: DbsApiException.

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())

    path = get_path(dataset)
    # Invoke Server.
    data = self._server._call ({ 'api' : 'listTiers', 'path' : path }, 'GET')

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'data_tier':
               result.append(DbsDataTier(
                                          Name=str(attrs['name']),
                                          CreationDate=str(attrs['creation_date']),
                                          CreatedBy=str(attrs['created_by']),
                                          LastModificationDate=str(attrs['last_modification_date']),
                                          LastModifiedBy=str(attrs['last_modified_by']),
                                        )
                            )

      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


