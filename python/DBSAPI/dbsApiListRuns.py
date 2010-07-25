
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsRun import DbsRun
from dbsProcessedDataset import DbsProcessedDataset
from dbsPrimaryDataset import DbsPrimaryDataset
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListRuns(self, dataset):
    """
    Retrieve list of runs inside a Processed Dataset.

    returns: list of DbsRun objects.
  
    params:
        dataset: No Default user has to provide a value. 

    examples: 

        List ALL Runs for Dataset Path /test_primary_anzar_001/TestProcessedDS002/SIM

	   Note: Mind that the TIER(s) portion of Path will have NO effect on the selection
		 This is still an open question if we need include that to Run selection as well.


           api.listRuns("/test_primary_anzar_001/TestProcessedDS002/SIM")
        
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
             api.listRuns(proc)


    raise: an DbsApiException.

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    path = get_path(dataset)
    # Invoke Server.
    data = self._server._call ({ 'api' : 'listRuns', 'path' : path }, 'GET')

    # Parse the resulting xml output.
    try:
      result = []

      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'run':
               self.currRun= DbsRun (
                                   RunNumber=getLong(attrs['run_number']),
                                   NumberOfEvents=getLong(attrs['number_of_events']),
                                   NumberOfLumiSections=getLong(attrs['number_of_lumi_sections']),
                                   TotalLuminosity=getLong(attrs['total_luminosity']),
                                   StoreNumber=getLong(attrs['store_number']),
                                   StartOfRun=getLong(attrs['start_of_run']),
                                   EndOfRun=getLong(attrs['end_of_run']),
                                   CreationDate=str(attrs['creation_date']),
                                   CreatedBy=str(attrs['created_by']),
                                   LastModificationDate=str(attrs['last_modification_date']),
                                   LastModifiedBy=str(attrs['last_modified_by']),
                                  )
          if name =='processed_dataset':
               self.currRun['Dataset'].append(DbsProcessedDataset (
                                            Name=str(attrs['processed_datatset_name']),
                                            PrimaryDataset=DbsPrimaryDataset(Name=str(attrs['primary_datatset_name']))
                                            ) )


        def endElement(self, name):
            if name == 'run':
               result.append(self.currRun)
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


