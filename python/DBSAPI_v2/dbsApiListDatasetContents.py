
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplListDatasetContents(self, path, block_name):
    """
    Dumps contents of a block in dataset in XML format.
    This API call is used for insertDatasetContents, which actually use this XML dump and
    repopulates (another) instance of DBS with same dataset 

    params: 
        path : Not Defaulted. Its the dataset path for which API is being invoked (can be provided as dataset object).
        block_name : Name of the Block thats being dumped.

    examples:
        Dump the contents of Block /this/block#1230-87698 for Dataset /PrimaryDS_01/procds-01/SIM
                  api.listDatasetContents("/PrimaryDS_01/procds-01/SIM", "/this/block#1230-87698") 
        
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
             api.listDatasetContents(proc, "/this/block#1230-87698")
 
    raisei: DbsApiException.

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ##logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    # Invoke Server.
    path = get_path(path)
    data = self._server._call ({ 'api' : 'listDatasetContents', 'path' : path, 'block_name' : block_name }, 'GET')
    ##logging.log(DBSDEBUG, data)

    return data


