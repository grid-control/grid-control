from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplUpdateFileMetaData(self, lfn, metaData):
    """
    Updates the QueryableMetadata field of a File (lfn)

    lfn: Logical File Name of file that needs to be updated
    metaData: The value for QueryableMetadata. Cannot be null
  
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    #logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    data = self._server._call ({ 'api' : 'updateFileMetaData',
                         'lfn' : file_name(lfn),
                         'queryable_meta_data' : metaData,
                         }, 'POST')

# ------------------------------------------------------------
