from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplUpdateFileMetaData(self, lfn, metaData):
    """
    Updates the QueryableMetadata field of a File (lfn)

    lfn: Logical File Name of file that needs to be updated
    metaData: The value for QueryableMetadata. Cannot be null
  
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    data = self._server._call ({ 'api' : 'updateFileMetaData',
                         'lfn' : file_name(lfn),
                         'queryable_meta_data' : metaData,
                         }, 'POST')

# ------------------------------------------------------------
