
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplUpdateProcDSStatus(self, dataset, status):
    """
    Updates the Status filed of a Dataset

    dataset: Dataset to be updated
    status: One of the possible status 
  
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    data = self._server._call ({ 'api' : 'updateProcDSStatus',
                         'path' : get_path(dataset),
                         'status' : status,
                         }, 'POST')

# -----------------------------------------------------------
