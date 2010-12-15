
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplUpdateProcDSDesc(self, dataset, desc):
    """
    Updates the description of a processed dataset

    dataset: Dataset to be updated
    desc : the description
  
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    data = self._server._call ({ 'api' : 'updateProcDSDesc',
                         'path' : get_path(dataset),
                         'desc' : str(desc),
                         }, 'POST')

