
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

def dbsApiImplSetApiVersion(self):
    """
    Sets DBS Client Api Version
    Reading for __version__ tag

    Note: Config (dbs.config) and Constructor 
      arguments have higher presedence 
    """

    return __version__

