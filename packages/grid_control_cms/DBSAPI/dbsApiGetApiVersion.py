# Revision: $"
# Id: $"

import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplGetApiVersion(self):
    """
    Returns the API version of the API
    """
    return self.version()

