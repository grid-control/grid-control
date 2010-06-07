
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *
from xml.sax import SAXParseException

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplSetMode(self):
    """
    Retrieves the server parameters, such as Server version, Schema version
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())

    # Invoke Server.
    data = self._server._call ({ 'api' : 'setMode' }, 'POST')

def dbsApiImplUnsetMode(self):
    """
    Retrieves the server parameters, such as Server version, Schema version
    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())

    # Invoke Server.
    data = self._server._call ({ 'api' : 'unsetMode' }, 'POST')

