import os, re, string, socket, xml.sax, xml.sax.handler
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplCreateAnalysisDatasetFromLFNs(self, adsxml):
    """
    Creates ADS from XML file
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    data = self._server._call ({ 'api' : 'createAnalysisDatasetFromLFNs',
                         'xmlinput' : adsxml }, 'POST')


