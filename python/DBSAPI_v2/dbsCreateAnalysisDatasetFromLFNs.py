import os, re, string, socket, xml.sax, xml.sax.handler
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *
from dbsUtil import *

def dbsApiImplCreateAnalysisDatasetFromLFNs(self, adsxml):
    """
    Creates ADS from XML file
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    ###logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    ###logging.log(DBSDEBUG, adsxml)

    data = self._server._call ({ 'api' : 'createAnalysisDatasetFromLFNs',
                         'xmlinput' : adsxml }, 'POST')
    ###logging.log(DBSDEBUG, data)


