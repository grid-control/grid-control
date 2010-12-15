
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplVersionDQ(self, version, description=""):
    """
    This API is used to Version the Data Quality (Tag) with the input 'version' provided

    params:
	version: the NAME you would like to give to your version
    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())

    xmlinput  = "<?xml version='1.0' standalone='yes'?>"
    xmlinput += "<dbs>"
    xmlinput += "<dq_version version='"+str(version)+"' description='"+description+"' />"
    xmlinput += "</dbs>"


    data = self._server._call ({ 'api' : 'versionDQ',
                                        'xmlinput': xmlinput,
                                         }, 'GET')


