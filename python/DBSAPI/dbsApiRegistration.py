
import os, re, string, socket, xml.sax, xml.sax.handler
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplRegister(self):
	funcInfo = inspect.getframeinfo(inspect.currentframe())
	data = self._server._call ({ 'api' : 'register'}, 'GET')
	print data

