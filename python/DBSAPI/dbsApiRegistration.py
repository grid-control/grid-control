
import os, re, string, socket, xml.sax, xml.sax.handler
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *

def dbsApiImplRegister(self):
	funcInfo = inspect.getframeinfo(inspect.currentframe())
	####logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))
	data = self._server._call ({ 'api' : 'register'}, 'GET')
	print data

