
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsQueryableParameterSet import DbsQueryableParameterSet
from dbsAlgorithm import DbsAlgorithm
from xml.sax import SAXParseException

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplListAlgorithms(self, patternVer="*", patternFam="*", patternExe="*", patternPS="*"):
    """
    Retrieve list of applications/algorithms matching a shell glob pattern.
    User can base his/her search on patters for Application Version, 
    Application Family, Application Executable Name or Parameter Set.

    returns:  list of DbsApplication objects.  

    params:
        patternVer: glob pattern for Application Version, v00_00_01, *
        patternFam: glob pattern for Application Family, GEN, *
        patternExe: glob pattern for Application Executable Name, CMSSW, writeDigi, *
        patternPS: glob pattern for PSet Name, whatever, *
 
    raise: DbsApiException.
    examples:
           Say I want to list all listAlgorithms that have application version v00_00_03, 
           produced by Application CMSSW, I can make my call as,

                 api.listAlgorithms(patternVer='v00_00_03', patternFam='CMSSW') 

           List ALL Algorithms know to DBS

                 api.listAlgorithms("*")

    """
    funcInfo = inspect.getframeinfo(inspect.currentframe())


    # Invoke Server.
    data = self._server._call ({ 'api' : 'listAlgorithms',
		    'app_version' : patternVer, 
		    'app_family_name' : patternFam, 
		    'app_executable_name' : patternExe, 
		    'ps_hash' : patternPS }, 
		    'GET')
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):

	def startElement(self, name, attrs):
	  if name == 'algorithm':
            result.append(DbsAlgorithm( ExecutableName=str(attrs['app_executable_name']),
                                                         ApplicationVersion=str(attrs['app_version']),
                                                         ApplicationFamily=str(attrs['app_family_name']),
                                                         ParameterSetID=DbsQueryableParameterSet
                                                          (
                                                           Hash=str(attrs['ps_hash']),
                                                           Name=str(attrs['ps_name']),
                                                           Version=str(attrs['ps_version']),
                                                           Type=str(attrs['ps_type']),
                                                           #Annotation=str(attrs['ps_annotation']),
                                                           Annotation=base64.decodestring(str(attrs['ps_annotation'])),
                                                           Content=base64.decodestring(str(attrs['ps_content']))
                                                           ),
                                                         CreationDate=str(attrs['creation_date']),
                                                         CreatedBy=str(attrs['created_by']),
                                                         LastModificationDate=str(attrs['last_modification_date']),
                                                         LastModifiedBy=str(attrs['last_modified_by']),
                                                        ) )
      xml.sax.parseString (data, Handler ())
      return result

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

