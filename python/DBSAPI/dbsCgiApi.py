#!/usr/bin/env python
#
# CGI implementation of the DBS API class. This version of API relies
# on CGI scripts providing xml output.  The script can be remove or
# executed locally.

import os, re, string, httplib, urllib, urllib2, gzip, xml.sax, xml.sax.handler
from xml.sax.saxutils import escape
from cStringIO import StringIO
from dbsException import DbsException
from dbsBaseObject import *
from dbsFile import DbsFile
from dbsFileBlock import DbsFileBlock
from dbsEventCollection import DbsEventCollection
from dbsPrimaryDataset import DbsPrimaryDataset
from dbsProcessedDataset import DbsProcessedDataset
from dbsParameterSet import DbsParameterSet
from dbsApplication import DbsApplication
from dbsApplicationConfig import DbsApplicationConfig
from dbsProcessing import DbsProcessing
from dbsParent import DbsParent
from dbsApi import DbsApi, DbsApiException, InvalidDataTier

DEFAULT_URL = "http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/dbsxml"

##############################################################################
# DBS CGI Exceptions

class DbsCgiApiException(DbsException):
  def __init__ (self, **kwargs):
    DbsException.__init__(self, **kwargs)

class DbsCgiNoObject(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

class DbsCgiObjectExists(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

class DbsCgiToolError(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

class DbsCgiBadRequest(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

class DbsCgiBadData(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

class DbsCgiExecutionError(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

class DbsCgiConnectionError(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

class DbsCgiDatabaseError(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

class DbsCgiBadResponse(DbsCgiApiException):
  def __init__ (self, **kwargs):
    DbsCgiApiException.__init__(self, **kwargs)

##############################################################################
class DbsCgiExecHandler (urllib2.BaseHandler):
  """ Handler for exec:/path?args=... URL for CGI script.  """
  def exec_open (self, req):
    path = req.get_selector()
    args = path.split("?", 1)
    if len(args) == 1: args.append('')
    #print "args ", args
    # Prepare CGI-like environment
    os.putenv ('GATEWAY_INTERFACE', 'CGI/1.1')
    os.putenv ('HTTP_ACCEPT_ENCODING', req.headers.get ('Accept-encoding'))
    os.putenv ('HTTP_USER_AGENT', 'DBS-CGI-Direct-call')
    os.putenv ('REQUEST_METHOD', 'POST')
    os.putenv ('CONTENT_LENGTH', str(req.headers.get ('Content-length')))
    os.putenv ('CONTENT_TYPE', req.headers.get ('Content-type'))
    os.putenv ('QUERY_STRING', args[1])
    os.putenv ('REQUEST_URI', path)
    os.putenv ('SCRIPT_NAME', args[0])
    os.putenv ('SERVER_NAME', 'localhost')
    os.putenv ('SERVER_PORT', str(80))
    os.putenv ('SERVER_PROTOCOL', 'HTTP/1.1')
    os.putenv ('SERVER_SOFTWARE', 'Builtin')

    # Open subprocess and write form data
    r, w = os.popen2(args[0])
    r.write (req.get_data())
    r.close ()

    # Read back headers, then leave the body to be read
    msg = httplib.HTTPMessage (w, 0)
    msg.fp = None
    return urllib.addinfourl (w, msg, path)

##############################################################################
# CLI implementation of the DBS API class.

class DbsCgiApi(DbsApi):
  def __init__(self, cgiUrl=None, cgiArgs={}):
    """ Constructor. """
    self._cgiUrl = cgiUrl
    self._cgiArgs = cgiArgs 
    if cgiUrl is None:
      self._cgiUrl = DEFAULT_URL

  def setCgiUrl(self, cgiUrl):
    """ Set cgi script url. """
    self._cgiUrl = cgiUrl

  def setArgs(self, args):
    """ Set cgi script url. """
    self._cgiArgs = args

  def setDebug(self, on=1):
    """ Set low-level debugging. """
    httplib.HTTPConnection.debuglevel = on

  def _encode(self, args):
    """
    Encode form (name, value) elements into multi-part/form-data.
    We don't actually need to know what we are uploading here, so
    just claim it's all text/plain.
    """
    boundary = '----------=_DBS_CGI_BOUNDARY_=-----------'
    (body, crlf) = ('', '\r\n')
    for key, value in args.items():
      body += '--' + boundary + crlf
      body += ('Content-disposition: form-data; name="%s"' % key) + crlf
      body += crlf + value + crlf
    body += '--' + boundary + '--' + crlf + crlf
    return ('multipart/form-data; boundary=' + boundary, body)

  def _marshall(self, args, request):
    """
    Marshalls the arguments to the CGI script as multi-part/form-data,
    not the default application/x-www-form-url-encoded.  This improves
    the transfer of the large inputs and eases command line invocation
    of the CGI script.
    """
    (type, body) = self._encode(args)
    request.add_header ('Content-type', type)
    request.add_header ('Content-length', str(len(body)))
    request.add_data (body)
    #print "body ", body

  def _call (self, args):
    """
    Make a call to the CGI server, either a remote HTTP request (the
    URL is of the form http:*), or invoke the CGI as a local executable
    (the URL is of the form file:*).
    
    For the HTTP case, we build a request object, add the form data
    to as multipart/form-data, then fetch the result.  The output is
    compressed so we decompress it, parse out the DBS special HTTP
    headers for status code and error information, and raise errors
    as exceptions.  If all went well, we return the output to caller.

    The local execution is similar except we pass call details via
    environment variables and form data on standard input to the CGI
    script.  The output isn't compressed.  (FIXME: Not implemented!)
    """
    # First apply forced options
    for k, v in self._cgiArgs.items(): args[k] = v
    # Fetch result from the CGI server
    try:
      #request = urllib2.Request (self._cgiUrl, urllib.urlencode(args))
      request = urllib2.Request (self._cgiUrl)
      request.add_header ('Accept-encoding', 'gzip')
      #print "args ",args
      self._marshall (args, request)
      #print "request ",request
      result = urllib2.build_opener (DbsCgiExecHandler).open (request)
      #print "\n\nresult ->>>>>>>>", result
      #import pdb
      #pdb.set_trace()
      data = result.read()
      #f = open("out.txt", "w")
      #print "\n\ndata ->>>>>>>>", data
      if result.headers.get ('Content-encoding', '') == 'gzip':
        data = gzip.GzipFile (fileobj=StringIO(data)).read ()
      #f.write(data);
      #f.close();

      statusCode = int(result.headers.get ('Dbs-status-code'))
      statusMessage = result.headers.get ('Dbs-status-message')
      statusDetail = result.headers.get ('Dbs-status-detail')
      #print "statusCode ", statusCode
      #print "statusMessage ", statusMessage 
      #print "statusDetail ", statusDetail
      # If there was a server-side error, raise an appropriate exception
      if statusCode != 100:
        exmsg = "Status message: '%s', Status detail: '%s'" % (statusMessage, statusDetail)
	if statusCode == 200: raise DbsCgiBadRequest (args=exmsg)
        elif statusCode == 300: raise DbsCgiBadData (args=exmsg)
        elif statusCode == 301: raise InvalidDataTier (args=exmsg)
        elif statusCode == 302: raise DbsCgiNoObject (args=exmsg)
        elif statusCode == 303: raise DbsCgiObjectExists (args=exmsg)
        elif statusCode == 400: raise DbsCgiExecutionError (args=exmsg)
        elif statusCode == 401: raise DbsCgiConnectionError (args=exmsg)
        elif statusCode == 402: raise DbsCgiDatabaseError (args=exmsg)
        else: raise DbsCgiToolError (args=exmsg)

      # All is ok, return the data
      return data

    except DbsException, ex:
      # One of our own errors, re-raise
      raise ex
    except Exception, ex:
      # URL access failed, raise an exception
      raise DbsCgiToolError (exception=ex)

  # ------------------------------------------------------------
  def _path (self, dataset):
    """
    Determine the dataset path of a dataset.  If the argument is a
    string, it's assumed to be the path and is returned.  If it is a
    a dataset object, we'll ask for it's path.  If that fails, we
    assume the object is a processed datatset and make it's path out
    of the primary dataset, tier and processed dataset name.
    """
    if isinstance(dataset, type("")):
      return dataset

    try:
      if dataset.get('datasetPathName') in ('', None):
         return "/" + dataset.get('primaryDataset').get('datasetName') \
             + "/" + dataset.get('dataTier') + "/" + dataset.get('datasetName')

      return dataset.get('datasetPathName')

    except:
      return "/" + dataset.get('primaryDataset').get('datasetName') \
	     + "/" + dataset.get('dataTier') + "/" + dataset.get('datasetName')

  # ------------------------------------------------------------
  #  dbsApi API Implementation follows
  # ------------------------------------------------------------
  def listPrimaryDatasets(self, pattern="*"):
    """
    Retrieve list of primary datasets matching a shell glob pattern.
    Returns a list of DbsPrimaryDataset objects.  If the pattern is
    given, it will be matched against the dataset name as a shell
    glob pattern.

    May raise an DbsCgiApiException.
    """
    # Invoke cgi script.
    data = self._call ({ 'api' : 'listPrimaryDatasets', 'pattern' : pattern , 'instance' : 'MCLocal/Writer' })
    #data = self._call ({ 'api' : 'listPrimaryDatasets', 'pattern' : pattern  })

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'primary-dataset':
	    result.append(DbsPrimaryDataset (objectId=long(attrs['id']),
					     datasetName=str(attrs['name'])))
      xml.sax.parseString (data, Handler ())
      return result
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def listProcessedDatasets(self, pattern="*"):
    """
    Retrieve list of processed datasets matching a shell glob pattern.
    Returns a list of DbsProcessedDataset objects.  If the pattern is
    given, it will be matched against the dataset path as a shell glob
    pattern.

    May raise an DbsCgiApiException.
    """
    # Invoke cgi script.
    # data = self._call ({ 'api' : 'listProcessedDatasets', 'pattern' : pattern })
    data = self._call ({ 'api' : 'listDatasets', 'pattern' : pattern })
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'processed-dataset':
	    result.append(DbsProcessedDataset (objectId=long(attrs['id']),
	    				       datasetPathName=str(attrs['path'])))
      xml.sax.parseString (data, Handler ())
      return result
    except DbsException, ex:
	raise DbsCgiBadResponse(exception=ex)
    except Exception, ex:
	raise DbsCgiBadResponse(exception=ex)


  # ------------------------------------------------------------
  def listDatasetsFromApp(self, pattern="*"):
    """
    Retrieve list of processed datasets matching a shell glob pattern against 
    application executable or version or family.
    Returns a list of DbsProcessedDataset objects.  If the pattern is
    given, it will be matched against the dataset path as a shell glob
    pattern.

    May raise an DbsCgiApiException.
    """
    # Invoke cgi script.
    # data = self._call ({ 'api' : 'listProcessedDatasets', 'pattern' : pattern })
    data = self._call ({ 'api' : 'listDatasetsFromApp', 'pattern' : pattern })
    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'processed-dataset':
            app = DbsApplication(executable = str(attrs['app']),
			    version =  str(attrs['version']),
                            family = str(attrs['family']) )
	    #print "app" , app
	    result.append(DbsProcessedDataset (objectId=long(attrs['id']),
	    				       datasetPathName=str(attrs['path']),
					       application = app))
      xml.sax.parseString (data, Handler ())
      return result
    except DbsException, ex:
	raise DbsCgiBadResponse(exception=ex)
    except Exception, ex:
	raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def listParameterSets(self, pattern="*"):
    """
    Retrieve list of parameter sets matching a shell glob pattern.
    Returns a list of DbsParameterSet objects.  If the pattern is
    given, it will be matched against the content as a shell glob
    pattern.

    May raise an DbsCgiApiException.
    """
    # Invoke cgi script.
    data = self._call ({ 'api' : 'listParameterSets', 'pattern' : pattern })

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'parameter-set':
	    result.append(DbsParameterSet (objectId=long(attrs['id']),
					   hash=str(attrs['hash']),
					   content=str(attrs['content'])))
      xml.sax.parseString (data, Handler ())
      return result
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def listApplications(self, pattern="*"):
    """
    Retrieve list of applications matching a shell glob pattern.
    Returns a list of DbsApplication objects.  If the pattern is
    given, it will be matched against the application label as
    /family/executable/version as a shell glob pattern.

    May raise an DbsCgiApiException.
    """
    # Invoke cgi script.
    data = self._call ({ 'api' : 'listApplications', 'pattern' : pattern })

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'application':
	    result.append(DbsApplication(objectId=long(attrs['id']),
					 executable=str(attrs['executable']),
					 version=str(attrs['version']),
					 family=str(attrs['family'])))
      xml.sax.parseString (data, Handler ())
      return result
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def listApplicationConfigs(self, pattern="*"):
    """
    Retrieve list of application configurations matching a shell glob
    pattern.  Returns a list of DbsApplicationConfig objects.  If the
    pattern is given, it will be matched against the application label
    as /family/executable/version as a shell glob pattern; all the
    parameter sets used with that application will be returned.

    May raise an DbsCgiApiException.
    """
    # Invoke cgi script.
    data = self._call ({ 'api' : 'listApplicationConfigs', 'pattern' : pattern })

    # Parse the resulting xml output.
    try:
      result = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'application':
	    self.app = DbsApplication(executable=str(attrs['executable']),
			              version=str(attrs['version']),
			              family=str(attrs['family']))
            
	  elif name == 'app-config':
	    pset = DbsParameterSet(objectId=long(attrs['psetid']),
				   hash=str(attrs['hash']),
				   content=str(attrs['content']))
	    result.append(DbsApplicationConfig(objectId=long(attrs['id']),
					       application = self.app,
					       parameterSet = pset))
      xml.sax.parseString (data, Handler ())
      return result
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def getDatasetProvenance(self, dataset, tiers=[]):
    """
    Retrieve the dataset parents of the dataset.  If tiers is an
    empty list, retrieves all parents.  Otherwise returns only the
    data tiers that match.  The result is a list of DbsParents
    with parentage type set and referring to DbsProcessedDatasets
    with path name and data tier filled in.

    The input dataset should be an DbsProcessedDataset with path
    set, or primary dataset, tier and processed dataset name
    filled in.  For backwards compatibility a simple dataset path
    name string is also accepted.

    Raises InvalidDataTier if tiers includes an unknown data tier,
    InvalidDatasetPathName if the dataset path is invalid,
    otherwise may raise an DbsCgiApiException.
    """
    # Check path.
    path = self._path (dataset)
    verifyDatasetPathName(path)

    # Invoke cgi script.
    args = { 'api' : 'getDatasetProvenance', 'path' : path }
    if len(tiers): args['datatier'] = string.join(tiers, ",")
    data = self._call (args)

    # Parse the resulting xml output.
    try:
      parents = []
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if name == 'parent':
	    p = DbsProcessedDataset(datasetPathName=str(attrs['path']),
			            dataTier=str(attrs['tier']))
	    parents.append(DbsParent(parent=p, type=str(attrs['type'])))
      xml.sax.parseString (data, Handler ())
      return parents
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def getDatasetContents(self, dataset):
    """
    Retrieve the event collections of dataset by file block.  Returns
    a list of DbsFileBlock objects, with event collection list filled
    with DbsEventCollection objects.

    The input dataset should be an DbsProcessedDataset with path set,
    or a DbsProcessedDataset with primary dataset, tier and processed
    dataset name filled in.  For backwards compatibility a simple
    dataset path name string is also accepted.

    Raises InvalidDatasetPathName if the path is invalid, otherwise
    may raise an DbsCgiApiException.

    See getDatasetFiles() for a version that returns files.
    """
    # Check path.
    path = self._path(dataset)
    verifyDatasetPathName(path)

    # Invoke cgi script.
    data = self._call ({ 'api' : 'getDatasetContents', 'path' : path })

    # Parse the resulting xml output.  The output consits of a list of blocks,
    # each with its list of event collections.
    try:
      fileBlocks = {}
      class Handler (xml.sax.handler.ContentHandler):
	def __init__ (self):
	  self._block = None
	def startElement(self, name, attrs):
	  if name == 'block':
	    id = attrs['id']
	    if not fileBlocks.has_key (id):
	      fileBlocks[id] = DbsFileBlock(objectId=long(id), blockName=str(attrs['name']))
	    self._block = fileBlocks[id]
          elif name == 'event-collection':
	    f =  DbsFile (     fileBlockId=long(attrs['inblock']),
                                guid=str(attrs['guid']),
                                logicalFileName=str(attrs['lfn']),
                                fileStatus=str(attrs['fstatus']),
                                checkSum=str(attrs['checksum']),
                                fileSize=long(attrs['size']))
	    self._block['eventCollectionList'].append (DbsEventCollection(fileList=[f], 
	      collectionName=str(attrs['name']), numberOfEvents=long(attrs['events'])))

      xml.sax.parseString (data, Handler ())
      return fileBlocks.values ()
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def getDatasetFileBlocks(self, dataset):
    """
    Retrieve the files related to the dataset, organised by file
    block.  Returns a list of DbsFileBlock objects, each with the
    file list filled with DbsFile objects.  Note that the files
    may contain data for other datasets.

    The input dataset should be an DbsProcessedDataset with path set,
    or a DbsProcessedDataset with primary dataset, tier and processed
    dataset name filled in.  For backwards compatibility a simple
    dataset path name string is also accepted.

    Raises InvalidDatasetPathName if the path is invalid, otherwise
    may raise an DbsCgiApiException.

    See getDatasetContents() for a version that returns event
    collections.
    """
    # Check path.
    path = self._path(dataset)
    verifyDatasetPathName(path)

    # Invoke cgi script.
    data = self._call ({ 'api' : 'getDatasetFiles', 'path' : path })

    # Parse the resulting xml output.  The output consits of a list of blocks,
    # each with its list of event collections.
    try:
      blocks = {}
      class Handler (xml.sax.handler.ContentHandler):
	def __init__ (self):
	  self._block = None
	def startElement(self, name, attrs):
	  if name == 'block':
	    id = attrs['id']
	    if not blocks.has_key (id):
	      blocks[id] = DbsFileBlock (objectId=long(id),
			      		 blockName=str(attrs['name']),
					 numberOfFiles=long(attrs['files']),
					 numberOfBytes=long(attrs['bytes']))
	    self._block = blocks[id]
          elif name == 'file':
	    self._block['fileList'].append(DbsFile (objectId=long(attrs['id']),
		    			  fileBlockId=long(attrs['inblock']),
		    			  guid=str(attrs['guid']),
		    			  logicalFileName=str(attrs['lfn']),
		    			  fileStatus=str(attrs['status']),
		    			  checkSum=str(attrs['checksum']),
		    			  fileSize=long(attrs['size'])))

      xml.sax.parseString (data, Handler ())
      return blocks.values ()
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)


  # ------------------------------------------------------------
  def getLFNs(self, blockName , dataset = None):
    # Check path.
    if(dataset) :
      path = self._path(dataset)
      verifyDatasetPathName(path)

    # Invoke cgi script.
    if (dataset) :
      data = self._call ({ 'api' : 'getLFNs', 'path' : path , 'blockName' : blockName})
    else :
      data = self._call ({ 'api' : 'getLFNs',  'blockName' : blockName})
    #print data
    # Parse the resulting xml output.  The output consits of a list of blocks,
    # each with its list of event collections.
    try:
      blocks = {}
      mylist = []
      class Handler (xml.sax.handler.ContentHandler):
	def __init__ (self):
	  self._block = None
	def startElement(self, name, attrs):
          """
	  if name == 'block':
	    if not blocks.has_key (blockId):
	      blocks[blockId] = DbsFileBlock (objectId=long(blockId),
                                              blockName=str(attrs['name']))
            self._block = blocks[blockId]
          """  
          if name == 'file':
            """
	    self._block['fileList'].append(DbsFile (
                                          fileBlockId=long(blockId),
		    			  logicalFileName=str(attrs['lfn']),
		    			  fileStatus=str(attrs['status']),
		    			  fileType=str(attrs['type']),
		    			  fileSize=long(attrs['size'])))
            """
            mylist.append( (  str(attrs['lfn']), long(attrs['size']), str(attrs['status']), str(attrs['type']) ) )


      xml.sax.parseString (data, Handler ())
      #print mylist
      #print blocks
      #print "***************"
      #print blocks.values ()
      #return blocks.values ()
      return mylist
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)


  # ------------------------------------------------------------
  def createPrimaryDataset(self, dataset):
    """
    Create a new primary dataset.  Instantiates a database entity for
    the dataset, and updates input object for the id of the new row.
    The input object should be a DbsPrimaryDataset with the name set.

    Raises DbsCgiObjectExists if a primary dataset already exists in
    the database, otherwise may raise an DbsCgiApiException.
    """
    data = self._call ({ 'api' : 'createPrimaryDataset',
		         'name' : dataset.get('datasetName') , 'instance' : 'MCLocal/Writer'  })
		         #'name' : dataset.get('datasetName') })
    try:
      class Handler (xml.sax.handler.ContentHandler):
	def startElement(self, name, attrs):
	  if (name == 'primary-dataset'):
	    dataset['objectId'] = long(attrs['id'])
      xml.sax.parseString (data, Handler())
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def createProcessing(self, processing):
    """
    Create a new processing.  Instantiates a database entity for the
    processing, and updates input object for the id of the new row.
    The input object should be a DbsProcessing duly filled in, with
    a reference to a primary dataset and application configuration,
    and optionally a parent.  Application-related information is
    automatically instantiated in the database if it doesn't exist.

    Raises DbsCgiObjectExists if a primary dataset already exists in
    the database, DbsCgiNoObject if the primary dataset, or parent
    if one was specified, doesn't exist in the database, otherwise
    may raise an DbsCgiApiException.
    """
    # Prepare XML description of the input
    appc = processing.get('applicationConfig')
    app = appc.get('application')
    pname = ""
    if processing.get('parent', None) is not None:
      pname = processing.get('parent').get('processingName')
    input = "<dbs><processing name='%s' primary='%s' parent='%s'>" % (
      escape (processing.get('processingName')),
      escape (processing.get('primaryDataset').get('datasetName')),
      escape (pname))
    input += "<application executable='%s' version='%s' family='%s'/>" % (
      escape (app.get('executable')), escape (app.get('version')), escape (app.get('family')))
    input += "<parameter-set hash='%s' content='%s'/>" % (
      escape (appc.get('parameterSet').get('hash')), escape (appc.get('parameterSet').get('content')))
    input += "</processing></dbs>"

    # Call the method and fill in object id
    #data = self._call ({ 'api' : 'createProcessing', 'xmlinput' : input })
    data = self._call ({ 'api' : 'createProcessing', 'xmlinput' : input , 'instance' : 'MCLocal/Writer' })
    try:
      class Handler (xml.sax.handler.ContentHandler):
	def startElement (self, name, attrs):
	  if (name == 'processing'):
	    processing['objectId'] = long(attrs['id'])
      xml.sax.parseString (data, Handler())
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def createFileBlock(self, block):
    """
    Create a new file block.  Instantiates a database entity for the
    block, and updates input object for the id of the new row.  The
    input object should be a DbsFileBlock duly filled in, referring
    to a DbsProcessing.  On successful return the block's id and name
    will have been updated; the block will be open.

    Raises DbsCgiNoObject if the processing does not exist in the
    database, otherwise may raise an DbsCgiApiException.
    """
    pname = "/%s/%s" % \
      (block.get('processing').get('primaryDataset').get('datasetName'),
       block.get('processing').get('processingName'))
    if  "blockName" in block.keys():
       data = self._call ({ 'api' : 'createFileBlock', 'processing' : pname , 'blockName' : block['blockName'] })
    else :
       data = self._call ({ 'api' : 'createFileBlock', 'processing' : pname })
    try:
      class Handler (xml.sax.handler.ContentHandler):
	def startElement (self, name, attrs):
	  if (name == 'block'):
	    block['objectId'] = long(attrs['id'])
	    block['blockName'] = str(attrs['name'])
      xml.sax.parseString (data, Handler())
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)
 
  def closeFileBlock(self, block):
    """
    Closes a file block.  
    """
    input = "<dbs><block name='%s'/></dbs>" % block.get('blockName').__str__()
    data = self._call ({ 'api' : 'closeFileBlock', 'xmlinput' : input })
    
  # ------------------------------------------------------------
  def createProcessedDataset(self, dataset):
    """
    Create a new processed dataset.  Instantiates a database entity
    for the dataset, and updates input object for the id of the new
    row.  The input object should be a DbsProcessedDataset filled in,
    referring to a DbsPrimaryDataset and having data tier and dataset
    name set.  On return the dataset's id will be updated.

    Raises DbsCgiObjectExists if the dataset already exists, or
    DbsCgiNoObject if required path components, the primary dataset
    and the processing name created through createProcessing(), do
    not exist in the database; otherwise may raise an
    DbsCgiApiException.
    """
    # Call the method and fill in object id
    data = self._call ({ 'api' : 'createProcessedDataset',
		    'path' : self._path(dataset), 'instance' : 'MCLocal/Writer'})
		    #'path' : self._path(dataset)})
    try:
      class Handler (xml.sax.handler.ContentHandler):
	def startElement (self, name, attrs):
	  if (name == 'processed-dataset'):
	    dataset['objectId'] = long(attrs['id'])
      xml.sax.parseString (data, Handler())
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

  # ------------------------------------------------------------
  def insertFiles(self, block, files):
    """
    Insert files to an existing block.  Instantiates a database row
    for each element of the file list.  The objects are *not* updated
    for database id on return. The block should be a DbsFileBlock.
    The files should be DbsFile objects fully described, including
    name, file size, checksum, type and optionally status.

    Raises DbsCgiObjectExists if any of the files already exists, or
    DbsCgiNoObject if the block does not exist in the database;
    otherwise may raise an DbsCgiApiException.
    """
    # Prepare XML description of the input

    input = "<dbs><block name='%s'>" % escape (block.get('blockName'))
    for f in files:
      input += ("<file lfn='%s' guid='%s' checksum='%s' size='%d'"
		+ " status='%s' type='%s' />") % (
	escape (f.get('logicalFileName')), escape (f.get('guid', '')),
	escape (f.get('checkSum')), f.get('fileSize'),
	escape (f.get('fileStatus', '')), escape (f.get('fileType')))
    input += "</block></dbs>"

    # Call the method.
    data = self._call ({ 'api' : 'insertFiles', 'xmlinput' : input })

  # ------------------------------------------------------------
  def insertEventCollections(self, dataset, eventCollections):
    """
    Insert event collections to a processed dataset.  Instantiates a
    database row for each element of the event collection list.  The
    objects are *not* updated for database id on return.  The dataset
    should be a DbsProcessedDataset.  The event collections should
    be DbsEventCollection objects fully described, including name and
    number of events, event collection parentage referring to other
    DbsEventCollection objects, and the list of files as DbsFile the
    collections are mapped to.  The files must have their logical
    names names set, and are assumed to already exist in the database.

    Raises DbsCgiObjectExists if any event collection already exists,
    or DbsCgiNoObject if the processed dataset, parent collections or
    the files do not exist in the database; otherwise may raise an
    DbsCgiApiException.
    """
    # Prepare XML description of the input
    input = "<dbs><processed-dataset path='%s'>" % escape (self._path(dataset))
    for evc in eventCollections:
      input += "<event-collection name='%s' events='%d' status='%s'>" % (
	escape (evc.get('collectionName')), evc.get('numberOfEvents'),
	escape (evc.get('collectionStatus', '')))
      for p in evc.get('parentageList'):
        input += "<parent name='%s' type='%s'/>" % (
	  escape (p.get('parent').get('collectionName')), escape (p.get('type')))
      for f in evc.get('fileList'):
        input += "<file lfn='%s'/>" % escape (f.get('logicalFileName'))
      input += "</event-collection>"
    input += "</processed-dataset></dbs>"

    # Call the method and parse output to fill in object id into each
    # event collection we passed in
    data = self._call ({ 'api' : 'insertEventCollections', 'xmlinput' : input })

  # ------------------------------------------------------------
  #def remap(self, eventCollections, outEventCollection, dataset):
  def remap(self, files, outFile):

   input = "<dbs>"
   for f in files:
        input += "<file-in lfn='%s'/>" % escape (f.get('logicalFileName'))
   input += "<file lfn='%s'/>" % escape (outFile.get('logicalFileName'))
   input += "</dbs>"
    
   #print "calling _call remap inside dbsCgiApi"
   data = self._call ({ 'api' : 'remap',
		         'xmlinput' : input })

 
  # ------------------------------------------------------------
  def getDatasetInfo(self, dataset, blockName=None):
    """
    Retrieve the complete DBS snapshpot of a processed dataset with 
    event collections , files ,processing and blocks

    The input dataset should be an DbsProcessedDataset with path set,
    or a DbsProcessedDataset with primary dataset, tier and processed
    dataset name filled in.  For backwards compatibility a simple
    dataset path name string is also accepted.

    Raises InvalidDatasetPathName if the path is invalid, otherwise
    may raise an DbsCgiApiException.

    """
    # Check path.
    path = self._path(dataset)
    verifyDatasetPathName(path)

    # Invoke cgi script.
    if(blockName != None) :
	print "blockName ",blockName
	return  self._call ({ 'api' : 'getDatasetInfo', 'path' : path , 'blockName' : blockName})
    else :
    	return  self._call ({ 'api' : 'getDatasetInfo', 'path' : path })
    

  # ------------------------------------------------------------
  def insertDatasetInfo(self, xmlinput):
    # Check path.
    #path = self._path(dataset)
    #verifyDatasetPathName(path)

    # Invoke cgi script.
    #data = self._call ({ 'api' : 'export', 'path' : path })
    data = self._call ({ 'api' : 'insertDatasetInfo', 'xmlinput' : xmlinput })
    return data
 # ------------------------------------------------------------
  
  def setFileStatus(self, name, status=None):
   if (status != None) :
    data = self._call ({ 'api' : 'setFileStatus',
		         'name' : name, 
		         'status' : status 
			 })
   else:
    data = self._call ({ 'api' : 'setFileStatus',
		         'name' : name, 
			 })
	   
    try:
       class Handler (xml.sax.handler.ContentHandler):
         def startElement(self, name, attrs):
		 pass
       xml.sax.parseString (data, Handler())
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)
 
 
  def listBlocks(self, dataset, events=None):
    # Check path.
    path = self._path(dataset)
    verifyDatasetPathName(path)

    # Invoke cgi script.
    if( events == None) :    
      data = self._call ({ 'api' : 'listBlocks', 'path' : path })
    else :
      data = self._call ({ 'api' : 'listBlocks', 'path' : path , 'events' : events})

    #print data

    # Parse the resulting xml output.  The output consits of a list of blocks,
    # each with its list of event collections.
    try:
      blocks = {}
      class Handler (xml.sax.handler.ContentHandler):
	def __init__ (self):
	  self._block = None
	def startElement(self, name, attrs):
	  if name == 'block':
            evts = ''
	    id = attrs['id']
	    name = attrs['name']# CHANGED from id to name
	    if not blocks.has_key (id):
              if(events == None) :  
   	         blocks[name] = DbsFileBlock (objectId=long(id),
                                         blockName=str(attrs['name']),
                                         status=str(attrs['status']),
					 numberOfFiles=long(attrs['files']),
					 numberOfBytes=long(attrs['bytes']))
              else :
                 if "events" in attrs.keys():
                    evts = long(attrs['events'])
                 blocks[name] = [evts, str(attrs['status']), long(attrs['files']), long(attrs['bytes'])]

	    self._block = blocks[name]
      xml.sax.parseString (data, Handler ())
      #return blocks.values ()
      return blocks
    except Exception, ex:
      raise DbsCgiBadResponse(exception=ex)

##############################################################################
# Unit testing: see dbsCgiTest*.py
