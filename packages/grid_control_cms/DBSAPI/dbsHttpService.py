#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
from dbsException import DbsException
from dbsApiException import *
try:
        import httplib

except Exception, ex:
        if ex.__str__() == "'Random' object has no attribute 'getrandbits'":
                exmsg ="\n\nDBS has detected a serious issue with your PYTHONPATH"
                exmsg+="\nTHIS IS A KNOWN ISSUE with using gLite UI and CMSSW"
                exmsg+="\nPlease remove lib-tk, and lib-dynload from PYTHONPATH and try again\n\n"
                raise DbsToolError(args=exmsg, code="9999")

from dbsExecHandler import DbsExecHandler
import os, re, string, urllib, urllib2, gzip, time, socket

import os, re, string, xml.sax, xml.sax.handler
from xml.sax.saxutils import escape, unescape
from xml.sax import SAXParseException
try:
  from socket import ssl, sslerror, error
except:
  print "Unable to import socket modules, \nStatement failed: \
	\n      from socket import ssl, sslerror, error \
	\n May not be able to support HTTPS \
	\n continuing.."
  pass

import urlparse

http_responses = {
    100: ('Continue', 'Request received, please continue'),
    101: ('Switching Protocols',
          'Switching to new protocol; obey Upgrade header'),
    200: ('OK', 'Request fulfilled, document follows'),
    201: ('Created', 'Document created, URL follows'),
    202: ('Accepted',
          'Request accepted, processing continues off-line'),
    203: ('Non-Authoritative Information', 'Request fulfilled from cache'),
    204: ('No Content', 'Request fulfilled, nothing follows'),
    205: ('Reset Content', 'Clear input form for further input.'),
    206: ('Partial Content', 'Partial content follows.'),
    300: ('Multiple Choices',
          'Object has several resources -- see URI list'),
    301: ('Moved Permanently', 'Object moved permanently -- see URI list'),
    302: ('Found', 'Object moved temporarily -- see URI list'),
    303: ('See Other', 'Object moved -- see Method and URL list'),
    304: ('Not Modified',
          'Document has not changed since given time'),
    305: ('Use Proxy',
          'You must use proxy specified in Location to access this '
          'resource.'),
    307: ('Temporary Redirect',
          'Object moved temporarily -- see URI list'),
    400: ('Bad Request',
          'Bad request syntax or unsupported method'),
    401: ('Unauthorized',
          'No permission -- see authorization schemes'),
    402: ('Payment Required',
          'No payment -- see charging schemes'),
    403: ('Forbidden',
          'Request forbidden -- authorization will not help'),
    404: ('Not Found', 'Nothing matches the given URI'),
    405: ('Method Not Allowed',
          'Specified method is invalid for this server.'),
    406: ('Not Acceptable', 'URI not available in preferred format.'),
    407: ('Proxy Authentication Required', 'You must authenticate with '
          'this proxy before proceeding.'),
    408: ('Request Timeout', 'Request timed out; try again later.'),
    409: ('Conflict', 'Request conflict.'),
    410: ('Gone',
          'URI no longer exists and has been permanently removed.'),
    411: ('Length Required', 'Client must specify Content-Length.'),
    412: ('Precondition Failed', 'Precondition in headers is false.'),
    413: ('Request Entity Too Large', 'Entity is too large.'),
    414: ('Request-URI Too Long', 'URI is too long.'),
    415: ('Unsupported Media Type', 'Entity body in unsupported format.'),
    416: ('Requested Range Not Satisfiable',
          'Cannot satisfy request range.'),
    417: ('Expectation Failed',
          'Expect condition could not be satisfied.'),
    500: ('Internal Server Error', 'Server got itself in trouble'),
    501: ('Not Implemented',
          'Server does not support this operation'),
    502: ('Bad Gateway', 'Invalid responses from another server/proxy.'),
    503: ('Service Unavailable',
          'The server cannot process the request due to a high load'),
    504: ('Gateway Timeout',
          'The gateway server did not receive a timely response'),
    505: ('HTTP Version Not Supported', 'Cannot fulfill request.'),
}

class DbsHttpService:

  """Provides Server connectivity through HTTP"""

  def __init__(self, Url, ApiVersion, Args={}):

	    self.ApiVersion = ApiVersion
            self.Url = Url

            spliturl = urlparse.urlparse(Url)
            callType = spliturl[0]
            if callType not in ['http', 'https']:
                raise DbsConfigurationError(args="HttpError, BAD URL: %s" %Url, code="200")
            hostport=urllib2.splitport(spliturl[1])
            self.Host=hostport[0]
            self.Port=hostport[1]
            self.ipList = [self.Host]
            try :
       	        self.ipList = socket.gethostbyname_ex(self.Host)[2]
            except:
                raise DbsConnectionError(args="Could not locate the host " + self.Host, code=400)

            if self.Port in [None, ""]:
                self.Port = "80"
            self.Servlet=spliturl[2]
            if self.Servlet in ['None', '']:
                raise DbsConfigurationError(args="HttpError, BAD URL: %s  Missing Servlet Path" %Url, code="200")
            if callType == 'https':
               ##Make a secure connection       
               self.Secure = True
            else:
               self.Secure = False

	    self.UserID = Args['userID']
	    self.retry_att = 0
            if Args.has_key('retry'):
               self.retry = int(Args['retry'])
            else:
               self.retry = None

  def setDebug(self, on=1):
    """ Set low-level debugging. """
    httplib.HTTPConnection.debuglevel = on

  def getKeyCert(self):
   """
   Gets the User Proxy if it exists otherwise THROWs an exception
   """

   # First presendence to HOST Certificate, RARE
   if os.environ.has_key('X509_HOST_CERT'):
        proxy = os.environ['X509_HOST_CERT']
        key = os.environ['X509_HOST_KEY']
   
   # Second preference to User Proxy, very common
   elif os.environ.has_key('X509_USER_PROXY'):
	proxy = os.environ['X509_USER_PROXY']
	key = proxy

   # Third preference to User Cert/Proxy combinition
   elif os.environ.has_key('X509_USER_CERT'):
   	proxy = os.environ['X509_USER_CERT']
   	key = os.environ['X509_USER_KEY']

   # Worst case, look for proxy at default location /tmp/x509up_u$uid
   else :
	uid = os.getuid()
   	proxy = '/tmp/x509up_u'+str(uid)
	key = proxy

   #Set but not found
   if not os.path.exists(proxy) or not os.path.exists(key):
	raise DbsProxyNotFound(args="Required Proxy for Secure Call \n("+ \
					self.Url+") not found for user '%s'" % os.environ.get("LOGNAME", None), code="9999")

   # All looks OK, still doesn't gurantee proxy's validity etc.
   return key, proxy
   
  def _call(self, args, typ, repeat = 0, delay = 2 ):
	  if self.retry and not self.retry_att:
            self.retry_att=1
            repeat=self.retry
	  try:
		  ret = self._callOriginal(args, typ)
                  return ret
	  except DbsConnectionError ,  ex:
		  ret = self.callAgain(args, typ, repeat, delay)
		  if ret in ("EXP"):
			exmsg ="Failed to connect in %s retry attempt(s)\n" % self.retry
			exmsg+=str(ex)
			raise DbsConnectionError(args=exmsg, code=5999)
		  else:
		  	return ret 
	  except DbsDatabaseError, ex:
                  ret = self.callAgain(args, typ, repeat, delay)
                  if ret in ("EXP"):
                        exmsg ="Failed to connect in %s retry attempt(s)\n" % self.retry
                        exmsg+=str(ex)
                        raise DbsConnectionError(args=exmsg, code=5999)
                  else:
                        return ret
		  
  def callAgain(self, args, typ, repeat, delay):

	  if(repeat!=0):
		  print "I will retry in %s seconds" % delay
		  self.Host = self.ipList[repeat % len(self.ipList)]
		  repeat -= 1
		  time.sleep(delay)
		  delay += 1
		  ret = self._call(args, typ, repeat, delay*10)
		  return ret
	  else:
		return "EXP"

  
  def _callOriginal (self, args, typ):
    """
    Make a call to the server, either a remote HTTP request (the
    URL is of the form http:*), or invoke as a local executable
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

    try:
       request_string = self.Servlet+'?apiversion='+self.ApiVersion
       #print "self.Host ", self.Host

       for key, value in args.items():
          if key == 'api':
             request_string += '&'+key+'='+value
          else: 
             if typ != 'POST':
               request_string += '&'+key+'='+urllib.quote(value) 
             continue 
       if self.Secure != True :
		self.conto = "\n\nhttp://" + self.Host + ":" + self.Port  + request_string + "\n\n" 	
       		self.conn = httplib.HTTPConnection(self.Host, int(self.Port))
       else:
       		self.conto = "\n\nhttps://" + self.Host + ":" + self.Port  + request_string + "\n\n"
		key, cert = self.getKeyCert()
		self.conn = httplib.HTTPSConnection(self.Host, int(self.Port), key, cert)

       params = urllib.urlencode(args)

       headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", 
							"UserID": self.UserID} 

       if typ == 'POST':
          result = self.conn.request(typ, request_string, params, headers)  
       else:
          result = self.conn.request(typ, request_string, headers=headers)

       response = self.conn.getresponse() 

       # See if HTTP call succeeded 
       responseCode = int(response.status)

       if responseCode != 200:

         exmsg = "\nCall to DBS Server ("+self.Url+") failed"
         statusResponse = response.read()
        
         try:
           statusDetail = statusResponse.split('<body>')[1].split('</body>')[0].split('description')[1]  
	   statusDetail = statusDetail.split('<u>')[1].split('</u>')[0]
         except:
           statusDetail = http_responses[responseCode]
         exmsg += "\nHTTP ERROR Status '%s', \nStatus detail: '%s'" % (responseCode, statusDetail)

         raise DbsConnectionError(args=exmsg, code=responseCode)

 
       # HTTP Call was presumly successful, and went throught to DBS Server 
       data = response.read()

    except sslerror, ex:
	msg  = "HTTPS Error, Unable to make API call"
	msg += "\nUnable to connect %s (Please verify!)" % self.Url
	msg += "\nMost probably your Proxy is not valid"
        raise DbsConnectionError (args=msg, code="505")   

    except error, ex:
	msg  = "HTTP(/S) Error, Unable to make API call"
        msg += "\nUnable to connect to %s (Please verify!)" % self.Url
        msg += "\nMost probably url (host, port) specified is incorrect, or using http instead of https"
        msg += "\nError Message: %s" % ex
        raise DbsConnectionError (args=msg, code="505")   
  
    except (urllib2.URLError, httplib.HTTPException), ex:
        msg = "HTTP ERROR, Unable to make API call"
        msg += "  \nVerify URL %s" % self.Url
        msg += "  \nError Message: %s" % ex
        raise DbsConnectionError (args=msg, code="505")   

    except Exception, ex:
        if (isinstance(ex,DbsApiException)):
		raise ex
	msg = "HTTP ERROR, Unable to make API call"
	if str(ex) == "(-2, 'Name or service not known')":
		msg += "\n   Error Message: %s, Verify URL %s" % (ex.args[1], self.Url)
	else: msg += "\n     Error Message: %s" %ex
	raise DbsExecutionError (args=msg, code="505")            

    try:

      data = data.replace("&apos;","")
      #data = unescape(data)
      qTrace = data[data.find('<stack_trace>') + 13 : data.find('</stack_trace>')]
      #print qTrace
      # DbsExecutionError message would arrive in XML, if any
      class Handler (xml.sax.handler.ContentHandler):
           def startElement(self, name, attrs):
             if name == 'exception':
                statusCode = attrs['code']
		statusCode_i = int(statusCode)
		
	        exmsg = unescape("DBS Server Raised An Error: " + attrs['message'] + "," +  attrs['detail'])
		exmsg = exmsg.replace("____________", "\n");
		exmsg = exmsg.replace("QUERY    ", "\n");
		exmsg = exmsg.replace("POSITION ", "\n");
		exmsg += "\n" + qTrace + "\n"
		
		if statusCode_i == 1018:
		    raise DbsBadRequest (args=exmsg, code=statusCode)

		elif statusCode_i in [1080, 3003, 2001, 4001]:
		    raise DbsToolError(args=exmsg, code=statusCode)

                if statusCode_i < 2000 and  statusCode_i > 1000 :
                   #print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ status code %s" %statusCode_i 
                   raise DbsBadRequest (args=exmsg, code=statusCode)
                   #raise Exception (exmsg, statusCode)

                if statusCode_i < 3000 and  statusCode_i >= 2000 :
                   raise DbsDatabaseError (args=exmsg, code=statusCode) 
             
                if statusCode_i < 4000 and  statusCode_i > 3000 :
                   raise DbsBadXMLData (args=exmsg, code=statusCode)

                else: raise DbsExecutionError (args=exmsg, code=statusCode)

             if name == 'warning':
                warn  = "\n DBS Raised a warning message"
                warn += "\n Waring Message: " + attrs['message']
                warn += "\n Warning Detail: " + attrs['detail']+"\n"

	     if name =='info':
                info = "\n DBS Info Message: %s " %attrs['message']
		info += "\n Detail: %s " %attrs['detail']+"\n"
		
#      print data
      tokenToSearch = 'took too long to execute'
      startIndex = data.find(tokenToSearch)
      
      if startIndex != -1:
	      msgToRaise = data[startIndex:]
	      raise DbsExecutionError(args=msgToRaise, code=4000)
      xml.sax.parseString (data, Handler ())
      # All is ok, return the data
      return data

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server not responding as desired %s" % self.Url
      raise DbsConnectionError (args=msg, code="505")


