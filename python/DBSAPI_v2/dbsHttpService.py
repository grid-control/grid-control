#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
import os, re, string, httplib, urllib, urllib2, gzip
from dbsException import DbsException
from dbsApiException import *
from dbsExecHandler import DbsExecHandler

import os, re, string, xml.sax, xml.sax.handler
from xml.sax.saxutils import escape
from xml.sax import SAXParseException
try:
  from socket import ssl, sslerror, error
except:
  print "Unable to support HTTPS"
  pass

import urlparse

import logging
from dbsLogger import *

class DbsHttpService:

  """Provides Server connectivity through HTTP"""

  def __init__(self, Url, ApiVersion, Args={}):

	    self.ApiVersion = ApiVersion
            self.Url = Url

            spliturl = urlparse.urlparse(Url)
            callType = spliturl[0]
            if callType not in ['http', 'https']:
                raise DbsConnectionError (args="HttpError, BAD URL: %s" %Url, code="200")
            hostport=urllib2.splitport(spliturl[1])
            self.Host=hostport[0]
            self.Port=hostport[1]
            if self.Port in [None, ""]:
                self.Port = "80"
            self.Servlet=spliturl[2]
            if self.Servlet in ['None', '']:
                raise DbsConnectionError (args="HttpError, BAD URL: %s  Missing Servlet Path" %Url, code="200")
            if callType == 'https':
               ##Make a secure connection       
               self.Secure = True
            else:
               self.Secure = False

  def setDebug(self, on=1):
    """ Set low-level debugging. """
    httplib.HTTPConnection.debuglevel = on

  def getKeyCert(self):
   """
   Gets the User Proxy if it exists otherwise THROWs an exception
   """

   if os.environ.has_key('X509_USER_PROXY'):
	proxy = os.environ['X509_USER_PROXY']
   else :
	uid = os.getuid()
   	proxy = '/tmp/x509up_u'+str(uid)
   if not os.path.exists(proxy):
	raise DbsProxyNotFound(args="Required Proxy for Secure Call \n("+self.Url+") not found for user %s" %os.getlogin(), code="9999")
   return proxy, proxy
   
  def _call (self, args, typ):
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

       logging.log(DBSINFO, self.conto)	
 
       params = urllib.urlencode(args)
       headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"} 


       if typ == 'POST':
          result = self.conn.request(typ, request_string, params, headers)  
       else:
          result = self.conn.request(typ, request_string )

       logging.log(DBSINFO, request_string)

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
           statusDetail=statusResponse

         exmsg += "\nHTTP ERROR Status '%s', \nStatus detail: '%s'" % (responseCode, statusDetail)
         if responseCode == 300: raise DbsBadData (args=exmsg, code=responseCode)
         elif responseCode == 302: raise DbsNoObject (args=exmsg, code=responseCode)
         elif responseCode == 400: raise DbsExecutionError (args=exmsg, code=responseCode)
         elif responseCode == 401 or responseCode == 404: 
		raise DbsConnectionError (args=exmsg, code=responseCode)
         elif responseCode == 403: 
			#statusDetail = response.read()
			#if statusDetail.find("DN provided not found in grid-mapfile") != -1:
			#	exmsg += "\nDN provided not found in grid-mapfile"	
			raise DbsConnectionError (args=exmsg, code=responseCode)
         else: 
		raise DbsToolError (args=exmsg, code=responseCode)
 
       # HTTP Call was presumly successful, and went throught to DBS Server 
       data = response.read()
       logging.log(DBSDEBUG, data)

    except sslerror, ex:
	msg  = "HTTPS Error, Unable to make API call"
	msg += "\nUnable to connect %s (Please verify!)" % self.Url
	msg += "\nMost probably your Proxy is not valid"
        raise DbsConnectionError (args=msg, code="505")   

    except error, ex:
	msg  = "HTTP(S) Error, Unable to make API call"
        msg += "\nUnable to connect %s (Please verify!)" % self.Url
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
	msg += "\n         Verify URL %s" % self.Url
	if str(ex) == "(-2, 'Name or service not known')":
		msg += "\n   Error Message: %s" %ex.args[1]
	else: msg += "\n     Error Message: %s" %ex
	raise DbsConnectionError (args=msg, code="505")            

    try:
      # Error message would arrive in XML, if any
      class Handler (xml.sax.handler.ContentHandler):
           def startElement(self, name, attrs):
             if name == 'exception':
                statusCode = attrs['code']
                exmsg = "DBS Server Raised An Error: %s, %s" \
                                 %(attrs['message'], attrs['detail'])
                
                if (int(statusCode) < 2000 and  int(statusCode) > 1000 ): 
                   raise DbsBadRequest (args=exmsg, code=statusCode)

                if (int(statusCode) < 3000 and  int(statusCode) > 2000 ):
                   raise DbsDatabaseError (args=exmsg, code=statusCode) 
             
                if (int(statusCode) < 4000 and  int(statusCode) > 3000 ):
                   raise DbsBadXMLData (args=exmsg, code=statusCode)

                else: raise DbsExecutionError (args=exmsg, code=statusCode)

             if name == 'warning':
                warn  = "\n DBS Raised a warning message"
                warn += "\n Waring Message: " + attrs['message']
                warn += "\n Warning Detail: " + attrs['detail']+"\n"
                logging.log(DBSWARNING, warn)


	     if name =='info':
                info = "\n DBS Info Message: %s " %attrs['message']
		info += "\n Detail: %s " %attrs['detail']+"\n"
                logging.log(DBSINFO, info)

      xml.sax.parseString (data, Handler ())
      # All is ok, return the data
      return data

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n   Verify URL %s" % self.Url
      raise DbsBadXMLData (args=msg, code="5999")	

    #except Exception, ex:
    #    msg = "HTTP ERROR, Unable to make API call"
    #    msg += "\n   Verify URL %s" % self.Url
    #    if self.Secure == True : 
    #		msg += "\n   Make sure you have a Valid Proxy"
    #    raise DbsConnectionError (args=msg, code="505")


