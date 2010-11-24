#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
import os
import re
import string
import httplib
import urllib
import urllib2

class DbsExecHandler (urllib2.BaseHandler):

  """ Handler for exec:/path?args=... URL for Server call.  """
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

