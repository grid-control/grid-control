#! /usr/bin/env python
# $Header: /cvsroot/pywebsvcs/zsi/ZSI/dispatch.py,v 1.20 2005/02/11 16:17:29 rsalz Exp $
'''Simple CGI dispatching.
'''

import types, os, sys, cStringIO as StringIO
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from ZSI import *
from ZSI import _child_elements, _copyright, _seqtypes, resolvers
from ZSI.auth import _auth_tc, AUTH, ClientBinding


# Client binding information is stored in a global. We provide an accessor
# in case later on it's not.
_client_binding = None

def GetClientBinding():
    '''Return the client binding object.
    '''
    return _client_binding

def _Dispatch(ps, modules, SendResponse, SendFault, docstyle=0,
              nsdict={}, typesmodule=None, rpc=None, **kw):
    '''Find a handler for the SOAP request in ps; search modules.
    Call SendResponse or SendFault to send the reply back, appropriately.
    '''
    global _client_binding
    try:
        what = ps.body_root.localName

        # See what modules have the element name.
        if modules is None:
            modules = ( sys.modules['__main__'], )

        handlers = [ getattr(m, what) for m in modules if hasattr(m, what) ]
        if len(handlers) == 0:
            raise TypeError("Unknown method " + what)

        # Of those modules, see who's callable.
        handlers = [ h for h in handlers if callable(h) ]
        if len(handlers) == 0:
            raise TypeError("Unimplemented method " + what)
        if len(handlers) > 1:
            raise TypeError("Multiple implementations found: " + `handlers`)
        handler = handlers[0]

        _client_binding = ClientBinding(ps)
        if docstyle:
            result = handler(ps.body_root)
            tc = TC.XML(aslist=1, pname=what + 'Response')
        else:
            data = _child_elements(ps.body_root)
            if len(data) == 0:
                arg = []
            else:
                try:
                    try:
                        type = data[0].localName
                        tc = getattr(typesmodule, type).typecode
                    except Exception, e:
                        tc = TC.Any()
                    arg = [ tc.parse(e, ps) for e in data ]
                except EvaluateException, e:
                    SendFault(FaultFromZSIException(e), **kw)
                    return
            result = handler(*arg)
            if hasattr(result, 'typecode'):
                tc = result.typecode
            else:
                tc = TC.Any(aslist=1, pname=what + 'Response')
                result = [ result ]
        reply = StringIO.StringIO()
        SoapWriter(reply, nsdict=nsdict).serialize(result, tc, rpc=rpc)
        return SendResponse(reply.getvalue(), **kw)
    except Exception, e:
        # Something went wrong, send a fault.
        return SendFault(FaultFromException(e, 0, sys.exc_info()[2]), **kw)


def _ModPythonSendXML(text, code=200, **kw):
    req = kw['request']
    req.content_type = 'text/xml'
    req.content_length = len(text)
    req.send_http_header()
    req.write(text)


def _ModPythonSendFault(f, **kw):
    _ModPythonSendXML(f.AsSOAP(), 500, **kw)

def _JonPySendFault(f, **kw):
    _JonPySendXML(f.AsSOAP(), 500, **kw)

def _JonPySendXML(text, code=200, **kw):
    req = kw['request']
    req.set_header("Content-Type", 'text/xml; charset="utf-8"')
    req.set_header("Content-Length", str(len(text)))
    req.write(text)

def _CGISendXML(text, code=200, **kw):
    print 'Status: %d' % code
    print 'Content-Type: text/xml; charset="utf-8"'
    print 'Content-Length: %d' % len(text)
    print ''
    print text

def _CGISendFault(f, **kw):
    _CGISendXML(f.AsSOAP(), 500, **kw)

def AsCGI(nsdict={}, typesmodule=None, rpc=None, modules=None):
    '''Dispatch within a CGI script.
    '''
    if os.environ.get('REQUEST_METHOD') != 'POST':
        _CGISendFault(Fault(Fault.Client, 'Must use POST'))
        return
    ct = os.environ['CONTENT_TYPE']
    try:
        if ct.startswith('multipart/'):
            cid = resolvers.MIMEResolver(ct, sys.stdin)
            xml = cid.GetSOAPPart()
            ps = ParsedSoap(xml, resolver=cid.Resolve)
        else:
            length = int(os.environ['CONTENT_LENGTH'])
            ps = ParsedSoap(sys.stdin.read(length))
    except ParseException, e:
        _CGISendFault(FaultFromZSIException(e))
        return
    _Dispatch(ps, modules, _CGISendXML, _CGISendFault, nsdict=nsdict,
              typesmodule=typesmodule, rpc=rpc)


class SOAPRequestHandler(BaseHTTPRequestHandler):
    '''SOAP handler.
    '''
    server_version = 'ZSI/1.1 ' + BaseHTTPRequestHandler.server_version

    def send_xml(self, text, code=200):
        '''Send some XML.
        '''
        self.send_response(code)
        self.send_header('Content-type', 'text/xml; charset="utf-8"')
        self.send_header('Content-Length', str(len(text)))
        self.end_headers()
        self.wfile.write(text)
        self.wfile.flush()

    def send_fault(self, f, code=500):
        '''Send a fault.
        '''
        self.send_xml(f.AsSOAP(), code)

    def do_POST(self):
        '''The POST command.
        '''
        try:
            ct = self.headers['content-type']
            if ct.startswith('multipart/'):
                cid = resolvers.MIMEResolver(ct, self.rfile)
                xml = cid.GetSOAPPart()
                ps = ParsedSoap(xml, resolver=cid.Resolve)
            else:
                length = int(self.headers['content-length'])
                ps = ParsedSoap(self.rfile.read(length))
        except ParseException, e:
            self.send_fault(FaultFromZSIException(e))
            return
        except Exception, e:
            # Faulted while processing; assume it's in the header.
            self.send_fault(FaultFromException(e, 1, sys.exc_info()[2]))
            return

        _Dispatch(ps, self.server.modules, self.send_xml, self.send_fault,
                  docstyle=self.server.docstyle, nsdict=self.server.nsdict,
                  typesmodule=self.server.typesmodule, rpc=self.server.rpc)

def AsServer(port=80, modules=None, docstyle=0, nsdict={}, typesmodule=None,
             rpc=None, **kw):
    address = ('', port)
    httpd = HTTPServer(address, SOAPRequestHandler)
    httpd.modules = modules
    httpd.docstyle = docstyle
    httpd.nsdict = nsdict
    httpd.typesmodule = typesmodule
    httpd.rpc = rpc
    httpd.serve_forever()

def AsHandler(request=None, modules=None, nsdict={}, rpc=None, **kw):
    '''Dispatch from within ModPython.'''
    ps = ParsedSoap(request)
    kw['request'] = request
    _Dispatch(ps, modules, _ModPythonSendXML, _ModPythonSendFault,
              nsdict=nsdict, rpc=rpc, **kw)

def AsJonPy(nsdict={}, typesmodule=None, rpc=None, modules=None, request=None, **kw):
    '''Dispatch within a jonpy CGI/FastCGI script.
    '''

    kw['request'] = request
    if request.environ.get('REQUEST_METHOD') != 'POST':
        _JonPySendFault(Fault(Fault.Client, 'Must use POST'), **kw)
        return
    ct = request.environ['CONTENT_TYPE']
    try:
        if ct.startswith('multipart/'):
            cid = resolvers.MIMEResolver(ct, request.stdin)
            xml = cid.GetSOAPPart()
            ps = ParsedSoap(xml, resolver=cid.Resolve)
        else:
            length = int(request.environ['CONTENT_LENGTH'])
            ps = ParsedSoap(request.stdin.read(length))
    except ParseException, e:
        _JonPySendFault(FaultFromZSIException(e), **kw)
        return
    _Dispatch(ps, modules, _JonPySendXML, _JonPySendFault, nsdict=nsdict,
              typesmodule=typesmodule, rpc=rpc, **kw)


if __name__ == '__main__': print _copyright
