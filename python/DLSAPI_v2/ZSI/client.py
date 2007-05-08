#! /usr/bin/env python
# $Header: /cvsroot/pywebsvcs/zsi/ZSI/client.py,v 1.35 2004/09/22 17:47:52 rsalz Exp $
#
# Copyright (c) 2001 Zolera Systems.  All rights reserved.

from ZSI import _copyright, ParsedSoap, SoapWriter, TC, ZSI_SCHEMA_URI, \
    FaultFromFaultMessage, _child_elements, _attrs, FaultException
from ZSI.auth import AUTH
import base64, httplib, cStringIO as StringIO, types, time, urlparse

_b64_encode = base64.encodestring

_AuthHeader = '<BasicAuth xmlns="' + ZSI_SCHEMA_URI + '''">
    <Name>%s</Name><Password>%s</Password>
</BasicAuth>'''


class _Caller:
    '''Internal class used to give the user a callable object
    that calls back to the Binding object to make an RPC call.
    '''

    def __init__(self, binding, name):
        self.binding, self.name = binding, name

    def __call__(self, *args):
        return self.binding.RPC(None, self.name, args, 
                                requesttypecode=TC.Any(self.name, aslist=1))
    

class _NamedParamCaller:
    '''Similar to _Caller, expect that there are named parameters
    not positional.
    '''

    def __init__(self, binding, name):
        self.binding, self.name = binding, name

    def __call__(self, **params):
        return self.binding.RPC(None, self.name, None, TC.Any(),
                _args=params, ## requesttypecode correct? XXX
                requesttypecode=TC.Any(self.name))


class Binding:
    '''Object that represents a binding (connection) to a SOAP server.
    Once the binding is created, various ways of sending and
    receiving SOAP messages are available, including a "name overloading"
    style.
    '''

    def __init__(self, nsdict=None, ssl=0, url=None, tracefile=None,
                 host='localhost', readerclass=None, port=None,
                 typesmodule=None, soapaction='""', ns=None, op_ns=None, **kw):
        '''Initialize.
        Keyword arguments include:
            host, port -- where server is; default is localhost
            ssl -- use SSL? default is no
            url -- resource to POST to
            soapaction -- value of SOAPAction header
            auth -- (type, name, password) triplet; default is unauth
            ns -- default namespace
            nsdict -- namespace entries to add
            tracefile -- file to dump packet traces
            cert_file, key_file -- SSL data (q.v.)
            readerclass -- DOM reader class
            ns -- the namespace to use for the SOAP:Body
            op_ns -- the namespace to use for the operation
        '''
        self.data = None
        self.ps = None
        self.ns = ns
        self.user_headers = []
        self.port = None
        self.typesmodule = typesmodule
        self.nsdict = nsdict or {}
        self.ssl = ssl
        self.url = url
        self.trace = tracefile
        self.host = host
        self.readerclass = readerclass
        self.soapaction = soapaction
        self.op_ns = op_ns

        if kw.has_key('auth'):
            self.SetAuth(*kw['auth'])
        else:
            self.SetAuth(AUTH.none)
        if self.nsdict.has_key(''):
            self.SetNS(nsdict[''])
        elif kw.has_key('ns'):
            self.SetNS(kw['ns'])
        if port:
            self.port = port
        elif url:
            hp = urlparse.urlsplit(url)
            if hp and hp[1].find(':') != -1:
                self.port = int(hp[1].split(':', 2)[1])
        if not self.ssl:
            if self.port is None: self.port = httplib.HTTP_PORT
        else:
            if self.port is None: self.port = httplib.HTTPS_PORT
            self.ssl_files = {}
            for k in [ 'cert_file', 'key_file' ]:
                if kw.has_key(k): self.ssl_files[k] = kw[k]

    def SetAuth(self, style, user=None, password=None):
        '''Change auth style, return object to user.
        '''
        self.auth_style, self.auth_user, self.auth_pass = \
            style, user, password
        return self

    def SetNS(self, uri):
        '''Change the default namespace.
        '''
        self.ns = uri
        return self

    def SetURL(self, url):
        '''Set the URL we post to.
        '''
        self.url = url
        return self

    def ResetHeaders(self):
        '''Empty the list of additional headers.
        '''
        self.user_headers = []
        return self

    def AddHeader(self, header, value):
        '''Add a header to send.
        '''
        self.user_headers.append((header, value))
        return self

    def RPC(self, url, opname, obj, replytype=None, **kw):
        '''Send a request, return the reply.  See Send() and Recieve()
        docstrings for details.
        '''
        self.Send(url, opname, obj, **kw)
        return self.Receive(replytype, **kw)

    def Send(self, url, opname, obj, nsdict=None, soapaction=None, **kw):
        '''Send a message.  If url is None, use the value from the
        constructor (else error). obj is the object (data) to send.
        Data may be described with a requesttypecode keyword, or a
        requestclass keyword; default is the class's typecode (if
        there is one), else Any.
        '''
            
        # Get the TC for the obj.
        if kw.has_key('requesttypecode'):
            tc = kw['requesttypecode']
        elif kw.has_key('requestclass'):
            tc = kw['requestclass'].typecode
        elif type(obj) == types.InstanceType:
            tc = getattr(obj.__class__, 'typecode')
            if tc is None: tc = TC.Any(opname, aslist=1)
        else:
            tc = TC.Any(opname, aslist=1)


        if self.op_ns:
            opname = '%s:%s' % (self.op_ns, opname)
            tc.oname = opname 

        # Determine the SOAP auth element.
        if kw.has_key('auth_header'):
            auth_header = kw['auth_header']
        elif self.auth_style & AUTH.zsibasic:
            auth_header = _AuthHeader % (self.auth_user, self.auth_pass)
        else:
            auth_header = None

        # Serialize the object.
        s = StringIO.StringIO()
        d = self.nsdict or {}
        if self.ns: d[''] = self.ns
        d.update(nsdict or self.nsdict or {})
        sw = SoapWriter(s, nsdict=d, header=auth_header)
        if kw.has_key('_args'):
            sw.serialize(kw['_args'], tc)
        else:
            sw.serialize(obj, tc, typed=0)
        sw.close()
        soapdata = s.getvalue()

        # Tracing?
        if self.trace:
            print >>self.trace, "_" * 33, time.ctime(time.time()), "REQUEST:"
            print >>self.trace, soapdata

        # Send the request.
        # host and port may be parsed from a WSDL file.  if they are, they
        # are most likely unicode format, which httplib does not care for
        if isinstance(self.host, unicode):
            self.host = str(self.host)
        if not isinstance(self.port, int):
            self.port = int(self.port)
            
        if not self.ssl:
            self.h = httplib.HTTPConnection(self.host, self.port)
        else:
            self.h = httplib.HTTPSConnection(self.host, self.port,
                        **self.ssl_files)

        self.h.connect()
        self.h.putrequest("POST", url or self.url)
        self.h.putheader("Content-length", "%d" % len(soapdata))
        self.h.putheader("Content-type", 'text/xml; charset=utf-8')
        SOAPActionValue = '"%s"' % (soapaction or self.soapaction)
        self.h.putheader("SOAPAction", SOAPActionValue)
        if self.auth_style & AUTH.httpbasic:
            val = _b64_encode(self.auth_user + ':' + self.auth_pass) \
                        .replace("\012", "")
            self.h.putheader('Authorization', 'Basic ' + val)
        for header,value in self.user_headers:
            self.h.putheader(header, value)
        self.h.endheaders()
        self.h.send(soapdata)

        # Clear prior receive state.
        self.data, self.ps = None, None

    def ReceiveRaw(self, **kw):
        '''Read a server reply, unconverted to any format and return it.
        '''
        if self.data: return self.data
        trace = self.trace
        while 1:
            response = self.h.getresponse()
            self.reply_code, self.reply_msg, self.reply_headers, self.data = \
                response.status, response.reason, response.msg, response.read()
            if trace:
                print >>trace, "_" * 33, time.ctime(time.time()), "RESPONSE:"
                print >>trace, str(self.reply_headers)
                print >>trace, self.data
            if response.status != 100: break
            self.h._HTTPConnection__state = httplib._CS_REQ_SENT
            self.h._HTTPConnection__response = None
        return self.data

    def IsSOAP(self):
        if self.ps: return 1
        self.ReceiveRaw()
        mimetype = self.reply_headers.type
        return mimetype == 'text/xml'

    def ReceiveSOAP(self, readerclass=None, **kw):
        '''Get back a SOAP message.
        '''
        if self.ps: return self.ps
        if not self.IsSOAP():
            raise TypeError(
                'Response is "%s", not "text/xml"' % self.reply_headers.type)
        if len(self.data) == 0:
            raise TypeError('Received empty response')
        self.ps = ParsedSoap(self.data, 
                        readerclass=readerclass or self.readerclass)
        return self.ps

    def IsAFault(self):
        '''Get a SOAP message, see if it has a fault.
        '''
        self.ReceiveSOAP()
        return self.ps.IsAFault()

    def ReceiveFault(self, **kw):
        '''Parse incoming message as a fault. Raise TypeError if no
        fault found.
        '''
        self.ReceiveSOAP(**kw)
        if not self.ps.IsAFault():
            raise TypeError("Expected SOAP Fault not found")
        return FaultFromFaultMessage(self.ps)

    def Receive(self, replytype=None, **kw):
        '''Parse message, create Python object. if replytype is None, use
        TC.Any to dynamically parse; otherwise it can be a Python class
        or the typecode to use in parsing.
        '''
        self.ReceiveSOAP(**kw)
        if self.ps.IsAFault():
            msg = FaultFromFaultMessage(self.ps)
            raise FaultException(msg)

        if replytype is None:
            tc = TC.Any(aslist=1)

            # if the message is RPC style, skip the fooBarResponse
            elt_name = '%s' % self.ps.body_root.localName
            if elt_name.find('Response') > 0:
                data = _child_elements(self.ps.body_root)
            else:
                data = [self.ps.body_root]
                
            if len(data) == 0: return None

            # check for array type, loop and process if found
            for attr in _attrs(data[0]):
                if attr.localName.find('arrayType') >= 0:
                    data = _child_elements(data[0])

                    toReturn = []
                    for node in data:
                        type = node.localName

                        # handle case where multiple elements are returned
                        if type.find('element') >= 0:
                            node = _child_elements(node)[0]
                            type = node.localName

                        toReturn.append(self.__parse(node, type))
                    return toReturn

            # parse a complex or primitive type and return it
            type = data[0].localName
            return self.__parse(data[0], type)
        elif hasattr(replytype, 'typecode'):
            tc = replytype.typecode
        else:
            tc = replytype
        return self.ps.Parse(tc)

    def __parse(self, node, type):
        try:
            if hasattr(self.typesmodule, type):
                clazz = getattr(self.typesmodule, type)
                tc = clazz.typecode
                return tc.parse(node, self.ps)
            else:
                tc = TC.Any(aslist=1)
                return tc.parse(node, self.ps)                
        except Exception:
            tc = TC.Any(aslist=1)
            return tc.parse(node, self.ps)
        
    def __repr__(self):
        return "<%s instance at 0x%x>" % (self.__class__.__name__, id(self))

    def __getattr__(self, name):
        '''Return a callable object that will invoke the RPC method
        named by the attribute.
        '''
        if name[:2] == '__' and len(name) > 5 and name[-2:] == '__':
            if hasattr(self, name): return getattr(self, name)
            return getattr(self.__class__, name)
        return _Caller(self, name)


class NamedParamBinding(Binding):
    '''Like binding, except the argument list for invocation is
    named parameters.
    '''

    def __getattr__(self, name):
        '''Return a callable object that will invoke the RPC method
        named by the attribute.
        '''
        if name[:2] == '__' and len(name) > 5 and name[-2:] == '__':
            if hasattr(self, name): return getattr(self, name)
            return getattr(self.__class__, name)
        return _NamedParamCaller(self, name)


if __name__ == '__main__': print _copyright
