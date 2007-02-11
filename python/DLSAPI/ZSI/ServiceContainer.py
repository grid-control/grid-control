#! /usr/bin/env python
'''Simple Service Container
   -- use with wsdl2py generated modules.
'''

import urlparse, types, os, sys, thread, cStringIO as StringIO
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

from ZSI import ParseException, FaultFromException, Fault
from ZSI import _copyright, _seqtypes, resolvers, Version
from ZSI.parse import ParsedSoap
from ZSI.writer import SoapWriter

"""
Functions:
    _Dispatch
    AsServer
    GetSOAPContext
    
Classes:
    SOAPContext
    NoSuchService
    PostNotSpecified
    SOAPActionNotSpecified
    ServiceSOAPBinding
    SOAPRequestHandler
    ServiceContainer
"""

class SOAPContext:
    def __init__(self, container, xmldata, ps, connection, httpheaders,
                 soapaction):

        self.container = container
        self.xmldata    = xmldata
        self.parsedsoap = ps
        self.connection = connection
        self.httpheaders= httpheaders
        self.soapaction = soapaction

_contexts = dict()
def GetSOAPContext():
    global _contexts
    return _contexts[thread.get_ident()]

def _Dispatch(ps, server, SendResponse, SendFault, post, action, nsdict={}, **kw):
    '''Send ParsedSoap instance to ServiceContainer, which dispatches to
    appropriate service via post, and method via action.  Response is a
    self-describing pyobj, which is passed to a SoapWriter.

    Call SendResponse or SendFault to send the reply back, appropriately.
        server -- ServiceContainer instance

    '''
    try:
        result = server(ps, post, action)
    except NotAuthorized, e:
        return SendFault(Fault(Fault.Server, "Not authorized"), code=401)
    except Exception, e:
        return SendFault(FaultFromException(e, 0, sys.exc_info()[2]), **kw)
    if result is None:
        return
    reply = StringIO.StringIO()
    try:
        SoapWriter(reply, nsdict=nsdict).serialize(result)
        return SendResponse(reply.getvalue(), **kw)
    except Exception, e:
        return SendFault(FaultFromException(e, 0, sys.exc_info()[2]), **kw)


def AsServer(port=80, services=()):
    '''port --
       services -- list of service instances
    '''
    address = ('', port)
    sc = ServiceContainer(address)
    for service in services:
        path = service.getPost()
        sc.setNode(service, path)
    sc.serve_forever()


class NoSuchService(Exception): pass
class NoSuchMethod(Exception): pass
class NotAuthorized(Exception): pass
class ServiceAlreadyPresent(Exception): pass
class PostNotSpecified(Exception): pass
class SOAPActionNotSpecified(Exception): pass

class ServiceSOAPBinding:
    """
    Binding defines the set of wsdl:binding operations, it takes as input
    a ParsedSoap instance and parses it into a pyobj.  It returns a
    response pyobj.
 
    class variables:
        soapAction -- dictionary of soapAction keys, and operation name values.
           These are specified in the WSDL soap bindings. There must be a 
           class method matching the operation name value.

    """
    soapAction = {}
    
    def __init__(self, post):
        self.post = post

    def authorize(self, auth_info, post, action):
        return 1

    def __call___(self, action, ps):
        return self.getOperation(action)(ps)

    def getPost(self):
        return self.post

    def getOperation(self, action):
        '''Returns a method of class.
           action -- soapAction value
        '''
        opName = self.getOperationName(action)
        return getattr(self, opName)

    def getOperationName(self, action):
        '''Returns operation name.
           action -- soapAction value
        '''
        if not self.soapAction.has_key(action):
            raise SOAPActionNotSpecified, '%s is NOT in soapAction dictionary' %action
        return self.soapAction[action]


class SOAPRequestHandler(BaseHTTPRequestHandler):
    '''SOAP handler.
    '''
    server_version = 'ZSI/%s ' % ".".join(map(str, Version())) + BaseHTTPRequestHandler.server_version

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
        global _contexts
        
        soapAction = self.headers.getheader('SOAPAction').strip('\'"')
        post = self.path
        if not post:
            raise PostNotSpecified, 'HTTP POST not specified in request'
        if not soapAction:
            raise SOAPActionNotSpecified, 'SOAP Action not specified in request'
        soapAction = soapAction.strip('\'"')
        post = post.strip('\'"')
        try:
            ct = self.headers['content-type']
            if ct.startswith('multipart/'):
                cid = resolvers.MIMEResolver(ct, self.rfile)
                xml = cid.GetSOAPPart()
                ps = ParsedSoap(xml, resolver=cid.Resolve)
            else:
                length = int(self.headers['content-length'])
                xml = self.rfile.read(length)
                ps = ParsedSoap(xml)
        except ParseException, e:
            self.send_fault(FaultFromZSIException(e))
        except Exception, e:
            # Faulted while processing; assume it's in the header.
            self.send_fault(FaultFromException(e, 1, sys.exc_info()[2]))
        else:
            # Keep track of calls
            thread_id = thread.get_ident()
            _contexts[thread_id] = SOAPContext(self.server, xml, ps,
                                               self.connection,
                                               self.headers, soapAction)

            
            _Dispatch(ps, self.server, self.send_xml, self.send_fault, 
                post=post, action=soapAction)

            # Clean up after the call
            if _contexts.has_key(thread_id):
                del _contexts[thread_id]


#     def do_GET(self):
#         '''The GET command.
#         '''
#         if self.path.endswith("?wsdl"):
#             service_path = self.path[:-5]
#             service = self.server.getNode(service_path)
#             if hasattr(service, "_wsdl"):
#                 self.send_xml(service._wsdl)
#             else:
#                 self.send_error(404)
#         else:
#             self.send_error(404)

class ServiceContainer(HTTPServer):
    '''HTTPServer that stores service instances according 
    to POST values.  An action value is instance specific,
    and specifies an operation (function) of an instance.
    '''
    class NodeTree:
        '''Simple dictionary implementation of a node tree
        '''
        def __init__(self):
            self.__dict = {}

        def __str__(self):
            return str(self.__dict)

        def getNode(self, path):
            if path.startswith("/"):
                path = path[1:]
                
            if self.__dict.has_key(path):                

                return self.__dict[path]
            else:
                raise NoSuchService, 'No service(%s) in ServiceContainer' %path

        def getPathForNode(self, node):
            path = None

            for k,v in self.__dict.items():
                if node == v:
                    path = k

            if node:
                if path.startswith("/"):
                    path = path[1:]
                
                return path
            else:
                raise NoSuchService, 'No service(%s) in ServiceContainer' %node
            
        def setNode(self, service, path):
            if path.startswith("/"):
                path = path[1:]
                
            if not isinstance(service, ServiceSOAPBinding):
               raise TypeError, 'A Service must implement class ServiceSOAPBinding'
            if self.__dict.has_key(path):
                raise ServiceAlreadyPresent, 'Service(%s) already in ServiceContainer' % path
            else:
                self.__dict[path] = service

        def removeNode(self, path):
            if path.startswith("/"):
                path = path[1:]
                
            if self.__dict.has_key(path):
                node = self.__dict[path]
                del self.__dict[path]
                return node
            else:
                raise NoSuchService, 'No service(%s) in ServiceContainer' %path
            
    def __init__(self, server_address, RequestHandlerClass=SOAPRequestHandler):
        '''server_address -- 
           RequestHandlerClass -- 
        '''
        HTTPServer.__init__(self, server_address, RequestHandlerClass)
        self._nodes = self.NodeTree()

    def __call__(self, ps, post, action):
        '''ps -- ParsedSoap representing the request
           post -- HTTP POST --> instance
           action -- Soap Action header --> method
        '''
        return self.getCallBack(post, action)(ps)

    def getNode(self, post):
        '''post -- POST HTTP value
        '''
        path = urlparse.urlsplit(post)[2]
        return self._nodes.getNode(path)

    def setNode(self, service, post):
        '''service -- service instance
           post -- POST HTTP value
        '''
        path = urlparse.urlsplit(post)[2]
        self._nodes.setNode(service, path)

    def removeNode(self, post):
        '''post -- POST HTTP value
        '''
        path = urlparse.urlsplit(post)[2]
        self._nodes.removeNode(path)

    def getPath(self, node):
        return self._nodes.getPathForNode(node)

    def getURL(self, node):
        path = self._nodes.getPathForNode(node)
        return self.makeURL(path)

    def makeURL(self, path):
        return "http://%s:%d/%s" % (self.server_name, self.server_port, path)
    
    def getCallBack(self, post, action):
        '''post -- POST HTTP value
           action -- SOAP Action value
        '''
        node = self.getNode(post)
        
        if node is None:
            raise NoSuchMethod, "Method (%s) not found at path (%s)" % (action,
                                                                        path)

        if node.authorize(None, post, action):
            return node.getOperation(action)
        else:
            raise NotAuthorized, "Authorization failed for method %s" % action

if __name__ == '__main__': print _copyright
