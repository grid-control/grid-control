#! /usr/bin/env python
# $Header: /cvsroot/pywebsvcs/zsi/ZSI/fault.py,v 1.8 2004/04/29 01:47:55 boverhof Exp $
'''Faults.
'''

from ZSI import _copyright, _children, _child_elements, \
        _textprotect, _stringtypes, _seqtypes, _Node, SoapWriter

from ZSI.wstools.Namespaces import SOAP
try:
    from xml.dom.ext import Canonicalize
except:
    from ZSI.compat import Canonicalize
import traceback, cStringIO as StringIO

class Fault:
    '''SOAP Faults.
    '''

    Client = "SOAP-ENV:Client"
    Server = "SOAP-ENV:Server"
    MU     = "SOAP-ENV:MustUnderstand"

    def __init__(self, code, string,
                actor=None, detail=None, headerdetail=None):
        self.code, self.string, self.actor, self.detail, self.headerdetail = \
                code, string, actor, detail, headerdetail

    def _do_details(self, out, header):
        if header:
            elt, detail = 'ZSI:detail', self.headerdetail
        else:
            elt, detail = 'detail', self.detail
        print >>out, '<%s>' % elt
        if type(detail) in _stringtypes:
            print >>out, detail
        else:
            for d in self.detail: Canonicalize(d, out)
        print >>out, '</%s>' % elt

    def DataForSOAPHeader(self):
        if not self.headerdetail: return None
        # SOAP spec doesn't say how to encode header fault data.
        s = StringIO.StringIO()
        self._do_details(s, 1)
        return s.getvalue()

    def serialize(self, sw):
        '''Serialize the object.'''
        print >>sw, '<SOAP-ENV:Fault>\n', \
            '<faultcode>%s</faultcode>\n' % self.code, \
            '<faultstring>%s</faultstring>' % self.string
        if self.actor:
            print >>sw, \
                '<SOAP-ENV:faultactor>%s</SOAP-ENV:faultactor>' % self.actor
        if self.detail: self._do_details(sw, 0)
        print >>sw, '</SOAP-ENV:Fault>'

    def AsSOAP(self, output=None, **kw):
        if output is None:
            s = StringIO.StringIO()
            output = s
        else:
            s = None
        mykw = { 'header': self.DataForSOAPHeader() }
        if kw: mykw.update(kw)
        sw = SoapWriter(output, **mykw)
        self.serialize(sw)
        sw.close()
        if s: 
            return s.getvalue()
        else:
            return None
    AsSoap = AsSOAP

def FaultFromNotUnderstood(uri, localname, actor=None):
    elt = '''<ZSI:URIFaultDetail>
<ZSI:URI>%s</ZSI:URI>
<ZSI:localname>%s</ZSI:localname>
</ZSI:URIFaultDetail>
''' % (uri, localname)
    detail, headerdetail = None, elt
    return Fault(Fault.MU, 'SOAP mustUnderstand not understood',
                actor, detail, headerdetail)

def FaultFromActor(uri, actor=None):
    elt = '''<ZSI:ActorFaultDetail>
<ZSI:URI>%s</ZSI:URI>
</ZSI:ActorFaultDetail>
''' % uri
    detail, headerdetail = None, elt
    return Fault(Fault.Client, 'Cannot process specified actor',
                actor, detail, headerdetail)

def FaultFromZSIException(ex, actor=None):
    '''Return a Fault object created from a ZSI exception object.
    '''
    mystr = getattr(ex, 'str') or str(ex)
    mytrace = getattr(ex, 'trace', '') or ''
    elt = '''<ZSI:ParseFaultDetail>
<ZSI:string>%s</ZSI:string>
<ZSI:trace>%s</ZSI:trace>
</ZSI:ParseFaultDetail>
''' % (_textprotect(mystr), _textprotect(mytrace))
    if getattr(ex, 'inheader', 0):
        detail, headerdetail = None, elt
    else:
        detail, headerdetail = elt, None
    return Fault(Fault.Client, 'Unparseable message',
                actor, detail, headerdetail)

def FaultFromException(ex, inheader, tb=None, actor=None):
    '''Return a Fault object created from a Python exception.
    '''
    if tb:
        try:
            lines = '\n'.join(['%s:%d:%s' % (name, line, func)
                        for name, line, func, text in traceback.extract_tb(tb)])
            tracetext = '<ZSI:trace>\n' + _textprotect(lines) + '</ZSI:trace>\n'
        except:
            tracetext = ''
    else:
        tracetext = ''

    elt = '''<ZSI:FaultDetail>
<ZSI:string>%s</ZSI:string>
%s</ZSI:FaultDetail>
''' % ( _textprotect(str(ex)), tracetext)
    if inheader:
        detail, headerdetail = None, elt
    else:
        detail, headerdetail = elt, None
    return Fault(Fault.Server, 'Processing Failure',
                actor, detail, headerdetail)

def FaultFromFaultMessage(ps):
    '''Parse the message as a fault.
    '''
    d = { 'faultcode': None, 'faultstring': None, 'faultactor': None,
        'detail': None, }
    for elt in _child_elements(ps.body_root):
        n = elt.localName
        if n == 'detail':
            d['detail'] = _child_elements(elt)
        if n in [ 'faultcode', 'faultstring', 'faultactor' ]:
            d[n] = ''.join([E.nodeValue for E in _children(elt)
                            if E.nodeType 
                            in [ _Node.TEXT_NODE, _Node.CDATA_SECTION_NODE ]])
    return Fault(d['faultcode'], d['faultstring'],
                d['faultactor'], d['detail'])

if __name__ == '__main__': print _copyright
