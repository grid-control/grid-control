#! /usr/bin/env python
# $Header: /cvsroot/pywebsvcs/zsi/ZSI/TCapache.py,v 1.5 2002/10/21 17:11:13 rsalz Exp $
'''Apache typecodes.
'''

from ZSI import _copyright, _child_elements
from ZSI.TC import TypeCode, Struct as _Struct, Any as _Any

class Apache:
    NS = "http://xml.apache.org/xml-soap"

class _Map(TypeCode):
    '''Apache's "Map" type.
    '''
    parselist = [ (Apache.NS, 'Map') ]

    def __init__(self, pname=None, aslist=0, **kw):
        TypeCode.__init__(self, pname, **kw)
        self.aslist = aslist
        self.tc = _Struct(None, [ _Any('key'), _Any('value') ], inline=1)

    def parse(self, elt, ps):
        self.checkname(elt, ps)
        if self.nilled(elt, ps): return None
        p = self.tc.parse
        if self.aslist:
            v = []
            for c in _child_elements(elt):
                d = p(c, ps)
                v.append((d['key'], d['value']))
        else:
            v = {}
            for c in _child_elements(elt):
                d = p(c, ps)
                v[d['key']] = d['value']
        return v

    def serialize(self, sw, pyobj, name=None, attrtext=None, **kw):
        n = name or self.oname or 'E%x' % id(pyobj)
        if self.typed:
            tstr = ' xsi:type="A:Map" xmlns:A="%s"' % Apache.NS
        else:
            tstr = ''
        print >>sw, "<%s%s%s>" % (n, attrtext or '', tstr)
        if self.aslist:
            for k,v in pyobj:
                self.tc.serialize(sw, {'key': k, 'value': v}, name='item')
        else:
            for k,v in pyobj.items():
                self.tc.serialize(sw, {'key': k, 'value': v}, name='item')
        print >>sw, "</%s>" % n


Apache.Map = _Map

if __name__ == '__main__': print _copyright
