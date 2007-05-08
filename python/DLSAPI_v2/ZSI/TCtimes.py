#! /usr/bin/env python
# $Header: /cvsroot/pywebsvcs/zsi/ZSI/TCtimes.py,v 1.13 2005/02/15 17:44:50 rsalz Exp $
'''Typecodes for dates and times.
'''

from ZSI import _copyright, _floattypes, _inttypes, EvaluateException
from ZSI.TC import TypeCode
import operator, re, time

_niltime = [
    0, 0, 0,    # year month day
    0, 0, 0,    # hour minute second
    0, 0, 0     # weekday, julian day, dst flag
]

def _dict_to_tuple(d):
    '''Convert a dictionary to a time tuple.  Depends on key values in the
    regexp pattern!
    '''
    retval = _niltime[:]
    for k,i in ( ('Y', 0), ('M', 1), ('D', 2), ('h', 3), ('m', 4), ):
        v = d.get(k)
        if v: retval[i] = int(v)
    v = d.get('s')
    if v: retval[5] = int(float(v))
    v = d.get('tz')
    if v and v != 'Z':
        h,m = map(int, v.split(':'))
        if h < 0:
            retval[3] += abs(h)
            retval[4] += m
        else:
            retval[3] -= abs(h)
            retval[4] -= m
    if d.get('neg', 0):
        retval[0:5] = map(operator.__neg__, retval[0:5])
    return tuple(retval)



class Duration(TypeCode):
    '''Time duration.
    '''

    parselist = [ (None,'duration') ]
    lex_pattern = re.compile('^' r'(?P<neg>-?)P' \
                    r'((?P<Y>\d+)Y)?' r'((?P<M>\d+)M)?' r'((?P<D>\d+)D)?' \
                    r'(?P<T>T?)' r'((?P<h>\d+)H)?' r'((?P<m>\d+)M)?' \
                    r'((?P<s>\d*(\.\d+)?)S)?' '$')

    def parse(self, elt, ps):
        self.checkname(elt, ps)
        elt = self.SimpleHREF(elt, ps, 'duration')
        if not elt: return None
        if self.nilled(elt, ps): return None
        v = self.simple_value(elt, ps).strip()
        m = Duration.lex_pattern.match(v)
        if m is None:
            raise EvaluateException('Illegal duration', ps.Backtrace(elt))
        d = m.groupdict()
        if d['T'] and (d['h'] is None and d['m'] is None and d['s'] is None):
            raise EvaluateException('Duration has T without time',
                ps.Backtrace(elt))
        try:
            retval = _dict_to_tuple(d)
        except ValueError, e:
            raise EvaluateException(str(e), ps.Backtrace(elt))
        return retval

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        n = name or self.oname or ('E%x' % id(pyobj))
        if 1 in map(lambda x: x < 0, pyobj[0:6]):
            pyobj = map(abs, pyobj)
            neg = '-'
        else:
            neg = ''
        val = '%sP%dY%dM%dDT%dH%dM%dS' % \
            ( neg, pyobj[0], pyobj[1], pyobj[2], pyobj[3], pyobj[4], pyobj[5])
        if kw.get('typed', self.typed):
            tstr = ' xsi:type="xsd:duration"'
        else:
            tstr = ''
        print >>sw, '<%s%s%s>%s</%s>' % (n, attrtext, tstr, val, n)

class Gregorian(TypeCode):
    '''Gregorian times.
    '''
    lex_pattern =tag = format = None

    def parse(self, elt, ps):
        self.checkname(elt, ps)
        elt = self.SimpleHREF(elt, ps, 'Gregorian')
        if not elt: return None
        if self.nilled(elt, ps): return None
        v = self.simple_value(elt, ps).strip()
        m = self.lex_pattern.match(v)
        if not m:
            raise EvaluateException('Bad Gregorian', ps.Backtrace(elt))
        try:
            retval = _dict_to_tuple(m.groupdict())
        except ValueError, e:
            raise EvaluateException(str(e), ps.Backtrace(elt))
        return retval

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        if type(pyobj) in _floattypes or type(pyobj) in _inttypes:
            pyobj = time.gmtime(pyobj)
        n = name or self.oname or ('E%x' % id(pyobj))
        d = {}
        pyobj = tuple(pyobj)
        if 1 in map(lambda x: x < 0, pyobj[0:6]):
            pyobj = map(abs, pyobj)
            d['neg'] = '-'
        else:
            d['neg'] = ''

        d = { 'Y': pyobj[0], 'M': pyobj[1], 'D': pyobj[2],
            'h': pyobj[3], 'm': pyobj[4], 's': pyobj[5], }
        val = self.format % d
        if kw.get('typed', self.typed):
            if self.tag and self.tag.find(':') != -1:
                tstr = ' xsi:type="%s"' % self.tag
            else:
                tstr = ' xsi:type="xsd:%s"' % self.tag


        else:
            tstr = ''

        # ignore the xmlns if it was explicitly stated
        i = n.find('xmlns')
        if i > 0:
            ctag = '</%s>' % n[:i - 1]
        else:
            ctag = '</%s>' % n
            
        print >>sw, '<%s%s%s>%s%s' % (n, attrtext, tstr, val, ctag)

class gDateTime(Gregorian):
    '''A date and time.
    '''
    parselist = [ (None,'dateTime') ]
    lex_pattern = re.compile('^' r'(?P<neg>-?)' \
                        '(?P<Y>\d{4,})-' r'(?P<M>\d\d)-' r'(?P<D>\d\d)' 'T' \
                        r'(?P<h>\d\d):' r'(?P<m>\d\d):' r'(?P<s>\d*(\.\d+)?)' \
                        r'(?P<tz>(Z|([-+]\d\d:\d\d))?)' '$')
    tag, format = 'dateTime', '%(Y)04d-%(M)02d-%(D)02dT%(h)02d:%(m)02d:%(s)02dZ'

class gDate(Gregorian):
    '''A date.
    '''
    parselist = [ (None,'date') ]
    lex_pattern = re.compile('^' r'(?P<neg>-?)' \
                        '(?P<Y>\d{4,})-' r'(?P<M>\d\d)-' r'(?P<D>\d\d)' \
                        r'(?P<tz>Z|([-+]\d\d:\d\d))?' '$')
    tag, format = 'date', '%(Y)04d-%(M)02d-%(D)02dZ'

class gYearMonth(Gregorian):
    '''A date.
    '''
    parselist = [ (None,'gYearMonth') ]
    lex_pattern = re.compile('^' r'(?P<neg>-?)' \
                        '(?P<Y>\d{4,})-' r'(?P<M>\d\d)' \
                        r'(?P<tz>Z|([-+]\d\d:\d\d))?' '$')
    tag, format = 'gYearMonth', '%(Y)04d-%(M)02dZ'

class gYear(Gregorian):
    '''A date.
    '''
    parselist = [ (None,'gYear') ]
    lex_pattern = re.compile('^' r'(?P<neg>-?)' \
                        '(?P<Y>\d{4,})' \
                        r'(?P<tz>Z|([-+]\d\d:\d\d))?' '$')
    tag, format = 'gYear', '%(Y)04dZ'

class gMonthDay(Gregorian):
    '''A gMonthDay.
    '''
    parselist = [ (None,'gMonthDay') ]
    lex_pattern = re.compile('^' r'(?P<neg>-?)' \
                        r'--(?P<M>\d\d)-' r'(?P<D>\d\d)' \
                        r'(?P<tz>Z|([-+]\d\d:\d\d))?' '$')
    tag, format = 'gMonthDay', '%(M)02d-%(D)02dZ'

class gDay(Gregorian):
    '''A gDay.
    '''
    parselist = [ (None,'gDay') ]
    lex_pattern = re.compile('^' r'(?P<neg>-?)' \
                        r'---(?P<D>\d\d)' \
                        r'(?P<tz>Z|([-+]\d\d:\d\d))?' '$')
    tag, format = 'gDay', '%(D)02dZ'

class gTime(Gregorian):
    '''A time.
    '''
    parselist = [ (None,'time') ]
    lex_pattern = re.compile('^' r'(?P<neg>-?)' \
                        r'(?P<h>\d\d):' r'(?P<m>\d\d):' r'(?P<s>\d*(\.\d+)?)' \
                        r'(?P<tz>Z|([-+]\d\d:\d\d))?' '$')
    tag, format = 'time', '%(h)02d:%(m)02d:%(s)02dZ'

if __name__ == '__main__': print _copyright
