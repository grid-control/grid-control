#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
import os
from testFwk import cmp_obj, run_test, try_catch
from grid_control.utils.webservice import GridJSONRestClient, JSONRestClient, RestClient, RestError, RestSession


def response_filter(response):
	response.pop('origin')
	header = response['headers']
	header.pop('Accept-Encoding')
	header.pop('User-Agent')
	return response


class Test_RestClient:
	"""
	>>> rc = RestClient()
	>>> 'UTF-8 encoding' in rc.get('https://httpbin.org/encoding/utf8')
	True
	>>> try_catch(lambda: RestSession().request(RestSession.GET, 'https://httpbin.org/encoding/utf8', None), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_JSONRestClient:
	"""
	>>> jrc0 = JSONRestClient(url = 'https://httpbin.org/get')
	>>> r0 = response_filter(jrc0.get())
	>>> cmp_obj(r0, {'headers': {'Accept': 'application/json',
	...    'Host': 'httpbin.org', 'Content-Type': 'application/json'},
	...    'args': {}, 'url': 'https://httpbin.org/get'})
	>>> jrc = JSONRestClient()
	>>> r1 = response_filter(jrc.get('https://httpbin.org/get'))
	>>> r1 = response_filter(jrc.get('https://httpbin.org/get'))
	>>> cmp_obj(r1, {'headers': {'Accept': 'application/json',
	...    'Host': 'httpbin.org', 'Content-Type': 'application/json'},
	...    'args': {}, 'url': 'https://httpbin.org/get'})
	>>> r2 = response_filter(jrc.get('https://httpbin.org', 'get', {'header': 'test'}, {'key': 'value'}))
	>>> cmp_obj(r2, {'headers': {'Accept': 'application/json',
	...    'Host': 'httpbin.org', 'Content-Type': 'application/json', 'Header': 'test'},
	...    'args': {'key': 'value'}, 'url': 'https://httpbin.org/get?key=value'})
	>>> r3 = response_filter(jrc.post('https://httpbin.org', 'post', data = {'test': 'value'}))
	>>> cmp_obj(r3, {'headers': {'Accept': 'application/json',
	...    'Host': 'httpbin.org', 'Content-Type': 'application/json', 'Content-Length': '17'},
	...    'files': {}, 'form': {}, 'url': 'https://httpbin.org/post', 'args': {},
	...    'json': {'test': 'value'}, 'data': '{"test": "value"}'})
	>>> r4 = response_filter(jrc.put('https://httpbin.org', 'put', data = {'test': 'value'}))
	>>> cmp_obj(r4, {'headers': {'Accept': 'application/json',
	...    'Host': 'httpbin.org', 'Content-Type': 'application/json', 'Content-Length': '17'},
	...    'files': {}, 'form': {}, 'url': 'https://httpbin.org/put', 'args': {},
	...    'json': {'test': 'value'}, 'data': '{"test": "value"}'})
	>>> r5 = response_filter(jrc.delete('https://httpbin.org', 'delete', params = {'key': 'value'}))
	>>> dummy = r5['headers'].pop('Content-Length', None)
	>>> cmp_obj(r5, {'headers': {'Accept': 'application/json',
	...    'Host': 'httpbin.org', 'Content-Type': 'application/json'},
	...    'files': {}, 'form': {}, 'url': 'https://httpbin.org/delete?key=value', 'args': {'key': 'value'},
	...    'json': None, 'data': ''})
	>>> try_catch(lambda: jrc.get('https://httpbin.org/getx'), 'RestError', 'Unable to query')
	caught
	>>> try_catch(lambda: jrc.get('https://httpbin.org/html'), 'RestError', 'Received invalid JSON reply')
	caught
	>>> try_catch(lambda: jrc.get('https://httpbin.org/bytes/0'), 'RestError', 'Received empty reply')
	caught
	"""

class Test_GridJSONRestClient:
	"""
	>>> os.environ['X509_USER_PROXY'] = ''
	>>> gjrc = GridJSONRestClient(cert_error_msg = 'AwesomeAPI:', cert_error_cls = RestError)
	AwesomeAPI: Using this webservice requires a valid grid proxy!
	>>> try_catch(lambda: gjrc.get('https://httpbin.org/bytes/0'), 'RestError', 'X509_USER_PROXY is not set')
	caught
	>>> os.environ['X509_USER_PROXY'] = 'invalid'
	>>> try_catch(lambda: gjrc.get('https://httpbin.org/bytes/0'), 'RestError', 'X509_USER_PROXY points to invalid path')
	caught
	"""

run_test()
