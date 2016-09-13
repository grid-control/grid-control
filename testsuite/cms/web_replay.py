import logging
from grid_control.utils.webservice import RestClient
from python_compat import json, sorted


def replay_read(fn):
	replay_cache = {}
	for line in open(fn):
		(url, headers, params, data, result) = json.loads(line)
		replay_cache[(url, headers, params, data)] = result
	return replay_cache

def replay_write(fn):
	fp = open(fn, 'w')
	for key in sorted(RestClient.replay_cache):
		(url, headers, params, data) = key
		fp.write(json.dumps((url, headers, params, data, RestClient.replay_cache[key])) + '\n')
	fp.close()

def replay_start(fn, modifier = None):
	RestClient.replay_cache = replay_read(fn)

	old_req = getattr(RestClient, '_request')
	def new_req(self, request_fun, url, api, headers, params = None, data = None):
		request_headers = dict(self._headers or {})
		request_headers.update(headers or {})
		url_key = url
		if url is None:
			url_key = self._url
		if api:
			url_key += '/%s' % api
		key = (url_key, json.dumps(sorted(request_headers.items())),
			json.dumps(sorted((params or {}).items())), json.dumps(data))
		if key not in RestClient.replay_cache or (len(RestClient.replay_cache[key]) == 32):
			logging.getLogger().critical('New request: %s' % repr(key))
			RestClient.replay_cache[key] = old_req(self, request_fun, url, api, headers, params, data)
		if modifier is None:
			return RestClient.replay_cache[key]
		return modifier(key, RestClient.replay_cache[key])
	setattr(RestClient, '_request', new_req)
