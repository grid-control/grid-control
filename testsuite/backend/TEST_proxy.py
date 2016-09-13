#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
import os, logging
from testFwk import create_config, run_test, setPath, try_catch
from grid_control.backends.access import AccessToken, RefreshableAccessToken, TimedAccessToken
from python_compat import lmap, sorted


config1 = create_config(config_dict={'proxy': {'min lifetime': '0:02:00', 'ignore warnings': 'False'}})
config2 = create_config(config_dict={'proxy': {'ignore warnings': 'True', 'proxy path': '/dev/null'}})

setPath('../bin')

class TestTimedProxy(TimedAccessToken):
	def __init__(self, config, tmp):
		TimedAccessToken.__init__(self, config, 'myproxy')
		self._tmp = tmp

	def _get_timeleft(self, cached):
		if self._tmp < 0:
			return TimedAccessToken._get_timeleft(self, cached)
		return self._tmp


class TestRefreshProxy(RefreshableAccessToken):
	def __init__(self, config, tmp, refresh_enabled = True):
		RefreshableAccessToken.__init__(self, config, 'myproxy')
		self._tmp = tmp
		self._refresh_enabled = refresh_enabled

	def _refreshAccessToken(self):
		if self._refresh_enabled:
			print('refresh')
		else:
			RefreshableAccessToken._refreshAccessToken(self)

	def _get_timeleft(self, cached):
		return self._tmp

class Test_Proxy:
	"""
	>>> proxy1 = AccessToken.create_instance('AccessToken', config1, 'myproxy')
	>>> try_catch(proxy1.getUsername, 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(proxy1.getFQUsername, 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(proxy1.getGroup, 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(proxy1.getAuthFiles, 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(lambda: proxy1.can_submit(60, True), 'AbstractError', 'is an abstract function!')
	caught

	>>> proxy2 = AccessToken.create_instance('TrivialProxy', config1, 'myproxy')
	>>> proxy2.getUsername() == os.environ['LOGNAME']
	True
	>>> proxy2.getFQUsername() == os.environ['LOGNAME']
	True
	>>> proxy2.getGroup() == os.environ.get('GROUP', 'None')
	True
	>>> proxy2.getAuthFiles()
	[]
	>>> proxy2.can_submit(None, None)
	True

	>>> proxy3 = TestTimedProxy(config1, 120)
	>>> proxy3.can_submit(60, True)
	0000-00-00 00:00:00 - Time left for access token "myproxy": 0h 02min 00sec
	0000-00-00 00:00:00 - Access token (myproxy) lifetime (0h 02min 00sec) does not meet the access and walltime (0h 03min 00sec) requirements!
	0000-00-00 00:00:00 - Disabling job submission
	False

	>>> proxy5 = TestTimedProxy(config1, 240)
	>>> proxy5.can_submit(60, True)
	0000-00-00 00:00:00 - Time left for access token "myproxy": 0h 04min 00sec
	True

	>>> proxy6 = AccessToken.create_instance('MultiAccessToken', config2, 'myproxy', [proxy2, proxy5])
	>>> proxy6.getUsername() == proxy2.getUsername()
	True
	>>> proxy6.getFQUsername() == proxy2.getFQUsername()
	True
	>>> proxy6.getGroup() == proxy2.getGroup()
	True
	>>> proxy6.getAuthFiles() == proxy2.getAuthFiles()
	True
	>>> proxy6.can_submit(60, True) == proxy2.can_submit(60, True)
	True

	>>> os.environ['GC_TEST_MODE'] = 'AFS5'
	>>> tmp = os.environ.pop('KRB5CCNAME', None)
	>>> tmp = os.environ.pop('KRBTKFILE', None)
	>>> proxy7 = AccessToken.create_instance('AFSProxy', config2, 'myproxy')
	>>> logging.getLogger().setLevel(logging.WARNING)
	>>> (proxy7.getUsername(), proxy7.getFQUsername(), proxy7.getGroup(), proxy7.can_submit(60, True))
	('stober', 'stober@DESY.DE', 'DESY.DE', True)
	>>> logging.getLogger().setLevel(logging.DEFAULT)
	>>> sorted(lmap(os.path.basename, proxy7.getAuthFiles()))
	['proxy.KRB5CCNAME', 'proxy.KRBTKFILE']

	>>> try_catch(lambda: TestTimedProxy(config1, 60).can_submit(60, True), 'UserError', 'Your access token (myproxy) only has 60 seconds left')
	0000-00-00 00:00:00 - Time left for access token "myproxy": 0h 01min 00sec
	caught

	>>> try_catch(lambda: TestTimedProxy(config1, -1).can_submit(60, True), 'AbstractError', 'is an abstract function')
	caught
	"""


class Test_Refresh:
	"""
	>>> proxy1 = TestRefreshProxy(config1, 2*60*60)
	>>> proxy1.can_submit(60, True)
	0000-00-00 00:00:00 - Time left for access token "myproxy": 2h 00min 00sec
	True

	>>> proxy2 = TestRefreshProxy(config1, 30*60, True)
	>>> proxy2.can_submit(60, True) # 2x refresh - due to checking for (minimal) and (minimal + needed) time
	refresh
	0000-00-00 00:00:00 - Time left for access token "myproxy": 0h 30min 00sec
	refresh
	True

	>>> proxy3 = TestRefreshProxy(config1, 30*60, False)
	>>> try_catch(lambda: proxy3.can_submit(60, True), 'AbstractError', 'is an abstract function')
	caught
	"""


class Test_GridProxy:
	"""
	>>> os.environ['GC_TEST_FILE'] = 'test.ARC.proxy1'
	>>> proxy1 = AccessToken.create_instance('ArcProxy', config2, 'myproxy')
	>>> proxy1.getUsername()
	'Fred Markus Stober'
	>>> proxy1.getGroup()
	'cms'
	>>> proxy1.getAuthFiles()
	['/afs/desy.de/user/s/stober/.globus/proxy_grid']
	>>> proxy1.can_submit(100, True)
	0000-00-00 00:00:00 - Time left for access token "myproxy": 11h 59min 18sec
	True

	>>> os.environ['GC_TEST_FILE'] = 'test.ARC.proxy2'
	>>> proxy1 = AccessToken.create_instance('ArcProxy', config2, 'myproxy')
	>>> try_catch(proxy1.getUsername, 'AccessTokenError', "Can't access 'identity' in proxy information")
	caught

	>>> proxy2 = AccessToken.create_instance('VomsProxy', config1, 'myproxy')
	>>> try_catch(lambda: proxy2.can_submit(100, True), 'AccessTokenError', 'voms-proxy-info failed with return code')
	caught

	>>> proxy2 = AccessToken.create_instance('VomsProxy', config2, 'myproxy')
	>>> proxy2.getUsername()
	'Fred-Markus Stober'
	>>> proxy2.getFQUsername()
	'/C=DE/O=GermanGrid/OU=KIT/CN=Fred-Markus Stober'
	>>> proxy2.getGroup()
	'cms'
	>>> proxy2.getAuthFiles()
	['/usr/users/stober/.globus/proxy.grid']
	>>> proxy2.can_submit(100, True)
	0000-00-00 00:00:00 - Time left for access token "myproxy": 5h 10min 00sec
	True
	"""

run_test()
