#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import os, logging
from testfwk import create_config, run_test, testfwk_set_path, try_catch
from grid_control.backends.access import AccessToken, RefreshableAccessToken, TimedAccessToken
from python_compat import lmap, sorted


config1 = create_config(config_dict={'proxy': {'min lifetime': '0:02:00', 'ignore warnings': 'False'}})
config2 = create_config(config_dict={'proxy': {'ignore warnings': 'True', 'proxy path': '/dev/null'}})

testfwk_set_path('../bin')

class TestTimedProxy(TimedAccessToken):
	def __init__(self, config, tmp):
		TimedAccessToken.__init__(self, config, 'myproxy')
		self._tmp = tmp

	def _get_timeleft(self, cached):
		if self._tmp < 0:
			return TimedAccessToken._get_timeleft(self, cached)
		return self._tmp


class TestRefreshProxy(RefreshableAccessToken):
	def __init__(self, config, tmp, refresh_enabled=True):
		RefreshableAccessToken.__init__(self, config, 'myproxy')
		self._tmp = tmp
		self._refresh_enabled = refresh_enabled

	def _refresh_access_token(self):
		if self._refresh_enabled:
			print('refresh')
		else:
			RefreshableAccessToken._refresh_access_token(self)

	def _get_timeleft(self, cached):
		return self._tmp

class Test_Proxy:
	"""
	>>> proxy1 = AccessToken.create_instance('AccessToken', config1, 'myproxy')
	>>> try_catch(proxy1.get_user_name, 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(proxy1.get_fq_user_name, 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(proxy1.get_group, 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(proxy1.get_auth_fn_list, 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(lambda: proxy1.can_submit(60, True), 'AbstractError', 'is an abstract function!')
	caught
	>>> try_catch(lambda: AccessToken.create_instance('GridAccessToken', create_config(), 'myproxy', 'ls').get_group(), 'AbstractError')
	caught

	>>> proxy2 = AccessToken.create_instance('TrivialProxy', config1, 'myproxy')
	>>> user_name = os.environ.get('LOGNAME', os.environ.get('USER', os.environ.get('LUSER', os.environ.get('USERNAME'))))
	>>> proxy2.get_user_name() == user_name
	True
	>>> proxy2.get_fq_user_name() == user_name
	True
	>>> proxy2.get_group() == os.environ.get('GROUP', 'None')
	True
	>>> proxy2.get_auth_fn_list()
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
	>>> proxy6.get_user_name() == proxy2.get_user_name()
	True
	>>> proxy6.get_fq_user_name() == proxy2.get_fq_user_name()
	True
	>>> proxy6.get_group() == proxy2.get_group()
	True
	>>> proxy6.get_auth_fn_list() == proxy2.get_auth_fn_list()
	True
	>>> proxy6.can_submit(60, True) == proxy2.can_submit(60, True)
	True

	>>> os.environ['GC_TEST_MODE'] = 'AFS5'
	>>> tmp = os.environ.pop('KRB5CCNAME', None)
	>>> tmp = os.environ.pop('KRBTKFILE', None)
	>>> proxy7 = AccessToken.create_instance('AFSProxy', config2, 'myproxy')
	>>> logging.getLogger().setLevel(logging.WARNING)
	>>> (proxy7.get_user_name(), proxy7.get_fq_user_name(), proxy7.get_group(), proxy7.can_submit(60, True))
	('stober', 'stober@DESY.DE', 'DESY.DE', True)
	>>> logging.getLogger().setLevel(logging.DEFAULT)
	>>> sorted(lmap(os.path.basename, proxy7.get_auth_fn_list()))
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
	>>> proxy1.get_user_name()
	'Fred Markus Stober'
	>>> proxy1.get_group()
	'cms'
	>>> proxy1.get_auth_fn_list()
	['/afs/desy.de/user/s/stober/.globus/proxy_grid']
	>>> proxy1.can_submit(100, True)
	0000-00-00 00:00:00 - Time left for access token "myproxy": 11h 59min 18sec
	True

	>>> os.environ['GC_TEST_FILE'] = 'test.ARC.proxy2'
	>>> proxy1 = AccessToken.create_instance('ArcProxy', config2, 'myproxy')
	>>> try_catch(proxy1.get_user_name, 'AccessTokenError', "Can't access 'identity' in proxy information")
	caught

	>>> proxy2 = AccessToken.create_instance('VomsProxy', config1, 'myproxy')
	>>> try_catch(lambda: proxy2.can_submit(100, True), 'AccessTokenError', 'voms-proxy-info failed with return code')
	caught

	>>> proxy2 = AccessToken.create_instance('VomsProxy', config2, 'myproxy')
	>>> proxy2.get_user_name()
	'Fred-Markus Stober'
	>>> proxy2.get_fq_user_name()
	'/C=DE/O=GermanGrid/OU=KIT/CN=Fred-Markus Stober'
	>>> proxy2.get_group()
	'cms'
	>>> proxy2.get_auth_fn_list()
	['/usr/users/stober/.globus/proxy.grid']
	>>> proxy2.can_submit(100, True)
	0000-00-00 00:00:00 - Time left for access token "myproxy": 5h 10min 00sec
	True
	"""

run_test()
