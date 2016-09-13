#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
import time
from testFwk import DummyObj, run_test, try_catch
from grid_control.job_db import Job, JobClass
from grid_control.job_selector import JobSelector
from grid_control.utils.parsing import str_dict
from python_compat import ifilter, imap, lmap, lrange, md5_hex


jobnum_list = lrange(0, 10) + lrange(20, 32) + [14, 17]
jobnum_list.sort()

backends = ['Host1', 'Grid', 'Grid1', 'ARC']
sites = [None, 'ce1.gridka.de/long', 'ce1.gridka.de/medium', 'ce2.gridka.de/test', 'ce1.fnal.gov/medium', 'ce2.fnal.gov']
time_base = time.time()

def createJobObj(jobnum):
	def getItem(src, fun):
		return src[abs(fun(jobnum)) % len(src)]
	result = Job()
	if jobnum < 30:
		result.gcID = 'WMSID.%s.%s' % (getItem(backends, lambda x: x + 20), md5_hex(str(jobnum))[:5])
		result.attempt = (jobnum % 2) * (jobnum % 3)
		result.changed = time_base - abs(jobnum * (jobnum - 51) + 630)
		result.state = getItem(Job.enum_value_list, lambda x: x + 123)
		site = getItem(sites, lambda x: (x + (x % 3)) * x - 12)
		if site is not None:
			result.dict['dest'] = site
	return result

datasetnicks = ['Dataset_Zee', 'Dataset_Zmm', 'Dataset_ZZ', 'Dataset_WW', 'Dataset_tt']

def createJobInfo(jobnum):
	return {'TEST': jobnum, 'DATASETNICK': datasetnicks[jobnum % len(datasetnicks)]}

dummy_task = DummyObj(get_job_dict = createJobInfo)
jobinfo_list = lmap(lambda jobnum: (jobnum, createJobObj(jobnum)), jobnum_list)

def applySelector(selector):
	return lmap(lambda x: x[0], ifilter(lambda x: selector(*x), jobinfo_list))

def fmtJobObj(jobnum, jobobj):
	changed_str = '---'
	if jobobj.changed > 0:
		changed_str = '%-3d' % (time_base - jobobj.changed)
	return '%02d | state: %-9s | attempt: %d | changed: %s | gcID: %-17s | dest: %s' % (jobnum,
		Job.enum2str(jobobj.state), jobobj.attempt, changed_str, jobobj.gcID, jobobj.get('dest'))

class Test_JobSelector:
	"""
	>>> print(str.join('\\n', imap(lambda x: fmtJobObj(*x), jobinfo_list)))
	00 | state: DONE      | attempt: 0 | changed: 630 | gcID: WMSID.Host1.cfcd2 | dest: None
	01 | state: FAILED    | attempt: 1 | changed: 580 | gcID: WMSID.Grid.c4ca4  | dest: ce1.fnal.gov/medium
	02 | state: SUCCESS   | attempt: 0 | changed: 532 | gcID: WMSID.Grid1.c81e7 | dest: ce1.fnal.gov/medium
	03 | state: INIT      | attempt: 0 | changed: 486 | gcID: WMSID.ARC.eccbc   | dest: ce2.gridka.de/test
	04 | state: SUBMITTED | attempt: 0 | changed: 442 | gcID: WMSID.Host1.a87ff | dest: ce1.gridka.de/medium
	05 | state: DISABLED  | attempt: 2 | changed: 400 | gcID: WMSID.Grid.e4da3  | dest: ce2.fnal.gov
	06 | state: READY     | attempt: 0 | changed: 360 | gcID: WMSID.Grid1.16790 | dest: None
	07 | state: WAITING   | attempt: 1 | changed: 322 | gcID: WMSID.ARC.8f14e   | dest: ce1.gridka.de/medium
	08 | state: QUEUED    | attempt: 0 | changed: 286 | gcID: WMSID.Host1.c9f0f | dest: ce1.gridka.de/medium
	09 | state: ABORTED   | attempt: 0 | changed: 252 | gcID: WMSID.Grid.45c48  | dest: ce2.gridka.de/test
	14 | state: DONE      | attempt: 0 | changed: 112 | gcID: WMSID.Grid1.aab32 | dest: ce1.gridka.de/medium
	17 | state: INIT      | attempt: 2 | changed: 52  | gcID: WMSID.Grid.70efd  | dest: ce2.fnal.gov
	20 | state: READY     | attempt: 0 | changed: 10  | gcID: WMSID.Host1.98f13 | dest: ce1.gridka.de/medium
	21 | state: WAITING   | attempt: 0 | changed: 0   | gcID: WMSID.Grid.3c59d  | dest: ce2.gridka.de/test
	22 | state: QUEUED    | attempt: 0 | changed: 8   | gcID: WMSID.Grid1.b6d76 | dest: ce1.gridka.de/medium
	23 | state: ABORTED   | attempt: 2 | changed: 14  | gcID: WMSID.ARC.37693   | dest: ce2.fnal.gov
	24 | state: RUNNING   | attempt: 0 | changed: 18  | gcID: WMSID.Host1.1ff1d | dest: None
	25 | state: CANCEL    | attempt: 1 | changed: 20  | gcID: WMSID.Grid.8e296  | dest: ce1.gridka.de/medium
	26 | state: UNKNOWN   | attempt: 0 | changed: 20  | gcID: WMSID.Grid1.4e732 | dest: ce1.gridka.de/medium
	27 | state: CANCELLED | attempt: 0 | changed: 18  | gcID: WMSID.ARC.02e74   | dest: ce2.gridka.de/test
	28 | state: DONE      | attempt: 0 | changed: 14  | gcID: WMSID.Host1.33e75 | dest: ce1.gridka.de/medium
	29 | state: FAILED    | attempt: 2 | changed: 8   | gcID: WMSID.Grid.6ea9a  | dest: ce2.fnal.gov
	30 | state: INIT      | attempt: 0 | changed: --- | gcID: None              | dest: None
	31 | state: INIT      | attempt: 0 | changed: --- | gcID: None              | dest: None

	>>> print(str.join('\\n', imap(lambda x: 'ID = %d, %s' % (x, str_dict(dummy_task.get_job_dict(x))), jobnum_list)))
	ID = 0, DATASETNICK = 'Dataset_Zee', TEST = 0
	ID = 1, DATASETNICK = 'Dataset_Zmm', TEST = 1
	ID = 2, DATASETNICK = 'Dataset_ZZ', TEST = 2
	ID = 3, DATASETNICK = 'Dataset_WW', TEST = 3
	ID = 4, DATASETNICK = 'Dataset_tt', TEST = 4
	ID = 5, DATASETNICK = 'Dataset_Zee', TEST = 5
	ID = 6, DATASETNICK = 'Dataset_Zmm', TEST = 6
	ID = 7, DATASETNICK = 'Dataset_ZZ', TEST = 7
	ID = 8, DATASETNICK = 'Dataset_WW', TEST = 8
	ID = 9, DATASETNICK = 'Dataset_tt', TEST = 9
	ID = 14, DATASETNICK = 'Dataset_tt', TEST = 14
	ID = 17, DATASETNICK = 'Dataset_ZZ', TEST = 17
	ID = 20, DATASETNICK = 'Dataset_Zee', TEST = 20
	ID = 21, DATASETNICK = 'Dataset_Zmm', TEST = 21
	ID = 22, DATASETNICK = 'Dataset_ZZ', TEST = 22
	ID = 23, DATASETNICK = 'Dataset_WW', TEST = 23
	ID = 24, DATASETNICK = 'Dataset_tt', TEST = 24
	ID = 25, DATASETNICK = 'Dataset_Zee', TEST = 25
	ID = 26, DATASETNICK = 'Dataset_Zmm', TEST = 26
	ID = 27, DATASETNICK = 'Dataset_ZZ', TEST = 27
	ID = 28, DATASETNICK = 'Dataset_WW', TEST = 28
	ID = 29, DATASETNICK = 'Dataset_tt', TEST = 29
	ID = 30, DATASETNICK = 'Dataset_Zee', TEST = 30
	ID = 31, DATASETNICK = 'Dataset_Zmm', TEST = 31

	>>> applySelector(JobSelector.create_instance('ClassSelector', JobClass.ATWMS))
	[4, 6, 7, 8, 20, 21, 22, 26]

	>>> applySelector(JobSelector.create_instance('NickSelector', '', task = dummy_task)) == jobnum_list
	True
	>>> applySelector(JobSelector.create_instance('NickSelector', 'Dataset_Z', task = dummy_task))
	[0, 1, 2, 5, 6, 7, 17, 20, 21, 22, 25, 26, 27, 30, 31]

	>>> applySelector(JobSelector.create_instance('BackendSelector', '')) == jobnum_list
	True
	>>> applySelector(JobSelector.create_instance('BackendSelector', 'Grid'))
	[1, 2, 5, 6, 9, 14, 17, 21, 22, 25, 26, 29]

	>>> applySelector(JobSelector.create_instance('IDSelector', '')) == jobnum_list
	True
	>>> try_catch(lambda: applySelector(JobSelector.create_instance('IDSelector', 'FAIL')), 'UserError', 'Job identifiers must be integers or ranges')
	caught
	>>> applySelector(JobSelector.create_instance('IDSelector', '7-10,10,-2,27-,23'))
	[0, 1, 2, 7, 8, 9, 23, 27, 28, 29, 30, 31]

	>>> applySelector(JobSelector.create_instance('StuckSelector', '0:00'))
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 14, 17, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]
	>>> applySelector(JobSelector.create_instance('StuckSelector', '0:05'))
	[0, 1, 2, 3, 4, 5, 6, 7]

	>>> applySelector(JobSelector.create_instance('StateSelector', '')) == jobnum_list
	True
	>>> applySelector(JobSelector.create_instance('StateSelector', 'ALL')) == jobnum_list
	True
	>>> applySelector(JobSelector.create_instance('StateSelector', 'TODO'))
	[4, 6, 7, 8, 20, 21, 22, 26]
	>>> applySelector(JobSelector.create_instance('StateSelector', 'R,Q'))
	[6, 8, 20, 22, 24]

	>>> applySelector(JobSelector.create_instance('SiteSelector', '')) == jobnum_list
	True
	>>> applySelector(JobSelector.create_instance('SiteSelector', 'gridka.de'))
	[3, 4, 7, 8, 9, 14, 20, 21, 22, 25, 26, 27, 28]

	>>> applySelector(JobSelector.create_instance('QueueSelector', '')) == jobnum_list
	True
	>>> applySelector(JobSelector.create_instance('QueueSelector', 'medium'))
	[1, 2, 4, 7, 8, 14, 20, 22, 25, 26, 28]

	>>> applySelector(JobSelector.create_instance('VarSelector', 'TEST=4$', task = dummy_task))
	[4, 14, 24]

	>>> applySelector(JobSelector.create_instance('MultiJobSelector', '7-10,10,-2,27-,23'))
	[0, 1, 2, 7, 8, 9, 23, 27, 28, 29, 30, 31]

	>>> applySelector(JobSelector.create_instance('MultiJobSelector', '~7-10,10,-2,27-,23'))
	[3, 4, 5, 6, 14, 17, 20, 21, 22, 24, 25, 26]

	>>> applySelector(JobSelector.create_instance('MultiJobSelector', '~5-25 id:8-17+12-'))
	[0, 1, 2, 3, 4, 14, 17, 26, 27, 28, 29, 30, 31]

	>>> try_catch(lambda: applySelector(JobSelector()), 'AbstractError', 'is an abstract function')
	caught
	"""

run_test()
