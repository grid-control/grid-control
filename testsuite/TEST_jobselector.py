#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testfwk').setup(__file__)
# - prolog marker
import time
from testfwk import DummyObj, run_test, try_catch
from grid_control.job_db import Job, JobClass
from grid_control.job_selector import JobSelector
from grid_control.utils.parsing import str_dict_linear
from python_compat import ifilter, imap, lmap, lrange, md5_hex


jobnum_list = lrange(0, 10) + lrange(20, 32) + [14, 17]
jobnum_list.sort()

backends = ['Host1', 'Grid', 'Grid1', 'ARC']
sites = [None, 'ce1.gridka.de/long', 'ce1.gridka.de/medium', 'ce2.gridka.de/test', 'ce1.fnal.gov/medium', 'ce2.fnal.gov/']
time_base = time.time()

def createJobObj(jobnum):
	def getItem(src, fun):
		return src[abs(fun(jobnum)) % len(src)]
	result = Job()
	if jobnum < 30:
		result.gc_id = 'WMSID.%s.%s' % (getItem(backends, lambda x: x + 20), md5_hex(str(jobnum))[:5])
		result.attempt = (jobnum % 2) * (jobnum % 3)
		result.changed = time_base - abs(jobnum * (jobnum - 51) + 630)
		result.state = getItem(Job.enum_value_list, lambda x: x + 123)
		site = getItem(sites, lambda x: (x + (x % 3)) * x - 12)
		if site is not None:
			site, queue = site.split('/', 1)
			result.set('QUEUE', queue)
			result.set('SITE', site)
	return result

datasetnicks = ['Dataset_Zee', 'Dataset_Zmm', 'Dataset_ZZ', 'Dataset_WW', 'Dataset_tt']

def createJobInfo(jobnum):
	return {'TEST': jobnum, 'DATASETNICK': datasetnicks[jobnum % len(datasetnicks)]}

dummy_task = DummyObj(get_job_dict=createJobInfo)
jobinfo_list = lmap(lambda jobnum: (jobnum, createJobObj(jobnum)), jobnum_list)

def applySelector(selector):
	real_selector = selector or JobSelector.create_instance('NullJobSelector')
	result = lmap(lambda x: x[0], ifilter(lambda x: real_selector(*x), jobinfo_list))
	print(repr(selector))
	return result

def fmtJobObj(jobnum, jobobj):
	changed_str = '---'
	if jobobj.changed > 0:
		changed_str = '%-3d' % (time_base - jobobj.changed)
	sq_info = jobobj.get('site')
	if jobobj.get('queue'):
		sq_info += '/' + jobobj.get('queue')
	return '%02d | state: %-9s | attempt: %d | changed: %s | gc_id: %-17s | site/queue: %s' % (jobnum,
		Job.enum2str(jobobj.state), jobobj.attempt, changed_str, jobobj.gc_id, sq_info)

class Test_JobSelector:
	"""
	>>> print(str.join('\\n', imap(lambda x: fmtJobObj(*x), jobinfo_list)))
	00 | state: READY     | attempt: 0 | changed: 630 | gc_id: WMSID.Host1.cfcd2 | site/queue: None
	01 | state: WAITING   | attempt: 1 | changed: 580 | gc_id: WMSID.Grid.c4ca4  | site/queue: ce1.fnal.gov/medium
	02 | state: QUEUED    | attempt: 0 | changed: 532 | gc_id: WMSID.Grid1.c81e7 | site/queue: ce1.fnal.gov/medium
	03 | state: ABORTED   | attempt: 0 | changed: 486 | gc_id: WMSID.ARC.eccbc   | site/queue: ce2.gridka.de/test
	04 | state: RUNNING   | attempt: 0 | changed: 442 | gc_id: WMSID.Host1.a87ff | site/queue: ce1.gridka.de/medium
	05 | state: CANCEL    | attempt: 2 | changed: 400 | gc_id: WMSID.Grid.e4da3  | site/queue: ce2.fnal.gov
	06 | state: UNKNOWN   | attempt: 0 | changed: 360 | gc_id: WMSID.Grid1.16790 | site/queue: None
	07 | state: CANCELLED | attempt: 1 | changed: 322 | gc_id: WMSID.ARC.8f14e   | site/queue: ce1.gridka.de/medium
	08 | state: DONE      | attempt: 0 | changed: 286 | gc_id: WMSID.Host1.c9f0f | site/queue: ce1.gridka.de/medium
	09 | state: FAILED    | attempt: 0 | changed: 252 | gc_id: WMSID.Grid.45c48  | site/queue: ce2.gridka.de/test
	14 | state: DISABLED  | attempt: 0 | changed: 112 | gc_id: WMSID.Grid1.aab32 | site/queue: ce1.gridka.de/medium
	17 | state: QUEUED    | attempt: 2 | changed: 52  | gc_id: WMSID.Grid.70efd  | site/queue: ce2.fnal.gov
	20 | state: CANCEL    | attempt: 0 | changed: 10  | gc_id: WMSID.Host1.98f13 | site/queue: ce1.gridka.de/medium
	21 | state: UNKNOWN   | attempt: 0 | changed: 0   | gc_id: WMSID.Grid.3c59d  | site/queue: ce2.gridka.de/test
	22 | state: CANCELLED | attempt: 0 | changed: 8   | gc_id: WMSID.Grid1.b6d76 | site/queue: ce1.gridka.de/medium
	23 | state: DONE      | attempt: 2 | changed: 14  | gc_id: WMSID.ARC.37693   | site/queue: ce2.fnal.gov
	24 | state: FAILED    | attempt: 0 | changed: 18  | gc_id: WMSID.Host1.1ff1d | site/queue: None
	25 | state: SUCCESS   | attempt: 1 | changed: 20  | gc_id: WMSID.Grid.8e296  | site/queue: ce1.gridka.de/medium
	26 | state: IGNORED   | attempt: 0 | changed: 20  | gc_id: WMSID.Grid1.4e732 | site/queue: ce1.gridka.de/medium
	27 | state: INIT      | attempt: 0 | changed: 18  | gc_id: WMSID.ARC.02e74   | site/queue: ce2.gridka.de/test
	28 | state: SUBMITTED | attempt: 0 | changed: 14  | gc_id: WMSID.Host1.33e75 | site/queue: ce1.gridka.de/medium
	29 | state: DISABLED  | attempt: 2 | changed: 8   | gc_id: WMSID.Grid.6ea9a  | site/queue: ce2.fnal.gov
	30 | state: INIT      | attempt: 0 | changed: --- | gc_id: None              | site/queue: None
	31 | state: INIT      | attempt: 0 | changed: --- | gc_id: None              | site/queue: None

	>>> print(str.join('\\n', imap(lambda x: 'ID = %d, %s' % (x, str_dict_linear(dummy_task.get_job_dict(x))), jobnum_list)))
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
	<ClassSelector:ATWMS>
	[0, 1, 2, 6, 17, 21, 28]

	>>> applySelector(JobSelector.create_instance('NickSelector', '', task=dummy_task)) == jobnum_list
	<nick:>
	True
	>>> try_catch(lambda: JobSelector.create_instance('NickSelector', 'Dataset_Z'), 'TaskNeededException')
	caught
	>>> applySelector(JobSelector.create_instance('NickSelector', 'Dataset_Z', task=dummy_task))
	<nick:Dataset_Z>
	[0, 1, 2, 5, 6, 7, 17, 20, 21, 22, 25, 26, 27, 30, 31]

	>>> applySelector(JobSelector.create_instance('BackendSelector', '')) == jobnum_list
	<wms:>
	True
	>>> applySelector(JobSelector.create_instance('BackendSelector', 'Grid'))
	<wms:Grid>
	[1, 2, 5, 6, 9, 14, 17, 21, 22, 25, 26, 29]

	>>> applySelector(JobSelector.create_instance('AndJobSelector', JobSelector.create_instance('BackendSelector', 'Grid'), JobSelector.create_instance('IDSelector', '1-3')))
	<AndJobSelector:<wms:Grid>&<id:1-3>>
	[1, 2]
	>>> applySelector(JobSelector.create_instance('AndJobSelector', JobSelector.create_instance('BackendSelector', 'Grid'), None))
	<wms:Grid>
	[1, 2, 5, 6, 9, 14, 17, 21, 22, 25, 26, 29]
	>>> applySelector(JobSelector.create_instance('AndJobSelector', None, None)) == jobnum_list
	None
	True

	>>> applySelector(JobSelector.create_instance('NullJobSelector')) == jobnum_list
	<null>
	True

	>>> applySelector(JobSelector.create_instance('IDSelector', '')) == jobnum_list
	None
	True
	>>> try_catch(lambda: applySelector(JobSelector.create_instance('IDSelector', 'FAIL')), 'UserError', 'Job identifiers must be integers or ranges')
	caught
	>>> applySelector(JobSelector.create_instance('IDSelector', '7-10,10,-2,27-,23'))
	<id:7-10,10-10,-2,27-,23-23>
	[0, 1, 2, 7, 8, 9, 23, 27, 28, 29, 30, 31]

	>>> applySelector(JobSelector.create_instance('StuckSelector', '0:00'))
	<stuck:0>
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 14, 17, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]
	>>> applySelector(JobSelector.create_instance('StuckSelector', '0:05'))
	<stuck:300>
	[0, 1, 2, 3, 4, 5, 6, 7]

	>>> applySelector(JobSelector.create_instance('StateSelector', '')) == jobnum_list
	<state:ABORTED,CANCEL,CANCELLED,DISABLED,DONE,FAILED,IGNORED,INIT,QUEUED,READY,RUNNING,SUBMITTED,SUCCESS,UNKNOWN,WAITING>
	True
	>>> applySelector(JobSelector.create_instance('StateSelector', 'ALL')) == jobnum_list
	<state:ABORTED,CANCEL,CANCELLED,CANCELLED,DISABLED,DONE,FAILED,IGNORED,INIT,QUEUED,READY,RUNNING,SUBMITTED,SUCCESS,UNKNOWN,WAITING>
	True
	>>> applySelector(JobSelector.create_instance('StateSelector', 'TODO'))
	<state:QUEUED,READY,SUBMITTED,UNKNOWN,WAITING>
	[0, 1, 2, 6, 17, 21, 28]
	>>> applySelector(JobSelector.create_instance('StateSelector', 'R,Q'))
	<state:QUEUED,READY,RUNNING>
	[0, 2, 4, 17]

	>>> applySelector(JobSelector.create_instance('SiteSelector', '')) == jobnum_list
	<site:>
	True
	>>> applySelector(JobSelector.create_instance('SiteSelector', 'gridka.de'))
	<site:gridka.de>
	[3, 4, 7, 8, 9, 14, 20, 21, 22, 25, 26, 27, 28]

	>>> applySelector(JobSelector.create_instance('QueueSelector', '')) == jobnum_list
	<queue:>
	True
	>>> applySelector(JobSelector.create_instance('QueueSelector', 'medium'))
	<queue:medium>
	[1, 2, 4, 7, 8, 14, 20, 22, 25, 26, 28]

	>>> try_catch(lambda: JobSelector.create_instance('VarSelector', 'TEST=4$'), 'TaskNeededException')
	caught
	>>> applySelector(JobSelector.create_instance('VarSelector', 'TEST=4$', task=dummy_task))
	<var:TEST=4$>
	[4, 14, 24]

	>>> applySelector(JobSelector.create_instance('MultiJobSelector', '7-10,10,-2,27-,23'))
	<MultiJobSelector:7-10,10,-2,27-,23>
	[0, 1, 2, 7, 8, 9, 23, 27, 28, 29, 30, 31]

	>>> applySelector(JobSelector.create_instance('MultiJobSelector', '~7-10,10,-2,27-,23'))
	<MultiJobSelector:~7-10,10,-2,27-,23>
	[3, 4, 5, 6, 14, 17, 20, 21, 22, 24, 25, 26]

	>>> applySelector(JobSelector.create_instance('MultiJobSelector', '~5-25 id:8-17+12-'))
	<MultiJobSelector:~5-25 id:8-17+12->
	[0, 1, 2, 3, 4, 14, 17, 26, 27, 28, 29, 30, 31]

	>>> try_catch(lambda: applySelector(JobSelector()), 'AbstractError', 'is an abstract function')
	caught
	"""

run_test()
