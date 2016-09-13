#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import create_config, display_jobs, run_test, try_catch
from grid_control.job_db import Job, JobDB
from grid_control.utils.parsing import str_dict


class Test_JobSelector:
	"""
	>>> job = Job()
	>>> str_dict(job.get_dict())
	"attempt = 0, changed = 0, id = None, status = 'INIT', submitted = 0"

	>>> config = create_config()
	>>> jobdb = JobDB(config)
	>>> jobdb.setJobLimit(-1)
	>>> try_catch(lambda: jobdb.getJob(0), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: jobdb.getJobTransient(0), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: jobdb.getJobPersistent(0), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: jobdb.commit(0, None), 'AbstractError', 'is an abstract function')
	caught
	>>> jobdb.get_work_path() == config.get_work_path()
	True

	>>> config = create_config(config_dict = {'global': {'workdir': './work.jobdb'}})

	>>> try_catch(lambda: JobDB.create_instance('TextFileJobDB', config), 'JobError', 'Unable to parse data')
	caught

	>>> display_jobs(JobDB.create_instance('TextFileJobDB', config, 2))
	Stopped reading job infos at job #2 out of 9 available job files, since the limit of 2 jobs is reached
	0 RUNNING  WMSID.GLITEWMS.https://grid-lb3.desy.de:9000/RnPEySRnkryNV8ii44Xuug (https://grid-lb3.desy.de:9000/RnPEySRnkryNV8ii44Xuug)
	1 SUCCESS  WMSID.GLITEWMS.https://grid-lb3.desy.de:9000/31DhvVNua7TXsvvpuOx7UQ (https://grid-lb3.desy.de:9000/31DhvVNua7TXsvvpuOx7UQ)

	>>> display_jobs(JobDB.create_instance('TextFileJobDB', config, 6))
	Stopped reading job infos at job #10 out of 9 available job files, since the limit of 6 jobs is reached
	0 RUNNING  WMSID.GLITEWMS.https://grid-lb3.desy.de:9000/RnPEySRnkryNV8ii44Xuug (https://grid-lb3.desy.de:9000/RnPEySRnkryNV8ii44Xuug)
	1 SUCCESS  WMSID.GLITEWMS.https://grid-lb3.desy.de:9000/31DhvVNua7TXsvvpuOx7UQ (https://grid-lb3.desy.de:9000/31DhvVNua7TXsvvpuOx7UQ)
	2 DONE     WMSID.Host.24468
	3 WAITING  WMSID.Host.24474
	4 QUEUED   WMSID.CREAMWMS.https://grid-ce.physik.rwth-aachen.de:8443/CREAM619600362
	5 FAILED   WMSID.OGE.5183066 (5183066.OGE)

	>>> display_jobs(JobDB.create_instance('TextFileJobDB', config, 7))
	Stopped reading job infos at job #10 out of 9 available job files, since the limit of 7 jobs is reached
	0 RUNNING  WMSID.GLITEWMS.https://grid-lb3.desy.de:9000/RnPEySRnkryNV8ii44Xuug (https://grid-lb3.desy.de:9000/RnPEySRnkryNV8ii44Xuug)
	1 SUCCESS  WMSID.GLITEWMS.https://grid-lb3.desy.de:9000/31DhvVNua7TXsvvpuOx7UQ (https://grid-lb3.desy.de:9000/31DhvVNua7TXsvvpuOx7UQ)
	2 DONE     WMSID.Host.24468
	3 WAITING  WMSID.Host.24474
	4 QUEUED   WMSID.CREAMWMS.https://grid-ce.physik.rwth-aachen.de:8443/CREAM619600362
	5 FAILED   WMSID.OGE.5183066 (5183066.OGE)
	6 <no stored data>

	>>> display_jobs(JobDB.create_instance('TextFileJobDB', config, 12))
	Stopped reading job infos at job #12 out of 9 available job files, since the limit of 12 jobs is reached
	0 RUNNING  WMSID.GLITEWMS.https://grid-lb3.desy.de:9000/RnPEySRnkryNV8ii44Xuug (https://grid-lb3.desy.de:9000/RnPEySRnkryNV8ii44Xuug)
	1 SUCCESS  WMSID.GLITEWMS.https://grid-lb3.desy.de:9000/31DhvVNua7TXsvvpuOx7UQ (https://grid-lb3.desy.de:9000/31DhvVNua7TXsvvpuOx7UQ)
	2 DONE     WMSID.Host.24468
	3 WAITING  WMSID.Host.24474
	4 QUEUED   WMSID.CREAMWMS.https://grid-ce.physik.rwth-aachen.de:8443/CREAM619600362
	5 FAILED   WMSID.OGE.5183066 (5183066.OGE)
	6 <no stored data>
	7 <no stored data>
	8 <no stored data>
	9 <no stored data>
	10 CANCELLED WMSID.OGE.34533066 (34533066.OGE)
	11 <no stored data>
	"""

run_test()
