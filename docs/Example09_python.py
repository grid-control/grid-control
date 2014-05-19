#from gcSettings import Settings
#   this import is only needed to execute the script standalone
#   (and <GCDIR>/python needs to be in your PYTHONPATH!)

import time
print time.time()

cfg = Settings()
cfg.workflow.task = 'UserTask'
cfg.workflow.backend = 'Host'

cfg.jobs.wall_time = '1:00'
cfg.section('jobs').jobs = 2

cfg.usertask.executable = 'Example02_local.sh'
cfg.usertask.set('arguments', 'arg1 arg2 arg3')

print cfg
print "=" * 20
