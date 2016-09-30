# | Copyright 2010-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import re, time, operator
from grid_control import utils
from grid_control.gc_exceptions import UserError
from grid_control.job_db import Job
from grid_control.utils.parsing import parse_time
from hpfwk import AbstractError, Plugin
from python_compat import identity, imap, ismap, lfilter, lmap, reduce


class JobSelector(Plugin):
	def create(arg, **kwargs):
		if arg:
			return MultiJobSelector(arg, **kwargs)
		return None
	create = staticmethod(create)

	def __call__(self, jobnum, jobObj):
		raise AbstractError


class AndJobSelector(JobSelector): # Internally used
	def __init__(self, *args):
		self._selectors = args

	def __call__(self, jobnum, jobObj):
		return reduce(operator.and_, imap(lambda selector: selector(jobnum, jobObj), self._selectors))


class ClassSelector(JobSelector):
	def __init__(self, arg, **kwargs):
		self._states = arg.states

	def __call__(self, jobnum, jobObj):
		return jobObj.state in self._states


class StuckSelector(JobSelector):
	alias_list = ['stuck']

	def __init__(self, arg, **kwargs):
		self._time_threshold = parse_time(arg)

	def __call__(self, jobnum, jobObj):
		return (jobObj.changed > 0) and (time.time() - jobObj.changed) > self._time_threshold


class IDSelector(JobSelector):
	alias_list = ['id']

	def __init__(self, arg, **kwargs):
		idList = imap(lambda x: x.split('-'), arg.split(','))
		try:
			parse = lambda x: utils.QM(x != '', int, str)
			self._ranges = lmap(lambda x: (parse(x[0])(x[0]), parse(x[-1])(x[-1])), idList)
		except Exception:
			raise UserError('Job identifiers must be integers or ranges.')

	def __call__(self, jobnum, jobObj):
		def checkID(jobRange):
			if (jobRange[0] == '') or (jobnum >= jobRange[0]):
				if (jobRange[1] == '') or (jobnum <= jobRange[1]):
					return True
			return False
		return reduce(operator.or_, imap(checkID, self._ranges))


class RegExSelector(JobSelector):
	def __init__(self, arg, objParser, regexParser = identity, **kwargs):
		self._rxList = lmap(lambda x: re.compile(regexParser(x)), arg.split(','))
		self._objParser = objParser

	def __call__(self, jobnum, jobObj):
		for regex in self._rxList:
			if regex.search(self._objParser(jobnum, jobObj)):
				return True
		return False


class SiteSelector(RegExSelector):
	alias_list = ['site']

	def __init__(self, arg, **kwargs):
		RegExSelector.__init__(self, arg, lambda num, obj: obj.get('dest', '').split('/')[0].split(':')[0])


class QueueSelector(RegExSelector):
	alias_list = ['queue']

	def __init__(self, arg, **kwargs):
		RegExSelector.__init__(self, arg, lambda num, obj: obj.get('dest', '').split('/')[-1].split(':')[0])


class BackendSelector(RegExSelector):
	alias_list = ['backend', 'wms']

	def __init__(self, arg, **kwargs):
		def parseID(gcID):
			if gcID and (gcID.count('.') == 2):
				return gcID.split('.')[1]
			return ''
		RegExSelector.__init__(self, arg, lambda num, obj: parseID(obj.gcID))


class StateSelector(RegExSelector):
	alias_list = ['state']

	def __init__(self, arg, **kwargs):
		predef = {'TODO': 'SUBMITTED,WAITING,READY,QUEUED,UNKNOWN', 'ALL': str.join(',', Job.enum_name_list)}
		RegExSelector.__init__(self, predef.get(arg.upper(), arg), None, lambda x: '^%s.*' % x.upper())
		stateList = reduce(operator.add, imap(lambda x: lfilter(x.match, Job.enum_name_list), self._rxList))
		self._states = lmap(Job.str2enum, stateList)

	def __call__(self, jobnum, jobObj):
		return jobObj.state in self._states


class VarSelector(JobSelector):
	alias_list = ['var']

	def __init__(self, arg, **kwargs):
		self._rxDict = lmap(lambda x: (x.split('=', 1)[0], re.compile(x.split('=', 1)[1])), arg.split(','))
		self._jobCfg = lambda jobnum, var: str(kwargs['task'].get_job_dict(jobnum).get(var, ''))

	def __call__(self, jobnum, jobObj):
		def match(var, rx):
			return rx.search(self._jobCfg(jobnum, var)) is not None
		return reduce(operator.and_, ismap(match, self._rxDict))


class NickSelector(RegExSelector):
	alias_list = ['nick']

	def __init__(self, arg, **kwargs):
		RegExSelector.__init__(self, arg, lambda jobnum, jobObj: kwargs['task'].get_job_dict(jobnum).get('DATASETNICK', ''))


class MultiJobSelector(JobSelector):
	def __init__(self, arg, **kwargs):
		def parseTerm(term):
			negate = (term[0] == '~')
			term = term.lstrip('~')
			selectorType = utils.QM(term[0].isdigit(), 'id', 'state')
			if ':' in term:
				selectorType = term.split(':', 1)[0]
			selector = JobSelector.create_instance(selectorType, term.split(':', 1)[-1], **kwargs)
			if negate:
				return lambda jobnum, jobObj: not selector.__call__(jobnum, jobObj)
			return selector.__call__
		orTerms = str.join('+', imap(str.strip, arg.split('+'))).split()
		self._js = lmap(lambda orTerm: lmap(parseTerm, orTerm.split('+')), orTerms)

	def __call__(self, jobnum, jobObj):
		def onTerm(term):
			return term(jobnum, jobObj) # [[f1], [f2,f3]] => f1(...) || (f2(...) && f3(...))
		return reduce(operator.or_, imap(lambda andTerm: reduce(operator.and_, imap(onTerm, andTerm)), self._js))
