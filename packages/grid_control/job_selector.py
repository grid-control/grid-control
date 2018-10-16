# | Copyright 2010-2017 Karlsruhe Institute of Technology
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
from grid_control.gc_exceptions import UserError
from grid_control.job_db import Job, JobClass
from grid_control.utils.parsing import parse_time
from hpfwk import AbstractError, Plugin
from python_compat import identity, imap, ismap, lfilter, lidfilter, lmap, reduce, sorted


class TaskNeededException(Exception):
	pass


class JobSelector(Plugin):
	def __call__(self, jobnum, job_obj):
		raise AbstractError

	def create(arg, **kwargs):
		if arg:
			return MultiJobSelector(arg, **kwargs)
		return None
	create = staticmethod(create)


class AndJobSelector(JobSelector):  # Internally used
	def __new__(cls, *args):
		args = lidfilter(args)
		if not args:
			return None
		elif len(args) == 1:
			return args[0]
		return JobSelector.__new__(cls)

	def __init__(self, *args):
		self._selectors = args

	def __call__(self, jobnum, job_obj):
		return reduce(operator.and_, imap(lambda selector: selector(jobnum, job_obj), self._selectors))

	def __repr__(self):
		return self._repr_base(str.join('&', imap(repr, self._selectors)))


class ClassSelector(JobSelector):
	def __init__(self, arg, **kwargs):
		self._state_list = arg.state_list

	def __call__(self, jobnum, job_obj):
		return job_obj.state in self._state_list

	def __repr__(self):
		return self._repr_base(JobClass.lookup_job_class_name(self._state_list))


class IDSelector(JobSelector):
	alias_list = ['id']

	def __new__(cls, arg, **kwargs):
		if not arg:
			return None
		return JobSelector.__new__(cls)

	def __init__(self, arg, **kwargs):
		id_list = imap(lambda x: x.split('-'), arg.split(','))
		try:
			def _parse(value):
				if value != '':
					return int
				return str
			self._ranges = lmap(lambda x: (_parse(x[0])(x[0]), _parse(x[-1])(x[-1])), id_list)
		except Exception:
			raise UserError('Job identifiers must be integers or ranges.')

	def __call__(self, jobnum, job_obj):
		def _check_id(job_range):
			if (job_range[0] == '') or (jobnum >= job_range[0]):
				if (job_range[1] == '') or (jobnum <= job_range[1]):
					return True
			return False
		return reduce(operator.or_, imap(_check_id, self._ranges))

	def __repr__(self):
		return self._repr_base(str.join(',', imap(lambda job_range: '%s-%s' % job_range, self._ranges)))


class MultiJobSelector(JobSelector):
	alias_list = ['multi']

	def __init__(self, arg, **kwargs):
		def _parse_term(term):
			negate = (term[0] == '~')
			term = term.lstrip('~')
			selector_type = 'state'
			if term[0].isdigit():
				selector_type = 'id'
			elif ':' in term:
				selector_type = term.split(':', 1)[0]
			selector = JobSelector.create_instance(selector_type, term.split(':', 1)[-1], **kwargs)
			if negate:
				return lambda jobnum, job_obj: not selector.__call__(jobnum, job_obj)
			return selector
		(self._arg, or_term_list) = (arg, str.join('+', imap(str.strip, arg.split('+'))).split())
		self._js = lmap(lambda orTerm: lmap(_parse_term, orTerm.split('+')), or_term_list)

	def __call__(self, jobnum, job_obj):
		def _on_term(term):
			return term(jobnum, job_obj)  # [[f1], [f2,f3]] => f1(...) || (f2(...) && f3(...))
		return reduce(operator.or_,
			imap(lambda andTerm: reduce(operator.and_, imap(_on_term, andTerm)), self._js))

	def __repr__(self):
		return self._repr_base(self._arg)


class NullJobSelector(JobSelector):
	alias_list = ['null']

	def __call__(self, jobnum, job_obj):
		return True

	def __repr__(self):
		return self._repr_base()


class RegExSelector(JobSelector):
	def __init__(self, arg, obj_parser, regex_parser=identity, **kwargs):
		self._regex_obj_list = lmap(lambda x: re.compile(regex_parser(x)), arg.split(','))
		(self._arg, self._obj_parser) = (arg, obj_parser)

	def __call__(self, jobnum, job_obj):
		for regex in self._regex_obj_list:
			if regex.search(self._obj_parser(jobnum, job_obj)):
				return True
		return False

	def __repr__(self):
		return self._repr_base(self._arg)


class StateSelector(JobSelector):
	alias_list = ['state']

	def __init__(self, arg, **kwargs):
		predef = {
			'TODO': 'SUBMITTED,WAITING,READY,QUEUED,UNKNOWN',
			'ALL': str.join(',', Job.enum_name_list)
		}
		self._state_list = []
		for selector_str in predef.get(arg.upper(), arg).split(','):
			state_name_list = lfilter(re.compile('^%s.*' % selector_str.upper()).match, Job.enum_name_list)
			self._state_list.extend(imap(Job.str2enum, state_name_list))

	def __call__(self, jobnum, job_obj):
		return job_obj.state in self._state_list

	def __repr__(self):
		return self._repr_base(str.join(',', sorted(imap(Job.enum2str, self._state_list))))


class StuckSelector(JobSelector):
	alias_list = ['stuck']

	def __init__(self, arg, **kwargs):
		self._time_threshold = parse_time(arg)

	def __call__(self, jobnum, job_obj):
		return (job_obj.changed > 0) and (time.time() - job_obj.changed) > self._time_threshold

	def __repr__(self):
		return self._repr_base(self._time_threshold)


class VarSelector(JobSelector):
	alias_list = ['var']

	def __init__(self, arg, **kwargs):
		if 'task' not in kwargs:
			raise TaskNeededException()

		def _create_regex_item(value):
			return (value.split('=', 1)[0], re.compile(value.split('=', 1)[1]))
		(self._arg, self._regex_obj_list) = (arg, lmap(_create_regex_item, arg.split(',')))
		self._job_config = lambda jobnum, var: str(kwargs['task'].get_job_dict(jobnum).get(var, ''))

	def __call__(self, jobnum, job_obj):
		def _match(var, regex_obj):
			return regex_obj.search(self._job_config(jobnum, var)) is not None
		return reduce(operator.and_, ismap(_match, self._regex_obj_list))

	def __repr__(self):
		return self._repr_base(self._arg)


class BackendSelector(RegExSelector):
	alias_list = ['backend', 'wms']

	def __init__(self, arg, **kwargs):
		def _parse_id(gc_id):
			if gc_id and (gc_id.count('.') == 2):
				return gc_id.split('.')[1]
			return ''
		RegExSelector.__init__(self, arg, lambda num, obj: _parse_id(obj.gc_id))


class NickSelector(RegExSelector):
	alias_list = ['nick']

	def __init__(self, arg, **kwargs):
		if 'task' not in kwargs:
			raise TaskNeededException()
		RegExSelector.__init__(self, arg,
			obj_parser=lambda jobnum, job_obj: kwargs['task'].get_job_dict(jobnum).get('DATASETNICK', ''))


class QueueSelector(RegExSelector):
	alias_list = ['queue']

	def __init__(self, arg, **kwargs):
		RegExSelector.__init__(self, arg, obj_parser=lambda num, obj: obj.get('queue', ''))


class SiteSelector(RegExSelector):
	alias_list = ['site']

	def __init__(self, arg, **kwargs):
		RegExSelector.__init__(self, arg, obj_parser=lambda num, obj: obj.get('site', '').split(':')[0])
