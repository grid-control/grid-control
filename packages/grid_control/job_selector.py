import re, operator
from grid_control import QM, UserError, Job, utils, AbstractError, AbstractObject

class JobSelector(AbstractObject):
	def create(arg, **kwargs):
		if arg:
			return MultiJobSelector(arg, **kwargs)
		return None
	create = staticmethod(create)

	def __call__(self, jobNum, jobObj):
		raise AbstractError
JobSelector.dynamicLoaderPath()
JobSelector.moduleMap.update({'id': 'IDSelector', 'state': 'StateSelector',
	'site': 'SiteSelector', 'queue': 'QueueSelector', 'var': 'VarSelector', 'nick': 'NickSelector'})


class IDSelector(JobSelector):
	def __init__(self, arg, **kwargs):
		idList = map(lambda x: x.split('-'), arg.split(','))
		try:
			parse = lambda x: QM(x != '', int, str)
			self.ranges = map(lambda x: (parse(x[0])(x[0]), parse(x[-1])(x[-1])), idList)
		except:
			raise UserError('Job identifiers must be integers or ranges.')

	def __call__(self, jobNum, jobObj):
		def checkID(jobRange):
			if (jobRange[0] == '') or (jobNum >= jobRange[0]):
				if (jobRange[1] == '') or (jobNum <= jobRange[1]):
					return True
			return False
		return reduce(operator.or_, map(checkID, self.ranges))


class RegExSelector(JobSelector):
	def __init__(self, arg, objParser, regexParser = lambda x: x, **kwargs):
		self.rxList = map(lambda x: re.compile(regexParser(x)), arg.split(','))
		self.objParser = objParser

	def __call__(self, jobNum, jobObj):
		for regex in self.rxList:
			if regex.search(self.objParser(jobNum, jobObj)):
				return True
		return False


class SiteSelector(RegExSelector):
	def __init__(self, arg, **kwargs):
		RegExSelector.__init__(self, arg, lambda num, obj: obj.get('dest', '').split('/')[0].split(':')[0])


class QueueSelector(RegExSelector):
	def __init__(self, arg, **kwargs):
		RegExSelector.__init__(self, arg, lambda num, obj: obj.get('dest', '').split('/')[-1].split(':')[0])


class StateSelector(RegExSelector):
	def __init__(self, arg, **kwargs):
		predef = {'TODO': 'SUBMITTED,WAITING,READY,QUEUED', 'ALL': str.join(',', Job.states)}
		RegExSelector.__init__(self, predef.get(arg.upper(), arg), None, lambda x: '^%s.*' % x.upper())
		stateList = reduce(operator.add, map(lambda x: list(filter(x.match, Job.states)), self.rxList))
		self.states = map(lambda x: list(Job.states).index(x), stateList)

	def __call__(self, jobNum, jobObj):
		return jobObj.state in self.states


class VarSelector(JobSelector):
	def __init__(self, arg, **kwargs):
		self.rxDict = map(lambda x: (x.split('=', 1)[0], re.compile(x.split('=', 1)[1])), arg.split(','))
		self.jobCfg = lambda jobNum, var: str(kwargs['module'].getJobConfig(jobNum).get(var, ''))

	def __call__(self, jobNum, jobObj):
		return reduce(operator.and_, map(lambda (var, rx): rx.search(self.jobCfg(jobNum, var)) != None, self.rxDict))

class NickSelector(RegExSelector):
	def __init__(self, arg, **kwargs):
		RegExSelector.__init__(self, arg, lambda jobNum, jobObj: kwargs['module'].getJobConfig(jobNum).get('DATASETNICK', ''))

class MultiJobSelector(JobSelector):
	def __init__(self, arg, **kwargs):
		def parseTerm(term):
			cmpValue = QM(term[0] == '~', False, True)
			term = term.lstrip('~')
			selectorType = QM(term[0].isdigit(), 'id', 'state')
			if ':' in term:
				selectorType = term.split(':', 1)[0]
			selector = JobSelector.open(selectorType, term.split(':', 1)[-1], **kwargs)
			return lambda jobNum, jobObj: selector.__call__(jobNum, jobObj) == cmpValue
		orTerms = str.join('+', map(str.strip, arg.split('+'))).split()
		self.js = map(lambda orTerm: map(parseTerm, orTerm.split('+')), orTerms)

	def __call__(self, jobNum, jobObj):
		onTerm = lambda term: term(jobNum, jobObj) # [[f1], [f2,f3]] => f1(...) || (f2(...) && f3(...))
		return reduce(operator.or_, map(lambda andTerm: reduce(operator.and_, map(onTerm, andTerm)), self.js))
