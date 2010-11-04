from python_compat import *
from grid_control import QM, Job, RuntimeError, utils
from gui import Console
import time

class Report:
	states = ['WAITING', 'RUNNING', 'FAILED', 'SUCCESS']

	def __init__(self, jobs, allJobs):
		self.allJobs = allJobs
		try:
			self.jobs = jobs._jobs.keys()
		except:
			self.jobs = jobs


	def show(self, opts, module = None):
		if opts.report:
			utils.vprint(level = -1)
			self.summary()
		if opts.reportJob:
			utils.vprint(level = -1)
			self.jobs = self.allJobs.getJobs(opts.reportJob)
			self.details()
		if opts.reportSite or opts.reportTime:
			self.siteReport(opts.reportSite, opts.reportTime)
		if opts.reportMod:
			self.modReport(module)
		if opts.report or opts.reportSite or opts.reportTime or opts.reportMod or opts.reportJob:
			return True
		return False


	def getJobCategory(self, job):
		cats = {Job.SUCCESS: 'SUCCESS', Job.FAILED: 'FAILED', Job.RUNNING: 'RUNNING', Job.DONE: 'RUNNING'}
		return cats.get(job.state, 'WAITING')


	def printHeader(self, message, level = -1):
		utils.vprint('-'*65, level)
		utils.vprint(message.ljust(65), level)
		utils.vprint(('-'*15).ljust(65), level)


	def details(self):
		reports = []
		for jobNum in self.jobs:
			jobObj = self.allJobs.get(jobNum)
			if jobObj.state == Job.INIT:
				continue
			reports.append({0: jobNum, 1: Job.states[jobObj.state], 2: jobObj.wmsId})
			if utils.verbosity() > 0:
				history = jobObj.history.items()
				history.reverse()
				for at, dest in history:
					if dest != 'N/A':
						reports.append({1: at, 2: ' -> ' + dest})
			elif jobObj.get('dest', 'N/A') != 'N/A':
				reports.append({2: ' -> ' + jobObj.get('dest')})
		utils.printTabular(zip(range(3), ['Job', 'Status / Attempt', 'Id / Destination']), reports, 'rcl')
		utils.vprint(level = -1)


	def summary(self, message = '', level = -1):
		# Print report summary
		self.printHeader('REPORT SUMMARY:')

		summary = map(lambda x: 0.0, Job.states)
		for jobNum in self.jobs:
			summary[self.allJobs.get(jobNum).state] += 1

		def makeSum(*states):
			return sum(map(lambda z: summary[z], states))
		def makePer(*states):
			count = makeSum(*states)
			return [count, round(count / self.allJobs.nJobs * 100.0)]

		utils.vprint('Total number of jobs:%9d     Successful jobs:%8d  %3d%%' % \
			tuple([self.allJobs.nJobs] + makePer(Job.SUCCESS)), -1)
		utils.vprint('Jobs assigned to WMS:%9d        Failing jobs:%8d  %3d%%' % \
			tuple([makeSum(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)] +
			makePer(Job.ABORTED, Job.CANCELLED, Job.FAILED)), -1)
		utils.vprint(' ' * 65 + '\nDetailed Status Information:'.ljust(65), level)
		for stateNum, category in enumerate(Job.states):
			utils.vprint('Jobs  %9s:%8d  %3d%%     ' % tuple([category] + makePer(stateNum)), \
				level, newline = stateNum % 2)
		utils.vprint('-' * 65, level)
		utils.vprint(message, level)
		return 0


	def modReport(self, module):
		(order, head, reports) = ([], [], {})
		allStates = dict.fromkeys(Report.states, 0)

		for jobNum in self.jobs:
			report = module.report(jobNum)
			if str(report) not in reports:
				reports[str(report)] = dict(map(lambda x: (x, 0), Report.states))
				for key, value in report.iteritems():
					if not key:
						continue
					reports[str(report)][key] = value
					if not str(report) in order:
						order.append(str(report))
					if not key in head:
						head.append(key)
			try:
				cat = self.getJobCategory(self.allJobs.get(jobNum))
				reports[str(report)][cat] += 1
				allStates[cat] += 1
			except:
				pass
		infos = map(lambda x: reports[x], order) + [None, allStates]
		self.printHeader('MODULE SUMMARY:')
		utils.vprint(level = -1)
		utils.printTabular(map(lambda x: (x, x), head + Report.states), infos, 'c' * len(head))
		utils.vprint(level = -1)


	def getWNInfos(self):
		def getDest(dest):
			if dest == 'N/A':
				return ('N/A', '', '')
			host, queue = rsplit(dest, '/', 1)
			host = host.split(':')[0]
			domain = str.join('.', host.split('.')[1:])
			if domain == '':
				domain = 'localhost'
			return (domain, host, queue)
			# Example: (gridka.de, wn1.gridka.de, job-queue-long)

		# init statinfo dictionary
		def initdict():
			tmp = dict.fromkeys(Report.states)
			for state in Report.states:
				tmp[state] = {'COUNT': 0, 'TIME': 0}
			return tmp

		statinfo = initdict()
		destinations = map(lambda id: self.allJobs.get(id).history.values(), self.jobs)
		for dest in map(getDest, reduce(lambda x, y: x+y, destinations, [])):
			tmp = statinfo.setdefault(dest[0], initdict()).setdefault(dest[1], initdict())
			tmp.setdefault(dest[2], initdict())

		# fill wn dictionary
		def incstat(dict, dest, state, time):
			(site, wn, queue) = getDest(dest)
			for (key, inc) in zip(['COUNT', 'TIME'], [1, time]):
				dict[site][wn][queue][state][key] += inc
				dict[site][wn][state][key] += inc
				dict[site][state][key] += inc
				dict[state][key] += inc

		for id in self.jobs:
			job = self.allJobs.get(id)
			for attempt in job.history:
				# Sort job into category
				rt = job.get('runtime', 0)
				if attempt == job.attempt:
					cat = self.getJobCategory(job)
					if cat == 'SUCCESS':
						incstat(statinfo, job.history[attempt], cat, QM(rt == '', 0, rt))
					else:
						incstat(statinfo, job.history[attempt], cat, time.time() - float(job.submitted))
				else:
					if job.history[attempt] == 'N/A':
						continue
					incstat(statinfo, job.history[attempt], 'FAILED', QM(rt == '', 0, rt))
		# statinfo = {'site1: {''wn1.site1': {'FAILED': 0, 'RUNNING': 0, 'WAITING': 0, 'SUCCESS': 1}, 'wn2.site1': ...}, 'site2': {'wn1.site2': ...}}
		return statinfo


	def siteReport(self, siteDetails = 0, timeDetails = 0):
		statinfo = self.getWNInfos()
		markDict = {'FAILED': [Console.COLOR_RED, Console.BOLD], 'SUCCESS': [Console.COLOR_BLUE, Console.BOLD]}

		report = []
		def addRow(level, stats, mark = False):
			fmt = lambda x, state: x
			if mark:
				fmt = lambda x, state: Console.fmt(x, markDict.get(state, []))
				level = Console.fmt(level, [Console.BOLD])
			def fmtRate(state):
				all = max(1, sum(map(lambda x: stats[x]['COUNT'], Report.states)))
				ratio = (100.0 * stats[state]['COUNT']) / all
				return fmt('%4d' % stats[state]['COUNT'], state) + ' (%3d%%)' % ratio
			def fmtTime(state):
				secs = stats[state]['TIME'] / max(1, stats[state]['COUNT']*1.0)
				return fmt(utils.strTime(secs, '%d:%0.2d:%0.2d'), state)
			report.append(dict([('SITE', level)] + map(lambda x: (x, fmtRate(x)), Report.states)))
			if timeDetails:
				report.append(dict(map(lambda x: (x, fmtTime(x)), Report.states)))

		sites = filter(lambda x: not x in Report.states, statinfo.keys())
		for num, site in enumerate(sorted(sites)):
			addRow(site, statinfo[site], True)

			if siteDetails > 1:
				wns = filter(lambda x: not x in Report.states, statinfo[site].keys())
				for wn in sorted(wns):
					addRow(3*' ' + wn, statinfo[site][wn], statinfo[site])

					if siteDetails > 2:
						queues = filter(lambda x: not x in Report.states, statinfo[site][wn].keys())
						for queue in sorted(queues):
							addRow(6*' ' + queue, statinfo[site][wn][queue], statinfo[site][wn])

				if num < len(sites) - 1:
					report.append('')
		report.append(None)
		addRow('', statinfo, True)
		header = [('SITE', 'SITE / WN')] + map(lambda x: (x, x), Report.states)

		self.printHeader('SITE SUMMARY:')
		utils.vprint(level = -1)
		utils.printTabular(header, report, 'lrrrr')
		utils.vprint(level = -1)
