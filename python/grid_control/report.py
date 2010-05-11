from python_compat import *
from grid_control import Job, RuntimeError, utils
from gui import Console

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
			print
			self.summary()
		if opts.reportJob:
			print
			self.jobs = self.allJobs.getJobs(opts.reportJob)
			self.details()
		if opts.reportSite:
			self.siteReport(opts.reportSite, False)
		if opts.reportTime:
			self.siteReport(opts.reportTime, True)
		if opts.reportMod:
			self.modReport(opts.reportMod, module)
		if opts.report or opts.reportSite or opts.reportTime or opts.reportMod or opts.reportJob:
			return True
		return False


	def getJobCategory(self, job):
		cats = {Job.SUCCESS: 'SUCCESS', Job.FAILED: 'FAILED', Job.RUNNING: 'RUNNING', Job.DONE: 'RUNNING'}
		return cats.get(job.state, 'WAITING')


	def printHeader(self, message):
		print '-----------------------------------------------------------------'
		print message.ljust(65)
		print '---------------'.ljust(65)


	def details(self):
		reports = []
		for jobNum in self.jobs:
			jobObj = self.allJobs.get(jobNum)
			report = jobObj.report()
			if jobObj.state == Job.INIT:
				continue
			report.update({'Job': jobNum})
			reports.append(report)
			if utils.verbosity() > 0:
				history = jobObj.history.items()
				history.reverse()
				for at, dest in history:
					if dest != 'N/A':
						reports.append({"Status": at, "Id": " -> " + dest})
			else:
				reports.append({"Id": " -> " + report["Destination"]})
		utils.printTabular([("Job", "Job"), ("Status", "Status / Attempt"), ("Id", "Id / Destination")], reports, "rcl")
		print


	def summary(self, message = ""):
		# Print report summary
		self.printHeader("REPORT SUMMARY:")

		summary = map(lambda x: 0.0, Job.states)
		for jobNum in self.jobs:
			summary[self.allJobs.get(jobNum).state] += 1

		def makeSum(*states):
			return reduce(lambda x, y: x + y, map(lambda x: summary[x], states))
		def makePer(*states):
			count = makeSum(*states)
			return [count, round(count / self.allJobs.nJobs * 100.0)]
		print 'Total number of jobs:%9d     Successful jobs:%8d  %3d%%' % \
			tuple([self.allJobs.nJobs] + makePer(Job.SUCCESS))
		print 'Jobs assigned to WMS:%9d        Failing jobs:%8d  %3d%%' % \
			tuple([makeSum(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)] +
			makePer(Job.ABORTED, Job.CANCELLED, Job.FAILED))
		print ' ' * 65 + '\nDetailed Status Information:'.ljust(65)
		for stateNum, category in enumerate(Job.states):
			print 'Jobs  %9s:%8d  %3d%%    ' % tuple([category] + makePer(stateNum)),
			if stateNum % 2:
				print
		print '-' * 65
		print message
		return 0


	def modReport(self, details, module):
		(order, head, reports) = ([], [], {})
		all = dict.fromkeys(Report.states, 0)

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
				all[cat] += 1
			except:
				pass
		infos = map(lambda x: reports[x], order) + [None, all]
		self.printHeader("MODULE SUMMARY:")
		print
		utils.printTabular(map(lambda x: (x, x), head + Report.states), infos, 'c' * len(head))
		print


	def getWNInfos(self):
		def add(x, y): return x+y
		def getDest(dest):
			if dest == 'N/A':
				return ('N/A', '', '')
			host, queue = dest.split('/')
			host = host.split(':')[0]
			domain = str.join('.', host.split('.')[1:])
			if domain == '':
				domain = 'localhost'
			return (domain, host, queue)
			# Example: (gridka.de, wn1.gridka.de, job-queue-long)
		def incstat(dict, L1, L2, L3, STAT, INFO, INC):
			dict[L1][L2][L3][STAT][INFO] += INC
			dict[L1][L2][STAT][INFO] += INC
			dict[L1][STAT][INFO] += INC
			dict[STAT][INFO] += INC
		def initdict():
			tmp = dict.fromkeys(Report.states)
			for state in Report.states:
				tmp[state] = {'COUNT': 0, 'TIME': 0}
			return tmp

		# init wn dictionary
		statinfo = initdict()

		# iterate over flat list of all occuring destinations
		destinations = reduce(add, map(lambda id: self.allJobs.get(id).history.values(), self.jobs))
		for dest in map(getDest, destinations):
			(site, wn, queue) = dest
			if site not in statinfo:
				statinfo[site] = initdict()
			if wn not in statinfo[site]:
				statinfo[site][wn] = initdict()
			if queue not in statinfo[site][wn]:
				statinfo[site][wn][queue] = initdict()

		# fill wn dictionary
		for id in self.jobs:
			job = self.allJobs.get(id)
			for attempt in job.history:
				if job.history[attempt] == 'N/A':
					continue
				# Extract site from history
				(site, wn, queue) = getDest(job.history[attempt])
				# Sort job into category
				if attempt == job.attempt:
					incstat(statinfo, site, wn, queue, self.getJobCategory(job), 'COUNT', 1)
					incstat(statinfo, site, wn, queue, self.getJobCategory(job), 'TIME', int(job.get('runtime')))
				else:
					incstat(statinfo, site, wn, queue, 'FAILED', 'COUNT', 1)
					incstat(statinfo, site, wn, queue, 'FAILED', 'TIME', int(job.get('runtime')))
		# statinfo = {'site1: {''wn1.site1': {'FAILED': 0, 'RUNNING': 0, 'WAITING': 0, 'SUCCESS': 1}, 'wn2.site1': ...}, 'site2': {'wn1.site2': ...}}
		return statinfo


	def siteReport(self, details = 0, showtime = False):
		statinfo = self.getWNInfos()
		markDict = {'FAILED': [Console.COLOR_RED, Console.BOLD], 'SUCCESS': [Console.COLOR_BLUE, Console.BOLD]}

		report = []
		def addRow(level, stats, pstats, mark = False):
			fmt = lambda x, state: x
			if mark:
				fmt = lambda x, state: Console.fmt(x, markDict.get(state, []))
				level = Console.fmt(level, [Console.BOLD])
			def fmtRate(state):
				all = max(1, sum(map(lambda x: stats[x]['COUNT'], Report.states)))
				ratio = (100.0 * stats[state]['COUNT']) / all
				return fmt("%4d" % stats[state]['COUNT'], state) + " (%3d%%)" % ratio
			def fmtTime(state):
				secs = stats[state]['TIME'] / max(1, stats[state]['COUNT']*1.0)
				return fmt(utils.strTime(secs, "%d:%0.2d:%0.2d"), state)
			report.append(dict([("SITE", level)] + map(lambda x: (x, fmtRate(x)), Report.states)))
			if showtime:
				report.append(dict(map(lambda x: (x, fmtTime(x)), Report.states)))

		sites = filter(lambda x: not x in Report.states, statinfo.keys())
		for num, site in enumerate(sorted(sites)):
			addRow(site, statinfo[site], statinfo, True)

			if details > 1:
				wns = filter(lambda x: not x in Report.states, statinfo[site].keys())
				for wn in sorted(wns):
					addRow(3*" " + wn, statinfo[site][wn], statinfo[site])

					if details > 2:
						queues = filter(lambda x: not x in Report.states, statinfo[site][wn].keys())
						for queue in sorted(queues):
							addRow(6*" " + queue, statinfo[site][wn][queue], statinfo[site][wn])

				if num < len(sites) - 1:
					report.append('')
		report.append(None)
		addRow('', statinfo, statinfo, True)
		header = [("SITE", 'SITE / WN')] + map(lambda x: (x, x), Report.states)

		self.printHeader("SITE SUMMARY:")
		print
		utils.printTabular(header, report, "lrrrr")
		print
