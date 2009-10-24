from grid_control import Job, RuntimeError, utils

class Report:
	states = ['WAITING', 'RUNNING', 'FAILED', 'SUCCESS']

	def __init__(self, jobs, allJobs):
		self.allJobs = allJobs
		if hasattr(jobs, '_jobs'):
			self.jobs = jobs._jobs.keys()
		else:
			self.jobs = jobs


	def show(self, opts, module = None):
		if opts.report:
			print
			self.details()
			self.summary()
		if opts.reportSite:
			self.siteReport(opts.reportSite, False)
		if opts.reportTime:
			self.siteReport(opts.reportTime, True)
		if opts.reportMod:
			self.modReport(opts.reportMod, module)
		if opts.report or opts.reportSite or opts.reportTime or opts.reportMod:
			return True
		return False


	def getJobCategory(self, job):
		cats = {Job.SUCCESS: 'SUCCESS', Job.FAILED: 'FAILED', Job.RUNNING: 'RUNNING', Job.DONE: 'RUNNING'}
		return cats.get(job.state, 'WAITING')


	def details(self):
		reports = []
		for jobNum in self.jobs:
			report = self.allJobs.get(jobNum).report()
			if report["Status"] == 'INIT':
				continue
			report.update({'Job': jobNum})
			reports.append(report)
			if report["Destination"] != 'N/A':
				reports.append({"Id": report["Destination"]})
		utils.printTabular([("Job", "Job"), ("Status", "Status"), ("Id", "Id / Destination")], reports, "rcl")
		print


	def summary(self, message = ""):
		# Print report summary
		print '-----------------------------------------------------------------'
		print 'REPORT SUMMARY:'
		print '---------------'

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
		print '\nDetailed Status Information:'#\33[0;1m'
		for stateNum, category in enumerate(Job.states):
			print 'Jobs  %9s:%8d  %3d%%    ' % tuple([category] + makePer(stateNum)),
			if stateNum % 2:
				print
		print '-----------------------------------------------------------------'
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
		print '-----------------------------------------------------------------'
		print 'MODULE SUMMARY:'
		print '---------------'
		print
		infos = map(lambda x: reports[x], order) + [None, all]
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
		maxlen = [0, 0, 0]

		# iterate over flat list of all occuring destinations
		destinations = reduce(add, map(lambda id: self.allJobs.get(id).history.values(), self.jobs))
		for dest in map(getDest, destinations):
			for i in range(3):
				maxlen[i] = max(maxlen[i], len(dest[i]))
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
		return (maxlen, statinfo)


	def siteReport(self, details = 0, showtime = False):
		print '-----------------------------------------------------------------'
		print 'SITE SUMMARY:'
		print '---------------'
		print

		(maxlen_detail, statinfo) = self.getWNInfos()
		maxlen = 22
		if details > 2:
			maxlen = max(maxlen, maxlen_detail[2])
		if details > 1:
			maxlen = max(maxlen, maxlen_detail[1])
		maxlen = max(maxlen, maxlen_detail[0])
		
		# Print header
		print ' %s       | %12s | %12s | %12s | %12s' % tuple(['SITE / WN'.ljust(maxlen)] + map(lambda x: x.center(12), Report.states))
		print '=%s=======' % (maxlen * '=') + len(Report.states) * ('+' + 14 * '=')

		def ratestats(entries):
			result = map(lambda state: entries[state]['COUNT'], Report.states)
			line = []
			all = max(1, sum(result))
			for x in result:
				line.extend([x, 100 * x / all])
			# return summed state infos in together with the percentage: [state1, percentage1, state2, percentage2, ...]
			return line

		def timestats(entries):
			result_count = map(lambda state: entries[state]['COUNT'], Report.states)
			result_time = map(lambda state: entries[state]['TIME'], Report.states)
			line = []
			all = max(1, sum(result_count))
			for x in result_time:
				x /= all
				line.extend([x / 60 / 60, (x / 60) % 60, x % 60])
			# return summed state infos in together with the percentage: [state1, percentage1, state2, percentage2, ...]
			return line

		rate_site = ' \33[0;1m%s\33[0m       | %5d (%3d%%) | %5d (%3d%%) | \33[0;91m%5d\33[0m (%3d%%) | \33[0;94m%5d\33[0m (%3d%%)'
		rate_wn = '    %s    | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%)'
		rate_queue = '       %s | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%)'

		time_site = ' \33[0;1m%s\33[0m       | %6d:%0.2d:%0.2d | %6d:%0.2d:%0.2d | \33[0;91m%6d:%0.2d:%0.2d\33[0m | \33[0;94m%6d:%0.2d:%0.2d\33[0m'
		time_wn = '    %s    | %6d:%0.2d:%0.2d | %6d:%0.2d:%0.2d | %6d:%0.2d:%0.2d | %6d:%0.2d:%0.2d'
		time_queue = '       %s | %6d:%0.2d:%0.2d | %6d:%0.2d:%0.2d | %6d:%0.2d:%0.2d | %6d:%0.2d:%0.2d'

		def print_stats(name, dict, maxlen, showtime, rate_fmt, time_fmt):
			print rate_fmt % tuple([name.ljust(maxlen)] + ratestats(dict))
			if showtime:
				print time_fmt % tuple([padding] + timestats(dict))

		padding = ' ' * maxlen
	
		sites = filter(lambda x: not x in Report.states, statinfo.keys())
		for num, site in enumerate(utils.sorted(sites)):
			print_stats(site, statinfo[site], maxlen, showtime, rate_site, time_site)

			if details > 1:
				wns = filter(lambda x: not x in Report.states, statinfo[site].keys())
				for wn in utils.sorted(wns):
					print_stats(wn, statinfo[site][wn], maxlen, showtime, rate_wn, time_wn)

					if details > 2:
						queues = filter(lambda x: not x in Report.states, statinfo[site][wn].keys())
						for queue in utils.sorted(queues):
							print_stats(queue, statinfo[site][wn][queue], maxlen, showtime, rate_queue, time_queue)
			if num < len(sites) - 1:
				print '----%s----' % (maxlen * '-') + 4 * ('+' + 14 * '-')

		print '====%s====' % (maxlen * '=') + 4 * ('+' + 14 * '=')
		print_stats('', statinfo, maxlen, showtime, rate_site, time_site)
		print
