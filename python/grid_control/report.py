from grid_control import Job, RuntimeError, utils

class Report:

	def __init__(self, jobs, allJobs):
		self.allJobs = allJobs
		if hasattr(jobs, 'all'):
			self.jobs = jobs.all
		else:
			self.jobs = jobs


	def show(self, opts, module = None):
		if opts.report:
			self.details()
			self.summary()
		elif opts.continuous:
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


	def details(self):
		reports = []
		for jobNum in self.jobs:
			report = self.allJobs.get(jobNum).report()
			report.update({'Job': jobNum})
			reports.append(report)
			if report["Destination"] != 'N/A':
				reports.append({"Id": report["Destination"]})
		print
		utils.printTabular([("Job", "Job"), ("Status", "Status"), ("Id", "Id / Destination")], reports)
		print


	def getJobCategory(self, job):
		cats = {Job.SUCCESS: 'SUCCESS', Job.FAILED: 'FAILED', Job.RUNNING: 'RUNNING', Job.DONE: 'RUNNING'}
		return cats.get(job.state, 'QUEUED')


	def modReport(self, details, module):
		print
		print '-----------------------------------------------------------------'
		print 'MODULE SUMMARY:'
		print '---------------'
		reports = {}
		states = ['QUEUED', 'RUNNING', 'FAILED', 'SUCCESS']
		head = {}
		for jobNum in self.jobs:
			report = module.report(jobNum)
			if not reports.has_key(str(report)):
				reports[str(report)] = dict(map(lambda x: (x, 0), states))
				for key, value in report.iteritems():
					if not key:
						continue
					reports[str(report)][key] = value
					head[key] = key
			reports[str(report)][self.getJobCategory(self.allJobs.get(jobNum))] += 1
		print
		utils.printTabular(map(lambda x: (x, x), head.keys() + states), reports.values())
		print


	def getWNInfos(self):
		states = ['QUEUED', 'RUNNING', 'FAILED', 'SUCCESS']

		def add(x,y): return x+y
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
			tmp = dict.fromkeys(states)
			for state in states:
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
			if not statinfo.has_key(site):
				statinfo[site] = initdict()
			if not statinfo[site].has_key(wn):
				statinfo[site][wn] = initdict()
			if not statinfo[site][wn].has_key(queue):
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
		# statinfo = {'site1: {''wn1.site1': {'FAILED': 0, 'RUNNING': 0, 'QUEUED': 0, 'SUCCESS': 1}, 'wn2.site1': ...}, 'site2': {'wn1.site2': ...}}
		return (maxlen, statinfo)


	def siteReport(self, details = 0, showtime = False):
		print '-----------------------------------------------------------------'
		print 'SITE SUMMARY:'
		print '---------------'
		print

		states = ['QUEUED', 'RUNNING', 'FAILED', 'SUCCESS']

		(maxlen_detail, statinfo) = self.getWNInfos()
		maxlen = 22
		if details > 2:
			maxlen = max(maxlen, maxlen_detail[2])
		if details > 1:
			maxlen = max(maxlen, maxlen_detail[1])
		maxlen = max(maxlen, maxlen_detail[0])
		
		# Print header
		print ' %s       | %12s | %12s | %12s | %12s' % tuple(['SITE / WN'.ljust(maxlen)] + map(lambda x: x.center(12), states))
		print '=%s=======' % (maxlen * '=') + len(states) * ('+' + 14 * '=')

		def ratestats(entries):
			result = map(lambda state: entries[state]['COUNT'], states)
			line = []
			all = max(1, sum(result))
			for x in result:
				line.extend([x, 100 * x / all])
			# return summed state infos in together with the percentage: [state1, percentage1, state2, percentage2, ...]
			return line

		def timestats(entries):
			result_count = map(lambda state: entries[state]['COUNT'], states)
			result_time = map(lambda state: entries[state]['TIME'], states)
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
	
		sites = filter(lambda x: not x in states, statinfo.keys())
		for num, site in enumerate(utils.sorted(sites)):
			print_stats(site, statinfo[site], maxlen, showtime, rate_site, time_site)

			if details > 1:
				wns = filter(lambda x: not x in states, statinfo[site].keys())
				for wn in utils.sorted(wns):
					print_stats(wn, statinfo[site][wn], maxlen, showtime, rate_wn, time_wn)

					if details > 2:
						queues = filter(lambda x: not x in states, statinfo[site][wn].keys())
						for queue in utils.sorted(queues):
							print_stats(queue, statinfo[site][wn][queue], maxlen, showtime, rate_queue, time_queue)
			if num < len(sites) - 1:
				print '----%s----' % (maxlen * '-') + 4 * ('+' + 14 * '-')

		print '====%s====' % (maxlen * '=') + 4 * ('+' + 14 * '=')
		print_stats('', statinfo, maxlen, showtime, rate_site, time_site)


	def summary(self):
		# Print report summary
		print '-----------------------------------------------------------------'
		print 'REPORT SUMMARY:'
		print '---------------'

		summary = map(lambda x: 0.0, Job.states)

		for id in self.allJobs.all:
			job = self.allJobs.get(id)
			summary[job.state] += 1

		def makeSum(*states):
			return reduce(lambda x, y: x + y, map(lambda x: summary[x], states))

		print 'Total number of jobs:      %4d   Number of successful jobs: %4d' % \
			(len(self.allJobs.all), summary[Job.SUCCESS])
		print 'Number of unfinished jobs: %4d   Number of failed jobs:     %4d\n' % \
			(makeSum(Job.INIT, Job.READY, Job.WAITING, Job.QUEUED, Job.SUBMITTED, Job.RUNNING),
			 makeSum(Job.ABORTED, Job.CANCELLED, Job.FAILED))
		print 'Detailed Information:'
		for state, category in enumerate(Job.states):
			if summary[state]:
				ratio = (summary[state] / (id + 1))*100
			else:
				ratio = 0
			print 'Jobs   %9s: %4d     %3d%%  ' % (category, summary[state], round(ratio)),
			if state % 2:
				print
		print
		print '-----------------------------------------------------------------\n'
		return 0
