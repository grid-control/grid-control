from grid_control import Job, RuntimeError, utils, enumerate, SortedList

class Report:
	def __init__(self, jobs, allJobs):
		self.allJobs = allJobs
		if hasattr(jobs, "all"):
			self.jobs = jobs.all
		else:
			self.jobs = jobs
	
	def details(self):
		reports = {}
		maxWidth = [6, 20, 0]

		# Get the maximum width of each column
		for id in self.jobs:
			job = self.allJobs.get(id)
			thisreport = job.report()
			reports[id] = thisreport
			if maxWidth[0] < len(thisreport[0]):
				maxWidth[0] = len(thisreport[0])
			if maxWidth[1] < len(thisreport[1]):
				maxWidth[1] = len(thisreport[1])
			if maxWidth[2] < len(thisreport[2]):
				maxWidth[2] = len(thisreport[2])

		# Print table header
		print "\n%4s %-*s %s" % ("Job", maxWidth[0], "Status", "Destination / Job ID")

		# Calculate width of horizontal lines
		if maxWidth[1] > maxWidth[2]:
		        lineWidth = maxWidth[0]+maxWidth[1]+8
		else:
		        lineWidth = maxWidth[0]+maxWidth[2]+6
		line = "-"*lineWidth
		print line

		# Calculate spacer width for second row
		spacer = " "*(maxWidth[0]+5)

		# Get information of each job
		if not len(self.jobs):
			print " No jobs found!"
		for id in self.jobs:
			print "%4d %-*s %s /" % (id, maxWidth[0], reports[id][0], reports[id][1])
			print "%s %-*s" % (spacer, maxWidth[0]+maxWidth[2]+6, reports[id][2])

		print line + "\n"


	def siteReport(self, details = False):
		print "-----------------------------------------------------------------"
		print "SITE SUMMARY:"
		print "---------------"
		print
		states = ["QUEUED", "RUNNING", "FAILED", "SUCCESS"]
		maxlen_site = 25

		def add(x,y): return x+y
		def getSite(x): return str.join(".", x.split(":")[0].split(".")[1:])

		# init wn dictionary
		wnodes = {}
		# iterate over flat list of all occuring destinations
		for destination in reduce(add, map(lambda id: self.allJobs.get(id).history.values(), self.jobs)):
			wn = destination.split(":")[0]
			wnodes[wn] = {}.fromkeys(states, 0)

		# fill wn dictionary
		for id in self.jobs:
			job = self.allJobs.get(id)
			for attempt in job.history:
				if job.history[attempt] == "N/A":
					continue
				# Extract site from history
				wn = job.history[attempt].split(":")[0]
				maxlen_site = max(maxlen_site, len((lambda x: details and x or getSite(x))(wn)))

				# Sort job into category
				if attempt == job.attempt:
					if job.state == Job.SUCCESS:
						wnodes[wn]["SUCCESS"] += 1
					elif job.state == Job.FAILED:
						wnodes[wn]["FAILED"] += 1
					elif job.state in (Job.RUNNING, Job.DONE):
						wnodes[wn]["RUNNING"] += 1
					else:
						wnodes[wn]["QUEUED"] += 1
				else:
					wnodes[wn]["FAILED"] += 1
		# wnodes = {'wn1.site1': {'FAILED': 0, 'RUNNING': 0, 'QUEUED': 0, 'SUCCESS': 1}, 'wn2.site1': ..., 'wn1.site2': ...}
		
		# Print header
		print " %s    | %12s | %12s | %12s | %12s" % tuple(["SITE / WN".ljust(maxlen_site)] + map(lambda x: x.center(12), states))
		print "=%s====" % (maxlen_site * "=") + len(states) * ("+" + 14 * "=")

		def stats(entries):
			# entries = [('wn1.site1', {'FAILED': 0, 'RUNNING': 0, 'QUEUED': 0, 'SUCCESS': 1}), ('wn2.site1', ...)]
			# map list of (wn, {state info}) to list of summed state info
			result = map(lambda state: sum(map(lambda x: x[1][state], entries)), states)
			line = []
			all = max(1, sum(result))
			for x in result:
				line.extend([x, 100 * x / all])
			# return summed state infos in together with the percentage: [state1, percentage1, state2, percentage2, ...]
			return line

		sites = {}.fromkeys(map(lambda x: getSite(x).split(":")[0], wnodes.keys()), {})
		for site in SortedList(sites.keys()):
			sitenodes = filter(lambda (wn, info): getSite(wn) == site, wnodes.items())

			# Print per site results
			if details:
				print " \33[0;1m%s\33[0m    | %5d (%3d%%) | %5d (%3d%%) | \33[0;91m%5d\33[0m (%3d%%) | \33[0;94m%5d\33[0m (%3d%%)" % \
					tuple([site.ljust(maxlen_site)] + stats(sitenodes))

				# Print per WN results
				for wn, info in SortedList(sitenodes):
					print "    %s | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%)" % \
						tuple([wn.ljust(maxlen_site)] + stats([(wn, info)]))
				print "-%s----" % (maxlen_site * "-") + 4 * ("+" + 14 * "-")
			else:
				print " %s    | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%)" % \
					tuple([site.ljust(maxlen_site)] + stats(sitenodes))

		if not details:
			print "-%s----" % (maxlen_site * "-") + 4 * ("+" + 14 * "-")
			# Print final sum
			print " %s    | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%) | %5d (%3d%%)" % \
				tuple(["".ljust(maxlen_site)] + stats(wnodes.items()))
		else:
			print " \33[0;1m%s\33[0m    | %5d (%3d%%) | %5d (%3d%%) | \33[0;91m%5d\33[0m (%3d%%) | \33[0;94m%5d\33[0m (%3d%%)" % \
				tuple(["".ljust(maxlen_site)] + stats(wnodes.items()))
		print


	def summary(self):
		# Print report summary
		print "-----------------------------------------------------------------"
		print "REPORT SUMMARY:"
		print "---------------"

		summary = map(lambda x: 0.0, Job.states)

		for id in self.allJobs.all:
			job = self.allJobs.get(id)
			summary[job.state] += 1

		def makeSum(*states):
			return reduce(lambda x, y: x + y, map(lambda x: summary[x], states))

		print "Total number of jobs:      %4d   Number of successful jobs: %4d" % \
			(len(self.allJobs.all), summary[Job.SUCCESS])
		print "Number of unfinished jobs: %4d   Number of failed jobs:     %4d\n" % \
			(makeSum(Job.INIT, Job.READY, Job.WAITING, Job.QUEUED, Job.SUBMITTED, Job.RUNNING),
			 makeSum(Job.ABORTED, Job.CANCELLED, Job.FAILED))
		print "Detailed Information:"
		for state, category in enumerate(Job.states):
			if summary[state]:
				ratio = (summary[state] / (id + 1))*100
			else:
				ratio = 0
			print "Jobs   %9s: %4d     %3d%%" % (category, summary[state], round(ratio))

		print "-----------------------------------------------------------------\n"
		return 0
