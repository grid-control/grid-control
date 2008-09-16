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
		#def getQueue(x): return x.split(":")[1].split("/")[1]
		def getSite(x): return str.join(".", x.split(":")[0].split(".")[1:])

		# init wn dictionary
		wnodes = {}.fromkeys(map(lambda x: x.split(":")[0], reduce(add, map(lambda id: self.allJobs.get(id).history.values(), self.jobs))), {})
		for node in wnodes:
			wnodes[node] = {}.fromkeys(states, 0)

		for id in self.jobs:
			job = self.allJobs.get(id)
			for attempt in job.history:
				if job.history[attempt] == "N/A":
					continue
				# Extract site from history
				wn = job.history[attempt].split(":")[0]
				if details:
					if len(wn) > maxlen_site:
						maxlen_site = len(wn)
				else:
					if len(getSite(wn)) > maxlen_site:
						maxlen_site = len(getSite(wn))

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

		def stats(entries):
			result = map(lambda state: sum(map(lambda x: x[1][state], entries)), states)
			line = []
			for x in result:
				line.extend([x, 100 * x / sum(result)])
			return line

		# Print header
		header = tuple(["SITE / WN".ljust(maxlen_site)] + map(lambda x: x.center(12), states))
		print " %s    | %12s | %12s | %12s | %12s" % header
		print "=%s====" % (maxlen_site * "=") + 4 * ("+" + 14 * "=")
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
